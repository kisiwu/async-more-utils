import {
  AsyncPriorityQueue,
  AsyncResultCallback,
  ErrorCallback,
  priorityQueue
} from 'async';
import {
  EventEmitter
} from 'events';

interface PriorityQueueManagerConstants {
  readonly EVENT_CREATED_QUEUE: 'created queue';
  readonly EVENT_DETACHED_QUEUE: 'detached queue';
  readonly EVENT_TASK_ERROR: 'task error';
  //readonly STATE_IDLE: 'idle'; // no more running queues
  readonly STATE_PAUSED: 'paused'; // paused queues
  readonly STATE_RESUMING: 'resuming'; // not paused
  //readonly STATE_STOPPED: 'stopped';
}

export interface PriorityQueueManagerWorker<T, E = Error> { (task: T, callback: ErrorCallback<E>, name: string): void };

export interface PriorityQueueManagerArg<T, E = Error> {
  worker: PriorityQueueManagerWorker<T, E>;
  concurrency?: number;  
}

export interface PriorityQueueStats {
  length: number;
  paused: boolean;
  running: number;
}

export class PriorityQueueManager<T, E = Error> extends EventEmitter {

  #_concurrency: number;
  #_paused: boolean;
  #worker: PriorityQueueManagerWorker<T, E>;

  protected queues: Record<string, AsyncPriorityQueue<T>>;

  static readonly constants: PriorityQueueManagerConstants = {
    EVENT_CREATED_QUEUE: 'created queue',
    EVENT_DETACHED_QUEUE: 'detached queue',
    EVENT_TASK_ERROR: 'task error',
    //STATE_IDLE: 'idle',
    STATE_PAUSED: 'paused',
    STATE_RESUMING: 'resuming',
    //STATE_STOPPED: 'stopped'
  };

  public set concurrency(concurrency: number) {
    this.#_concurrency = concurrency;
    Object.keys(this.queues).forEach(id => {
      const q = this.queues[id];
      if(q) {
        q.concurrency = concurrency;
      }
    });
  }

  public get concurrency(): number {
    return this.#_concurrency;
  }

  /**
   * Number of items waiting to be processed.
   */
  public get length(): number {
    return Object.keys(this.queues).map(id => {
      let qLength = 0;
      const q = this.queues[id];
      if(q) {
        qLength = q.length();
      }
      return qLength;
    }).reduce((a, b) => a + b, 0);
  }

  public get paused(): boolean {
    return this.#_paused;
  }

  public get queueNames(): string[] {
    return Object.keys(this.queues);
  }

  /**
   * Number of items currently being processed.
   */
  public get running(): number {
    return Object.keys(this.queues).map(id => {
      let qLength = 0;
      const q = this.queues[id];
      if(q) {
        qLength = q.running();
      }
      return qLength;
    }).reduce((a, b) => a + b, 0);
  }

  /**
   * Number of queues processing.
   */
  public get size(): number {
    return Object.keys(this.queues).length;
  }

  public get stats(): Record<string, PriorityQueueStats> {
    const r: Record<string, PriorityQueueStats> = {};
    Object.keys(this.queues).forEach(id => {
      const q = this.queues[id];
      if(q) {
        r[id] = {
          length: q.length(),
          paused: q.paused,
          running: q.running()
        };
      }
    });
    return r;
  }

  constructor(arg: PriorityQueueManagerArg<T, E>);
  constructor(worker: PriorityQueueManagerWorker<T, E>, concurrency?: number);
  constructor(worker: PriorityQueueManagerWorker<T, E> | PriorityQueueManagerArg<T, E>, concurrency = 1) {
    super();
    let arg: PriorityQueueManagerArg<T, E>;
    if (typeof worker === 'function') {
      arg = {
        worker,
        concurrency
      };
    } else {
      arg = worker;
    }

    this.#worker = arg.worker;

    this.#_concurrency = arg.concurrency || concurrency;
    this.#_paused = false;
    this.queues = {};
    this.resume();
  }

  private _getOrCreate(name: string, autoDetach = true): AsyncPriorityQueue<T> {
    if (!this.queues[name]) {
      this.queues[name] = priorityQueue((t: T, cb: ErrorCallback<E>) => {
        this._handle(t, cb, name);
      }, this.#_concurrency);

      if (autoDetach) {
        this.queues[name].drain(() => {
          this.detach(name);
        });
      }

      this.queues[name].error((err: Error, task: T) => {
        this.emit(
          PriorityQueueManager.constants.EVENT_TASK_ERROR,
          err,
          task,
          {
            name
          }
        );
      });

      if(this.#_paused) {
        this.queues[name].pause();
      }

      this.emit(
        PriorityQueueManager.constants.EVENT_CREATED_QUEUE,
        {
          name: name
        }
      );
    }
    return this.queues[name];
  }

  private _handle(task: T, cb: ErrorCallback<E>, name: string): void {
    this.#worker(task, cb, name);
  }

  /**
   * 
   * Create or get a queue if it already exists.
   */
  create(name: string): AsyncPriorityQueue<T> {
    return this._getOrCreate(name, false);
  }

  /**
   * 
   * Detach queue from manager if it exists.
   */
  detach(name: string): undefined | AsyncPriorityQueue<T> {
    const q: undefined | AsyncPriorityQueue<T> = this.queues[name];
    delete this.queues[name];
    if(q) {
      this.emit(
        PriorityQueueManager.constants.EVENT_DETACHED_QUEUE,
        {
          name: name,
          queue: q
        }
      );
    }
    return q;
  }

  /**
   * 
   * Get a queue if it exists.
   */
  get(name: string): AsyncPriorityQueue<T> | undefined {
    return this.queues[name];
  }

  //// events ////

  onCreatedQueue(listener: (queue: {name: string}) => void): PriorityQueueManager<T, E> {
    this.on(
      PriorityQueueManager.constants.EVENT_CREATED_QUEUE,
      listener
    );
    return this;
  }

  onDetachedQueue(listener: (queue: {name: string, queue: AsyncPriorityQueue<T>}) => void): PriorityQueueManager<T, E> {
    this.on(
      PriorityQueueManager.constants.EVENT_DETACHED_QUEUE,
      listener
    );
    return this;
  }
  
  onTaskError(listener: (err: Error, task: T, queue: { name: string }) => void): PriorityQueueManager<T, E> {
    this.on(
      PriorityQueueManager.constants.EVENT_TASK_ERROR,
      listener
    );
    return this;
  }

  //// ASYNC METHODS ////

  pause(): void {
    this.#_paused = true;
    Object.keys(this.queues).forEach(id => {
      const q = this.queues[id];
      if(q) {
        q.pause();
      }
    });
    this.emit(
      PriorityQueueManager.constants.STATE_PAUSED,
      {}
    );
  }
  
  /**
   * 
   * Add a new task to the queue.
   */
  push<R>(name: string, task: T | T[], priority?: number): Promise<R>;
  push<R, E = Error>(name: string, task: T | T[], priority: number, callback?: AsyncResultCallback<R, E>): void;
  push<R, E = Error>(name: string, task: T | T[], priority: number, callback?: AsyncResultCallback<R, E>): void | Promise<R> {
    let r;
    const q = this._getOrCreate(name);
    if (!callback) {
      r = q.push<R>(task, priority);
    } else {
      r = q.push<R, E>(task, priority, callback);
    }
    return r;
  }

  resume(): void {
    this.#_paused = false;
    Object.keys(this.queues).forEach(id => {
      const q = this.queues[id];
      if(q) {
        q.resume();
      }
    });
    this.emit(
      PriorityQueueManager.constants.STATE_RESUMING,
      {}
    );
  }

  unshift<R>(name: string, task: T | T[]): Promise<R>;
  unshift<R>(name: string, task: T | T[]): Promise<R> {
    return this._getOrCreate(name).unshiftAsync(task);
  }
}