import { AsyncPriorityQueue, AsyncResultCallback } from 'async';
import { PriorityQueueManager } from '../src';
//import { expect } from 'chai';

describe('PriorityQueueManager test', () => {
  it('works', () => {
    //const start = Date.now();
    const pqm = new PriorityQueueManager<string, unknown>({
      worker(task, cb) {
        console.log(task);
        //console.log('working for task:', task, `(${(Date.now() - start)/1000} s)`);
        if (task === 'Bongo') {
          setTimeout(cb, 2000);
        } else {
          cb(new Error(`Not Bongo`));
        }
      }
    }).on('created queue', (o: {name: string}) => {
      console.log(`created queue ${o.name}`);
    })
    .on('detached queue', (o: {name: string, queue: AsyncPriorityQueue<string>}) => {
      console.log(`detached queue ${o.name} (${o.queue.idle()})`);
    })
    .onTaskError((err: Error, task, { name }) => {
      console.log(`ERROR task "${task}" at queue "${name}":`, err.message);
    });

    const logSize = () => {
      //console.log(`number of queues processing: ${pqm.size}`);
      //console.log(`number of items being processed: ${pqm.running}`);
      console.log(`number of items waiting to be processed: ${pqm.length}`);
    };

    const resultCallback: AsyncResultCallback<undefined> = () => {
      logSize();
    };

    //pqm.create('code-lyoko').push('Aelita', 6, resultCallback);

    pqm.push('X.A.N.A.', ['William', 'Ulrich'], 3, resultCallback);

    pqm.push('code-lyoko', 'Bongo', 1, resultCallback);

    logSize();

    //pqm.concurrency = 2;
  });
});
