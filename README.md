# File-Queue

A simple file system based queuing implementation. Based on the maildir format,
the queues are lockfree and therefore work in most situatuions. Look
[here](http://cr.yp.to/proto/maildir.html) for the maildir specification.
The queue organises everything in FIFO (first-in first-out) order, because of
that it is not possible to get the messages of the queue with random access.
All objects are encoded using json and therefore can be easily read manually.

This queuing library has a specialty, its supports transactions for popping items.

## Create a queue

The queue is created based on a folder. The passed folder will contain have
three directories `cur`, `tmp` and `new`. The names of the files follow the 
maildir conventions.

    var Queue = require('file-queue').Queue,
        queue = new Queue('.', callback);

## Pushing and popping messages from the queue

Popping a message can be done at any time. If the queue doesn't contain an item at the
moment it 'blocks' until it does. If there was an error, while removing the
message `err` will contain an error message.

    queue.pop(function(err, message) {
      if (err) throw err;
      console.log(message);
    });

Pushing an item into the queue could cause an error if e.g. no disk space is
left on the device.

    queue.push('Hello World', function(err) {
      if (err) throw err;
    });

## Getting the length of the queue

The queue length can easily be determined with the following call:

    queue.length(function(err, length) {
      console.log(length);
    });

## Transactional popping

A transactional pop means, that the element is taken from the queue, but will
not be removed until commit is called. The rollback action makes the item again
available for popping.

    queue.tpop(function(err, message, commit, rollback) {
      if (Processor.process(message) === true) {
        commit(function(err) { if (err) throw err; });
      } else {
        rollback(function(err) { if (err) throw err; });
      }
    });

There can be multiple layers of transactions. Since transactional pops (tpops) don't block the popping in general, there can be multiple inside of each other.
The downside is that it can't be assured that the messages are processed in order.

## Clearing

To remove all items from a queue, for example for testing purposes, use clear.

    queue.clear(function(err) { if (err) throw err; });
