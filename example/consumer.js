'use strict';

var Queue = require('../queue').Queue, queue;

function workQueue(queue) {
  queue.tpop(function(err, message, commit) {
    if (err) { throw err; }
    setTimeout(function() {
      commit(function(err) {
        if (err) { throw err; }
        console.log(message);
        workQueue(queue);
      });
    }, 500);
  });
}

queue = new Queue('.', function(err) {
  workQueue(queue);
});
