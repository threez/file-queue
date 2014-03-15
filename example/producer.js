'use strict';

var Queue = require('../queue').Queue,
    i = 0, queue;

queue = new Queue('.', function(err) {
  setInterval(function() {
    queue.push({ time: i++ }, function(err) {
      if(err) { throw err; }
    });
  }, 100);
});
