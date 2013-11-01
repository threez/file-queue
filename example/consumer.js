var Queue = require('../queue').Queue;

function workQueue(queue) {
  queue.tpop(function (err, message, commit) {
    if (err) throw err;
    setTimeout(function() {
      commit(function (err) {
        if (err) throw err;
        console.log(message);
        workQueue(queue);
      });
    }, 500);
  })
}

var queue = new Queue('.', function(err) {
  workQueue(queue);
});
