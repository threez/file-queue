var Queue = require('../queue').Queue;
var i = 0;
var queue = new Queue('.', function(err) {
  setInterval(function () {
    queue.push({ time: i++ }, function (err) {
      if(err) throw err;
    })
  }, 100);
});
