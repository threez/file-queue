'use strict';

var Maildir = require('./lib/maildir').Maildir;

function Queue(options, cb) {
  var that = this, path, persistent = true;

  // determine maildir path
  path = (typeof options === 'string') ? options : options.path;

  // determine if maildir has a persistent watcher (default true)
  // if
  if (typeof options.persistent !== 'undefined') {
    persistent = options.persistent;
  }

  this.maildir = new Maildir(path);
  this.laterPop = [];

  // be notified, when new messages are available
  this.maildir.on('new', function(messages) {
    var callback = that.laterPop.shift();
    if (callback) { that.tpop(callback); }
  });

  // Create the queue with the given path
  this.maildir.create(persistent, cb);
}

// Pushs one message into the queue
Queue.prototype.push = function(message, callback) {
  this.maildir.newFile(JSON.stringify(message), callback);
};

// Pops one message of the queue
Queue.prototype.pop = function(callback) {
  this.tpop(function(err, message, commit, rollback) {
    if (err) {
      callback(err);
    } else {
      commit(function(err) {
        if (err) { callback(err); }
        else { callback(null, message); }
      });
    }
  });
};

// Pops one item in a transaction from the queue
Queue.prototype.tpop = function(callback) {
  var that = this;
  this.maildir.listNew(function(err, messages) {
    if (messages.length > 0) {
      that.tryPop(messages, callback);
    } else {
      that.laterPop.push(callback);
    }
  });
};

/*
 * Private function to try poping one item.
 * Analyse the error handling for:
 * - What should happen if the was couldn't be deleted?
 * - What should happen if the rename work but the message can't be read?
 * - What if the message can be read but the message doesn't contain
 *   valid json?
 */
Queue.prototype.tryPop = function(messages, callback) {
  var that = this,
      message = messages.shift();
  this.maildir.process(message, function(err, data, commit, rollback) {
    if (err) {
      if (messages.length === 0) {
        that.laterPop.push(callback); // no elements to pop, try later...
      } else {
        that.tryPop(messages, callback);
      }
    } else {
      try {
        callback(null, JSON.parse(data), commit, rollback);
      } catch(exception) {
        callback(new Error('JSONError: Message ' + message +
                           ' not valid! (' + exception + ')'));
      }
    }
  });
};

// Removes all elements from the queue
Queue.prototype.clear = function(callback) {
  this.maildir.clear(callback);
};

// Determines the length of the queue
Queue.prototype.length = function(callback) {
  this.maildir.length(callback);
};

// Determines if the directories are being monitored
Queue.prototype.isRunning = function() {
  return !!this.maildir.watcher;
};

// Stops monitoring the queue directories
Queue.prototype.stop = function() {
  this.maildir.stopWatching();
};

module.exports = {
  Maildir: Maildir,
  Queue: Queue
};
