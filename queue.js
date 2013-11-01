'use strict';
var Maildir = require('./lib/maildir').Maildir;

module.exports = {
  Queue: function(path, callback) {
    var that = this, laterPop = [];
    this.maildir = new Maildir(path);

    // be notified, when new messages are available
    this.maildir.on('new', function (messages) {
      var callback = laterPop.shift();
      if (callback) that.tpop(callback);
    });

    // Pushs one message into the queue
    this.push = function(message, callback) {
      this.maildir.newFile(JSON.stringify(message), callback);
    };

    // Pops one message of the queue
    this.pop = function(callback) {
      this.tpop(function(err, message, commit, rollback) {
        if (err) {
          callback(err);
        } else {
          commit(function(err) {
            if (err) callback(err);
            else callback(null, message);
          });
        }
      });
    };

    // Pops one item in a transaction from the queue
    this.tpop = function(callback) {
      this.maildir.listNew(function (err, messages) {
        if (messages.length > 0) {
          that.tryPop(messages, callback);
        } else {
          laterPop.push(callback);
        }
      });
    };

    /*
     * Private function to try poping one item.
     * Analyse the error handling for:
     * - What should happen if the was couldn't be deleted?
     * - What should happen if the rename work but the message can't be read?
     * - What if the message can be read but the message doesn't contain valid json?
     */
    this.tryPop = function(messages, callback) {
      var message = messages.shift();
      this.maildir.process(message, function(err, data, commit, rollback) {
        if (err) {
          if (messages.length === 0) {
            laterPop.push(callback); // no elements to pop, try later...
          } else {
            that.tryPop(messages, callback);
          }
        } else {
          try {
            callback(null, JSON.parse(data), commit, rollback);
          } catch(err) {
            throw "JSONError: Message" + message + " not valid! (" + err + ")";
          }
        }
      });
    };

    // Removes all elements from the queue
    this.clear  = function(callback) { this.maildir.clear(callback);  };

    // Determines the length of the queue
    this.length = function(callback) { this.maildir.length(callback); };

    // Create the queue with the given path
    this.maildir.create(callback);
  }
};
