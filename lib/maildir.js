'use strict';

var util = require('util'),
    events = require('events'),
    fs = require('fs'),
    os = require('os'),
    crypto = require('crypto'),
    async = require('async'),
    path = require('path'),
    TMP = 0,
    NEW = 1,
    CUR = 2;

function Maildir(root) {
  this.hostname = os.hostname();
  this.dirPaths = [path.resolve(path.join(root, 'tmp')),
                   path.resolve(path.join(root, 'new')),
                   path.resolve(path.join(root, 'cur'))];
  this.pushed = 0;
}

util.inherits(Maildir, events.EventEmitter);

// Finds the length of the queue (list of files in new)
Maildir.prototype.length = function(callback) {
  fs.readdir(this.dirPaths[NEW], function(err, files) {
    if (err) { callback(err); }
    else { callback(null, files.length); }
  });
};

// Generates a universal uniq name for each message
Maildir.prototype.generateUniqueName = function(callback) {
  var time = (new Date()).getTime(),
      that = this;
  crypto.pseudoRandomBytes(8, function(err, randomBytes) {
    if (err) { callback(err); }
    else {
      callback(null, util.format('%d.%d.%d.%d%d.%s',
                                 time, that.pushed++, process.pid,
                                 randomBytes.readUInt32BE(0),
                                 randomBytes.readUInt32BE(4),
                                 that.hostname));
    }
  });
};

// Creates all folders required for maildir
Maildir.prototype.create = function(persistent, cb) {
  var that = this;
  async.each(this.dirPaths, function(path, callback) {
    fs.exists(path, function(exists) {
      if (exists) { callback(); }
      else { fs.mkdir(path, callback); }
    });
  }, function() {
    if (persistent) {
      that.watcher = fs.watch(that.dirPaths[NEW], {}, function(err, messages) {
        that.emit('new', [messages]);
      });
    }
    cb();
  });
};

Maildir.prototype.stopWatching = function() {
  if (this.watcher && this.watcher.close) {
    this.watcher.close();
    this.watcher = null;
  }
};

// Creates a new message in the new folder
Maildir.prototype.newFile = function(data, callback) {
  var that = this;
  this.generateUniqueName(function(err, uniqueName) {
    if (err) { callback(err); }
    else {
      var tmpPath = path.join(that.dirPaths[TMP], uniqueName),
          newPath = path.join(that.dirPaths[NEW], uniqueName);
      fs.writeFile(tmpPath, data, function(err) {
        if (err) { callback(err); }
        else { fs.rename(tmpPath, newPath, callback); }
      });
    }
  });
};

// Lists all messages in the new folder
Maildir.prototype.listNew = function(callback) {
  fs.readdir(this.dirPaths[NEW], callback);
};

// Clears all messages from all folders
Maildir.prototype.clear = function(callback) {
  var that = this;
  async.map(this.dirPaths, fs.readdir, function(err, results) {
    if (err) { callback(err); }
    else {
      var unlinks = [], i, fn, len = that.dirPaths.length,
          pushDir = function(root) {
            return function(message) {
              unlinks.push(path.join(root, message));
            };
          };
      for (i = 0; i < len; i++) {
        fn = pushDir(that.dirPaths[i]);
        results[i].forEach(fn);
      }
      async.each(unlinks, fs.unlink, callback);
    }
  });
};

// Processes one message from the queue (if possible)
Maildir.prototype.process = function(message, callback) {
  var newPath = path.join(this.dirPaths[NEW], message),
      curPath = path.join(this.dirPaths[CUR], message);

  fs.rename(newPath, curPath, function(err) {
    // if message could not be moved, another process probably already works
    // on it, so we try to pop again, but we try further on the list
    if (err) { callback(err); }
    else {
      fs.readFile(curPath, function(err, data) {
        if (err) { callback(err); }
        else {
          callback(null, data,
            // commit function
            function(cb) { fs.unlink(curPath, cb); },
            // rollback function
            function(cb) { fs.rename(curPath, newPath, cb); }
          );
        }
      });
    }
  });
};

module.exports = {
  Maildir: Maildir
};
