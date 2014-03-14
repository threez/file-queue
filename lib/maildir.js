/*jslint white: true, indent:2, plusplus:true */
'use strict';
var util = require("util"),
    events = require("events"),
    fs = require('fs'),
    os = require('os'),
    crypto = require('crypto'),
    async = require('async');

module.exports = {
  Maildir: function(path) {
    path = path + '/';
    var dirs = ['tmp/', 'new/', 'cur/'],
        dirPaths = [path + dirs[0], path + dirs[1], path + dirs[2]],
        that = this,
        pushed = 0,
        hostname = os.hostname();

    // Finds the length of the queue (list of files in new)
    this.length = function(callback) {
      fs.stat(dirPaths[1], function(err, stat) {
        if (err) {callback(err);}
        else {callback(null, stat.nlink - 2);} // 2 == ['.', '..']
      });
    };

    // Generates a universal uniq name for each message
    this.generateUniqueName = function(callback) {
      var time = (new Date()).getTime();
      crypto.pseudoRandomBytes(8, function(err, randomBytes) {
        if (err) {callback(err);}
        else {
          callback(null, util.format('%d.%d.%d.%d%d.%s',
                                     time, pushed++, process.pid,
                                     randomBytes.readUInt32BE(0),
                                     randomBytes.readUInt32BE(4),
                                     hostname));
        }
      });
    };

    // Creates all folders required for maildir
    this.create = function(persistent, cb) {
      async.each(dirPaths, function(path, callback) {
        fs.exists(path, function(exists) {
          if (exists) {callback();}
          else {fs.mkdir(path, callback);}
        });
      }, function () {
        if (persistent) {
          that.watcher = fs.watch(dirPaths[1], {}, function (err, messages) {
            that.emit('new', [messages]);
          });
        }
        cb();
      });
    };

    this.stopWatching = function () {
      if (that.watcher && that.watcher.close) {
        that.watcher.close();
        that.watcher = null;
      }
    };

    // Creates a new message in the new folder
    this.newFile = function(data, callback) {
      this.generateUniqueName(function(err, uniqueName) {
        if (err) {callback(err);}
        else {
          var tmpPath = dirPaths[0] + uniqueName,
              newPath = dirPaths[1] + uniqueName;
          fs.writeFile(tmpPath, data, function (err) {
            if (err) {callback(err);}
            else {fs.rename(tmpPath, newPath, callback);}
          });
        }
      });
    };

    // Lists all messages in the new folder
    this.listNew = function (callback) {
      fs.readdir(dirPaths[1], callback);
    };

    // Clears all messages from all folders
    this.clear = function(callback) {
      async.map(dirPaths, fs.readdir, function(err, results) {
        if (err) {callback(err);}
        else {
          var unlinks = [], i, fn, len = dirPaths.length,
              pushDir = function (path) {
                return function (message) {
                  unlinks.push(path + message);
                };
              };
          for (i = 0; i < len; i++) {
            fn = pushDir(dirPaths[i]);
            results[i].forEach(fn);
          }
          async.each(unlinks, fs.unlink, callback);
        }
      });
    };

    // Processes one message from the queue (if possible)
    this.process = function(message, callback) {
      var newPath = dirPaths[1] + message,
          curPath = dirPaths[2] + message;

      fs.rename(newPath, curPath, function (err) {
        // if message could not be moved, another process probably already works
        // on it, so we try to pop again, but we try further on the list
        if (err) {
          callback(err);
        } else {
          fs.readFile(curPath, function(err, data) {
            if (err) {
              callback(err);
            } else {
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
  }
};

util.inherits(module.exports.Maildir, events.EventEmitter);
