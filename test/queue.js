'use strict';

var assert = require('assert'),
    Queue = require('../queue').Queue;

describe('Queue', function() {
  var queue;

  beforeEach(function(done) {
    queue = new Queue('tmp', done);
  });

  afterEach(function(done) {
    queue.clear(done);
  });

  describe('#length', function() {
    it('returns 0 for an empty queue', function(done) {
      queue.length(function(err, value) {
        if (err) { done(err); }
        else {
          assert.equal(0, value);
          done();
        }
      });
    });
  });

  describe('#push', function() {
    it('returns 1 if one item has been pushed', function(done) {
      queue.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
        else {
          queue.length(function(err, value) {
            assert.equal(1, value);
            done();
          });
        }
      });
    });
  });

  describe('#pop', function() {
    it('returns 1 item from the queue (push then pop)', function(done) {
      queue.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
        else {
          queue.pop(function(err, message) {
            if (err) { done(err); }
            assert.deepEqual({ 'hello': 'world' }, message);
            done();
          });
        }
      });
    });

    it('returns 1 item from the queue (pop then push)', function(done) {
      queue.pop(function(err, message) {
        if (err) { done(err); }
        else {
          assert.deepEqual({ 'hello': 'world' }, message);
          done();
        }
      });

      queue.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
      });
    });

    it('returns all items in order (pop then push)', function(done) {
      queue.pop(function(err, message) {
        if (err) { done(err); }
        assert.deepEqual({ 'id': 1 }, message);
        queue.pop(function(err, message) {
          if (err) { done(err); }
          assert.deepEqual({ 'id': 2 }, message);
          queue.pop(function(err, message) {
            if (err) { done(err); }
            assert.deepEqual({ 'id': 3 }, message);
            queue.pop(function(err, message) {
              if (err) { done(err); }
              else {
                assert.deepEqual({ 'id': 4 }, message);
                done();
              }
            });
          });
        });
      });

      queue.push({ 'id': 1}, function(err) {
        if (err) { done(err); }
        queue.push({ 'id': 2 }, function(err) {
          if (err) { done(err); }
          queue.push({ 'id': 3 }, function(err) {
            if (err) { done(err); }
            queue.push({ 'id': 4 }, function(err) {
              if (err) { done(err); }
            });
          });
        });
      });
    });

    it('returns all items in order (pop then push)', function(done) {
      queue.push({ 'id': 1}, function(err) {
        if (err) { done(err); }
        queue.push({ 'id': 2 }, function(err) {
          if (err) { done(err); }
          queue.push({ 'id': 3 }, function(err) {
            if (err) { done(err); }
            queue.push({ 'id': 4 }, function(err) {
              if (err) { done(err); }
              queue.pop(function(err, message) {
                if (err) { done(err); }
                assert.deepEqual({ 'id': 1 }, message);
                queue.pop(function(err, message) {
                  if (err) { done(err); }
                  assert.deepEqual({ 'id': 2 }, message);
                  queue.pop(function(err, message) {
                    if (err) { done(err); }
                    assert.deepEqual({ 'id': 3 }, message);
                    queue.pop(function(err, message) {
                      if (err) { done(err); }
                      else {
                        assert.deepEqual({ 'id': 4 }, message);
                        done();
                      }
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });

  describe('#tpop', function() {
    it('rollbacks a message so that it is available again', function(done) {
      queue.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
        queue.tpop(function(err, message, commit, rollback) {
          if (err) { done(err); }
          assert.deepEqual({ 'hello': 'world' }, message);
          queue.length(function(err, value) {
            if (err) { done(err); }
            assert.equal(0, value);
            rollback(function(err) {
              if (err) { done(err); }
              queue.length(function(err, value) {
                if (err) { done(err); }
                else {
                  assert.equal(1, value);
                  done();
                }
              });
            });
          });
        });
      });
    });

    it('commits a transaction successfully', function(done) {
      queue.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
        queue.tpop(function(err, message, commit, rollback) {
          if (err) { done(err); }
          assert.deepEqual({ 'hello': 'world' }, message);
          queue.length(function(err, value) {
            if (err) { done(err); }
            assert.equal(0, value);
            commit(function(err) {
              if (err) { done(err); }
              queue.length(function(err, value) {
                if (err) { done(err); }
                else {
                  assert.equal(0, value);
                  done();
                }
              });
            });
          });
        });
      });
    });

    it('returns 1 item from the queue (tpop then push)', function(done) {
      queue.tpop(function(err, message, commit, rollback) {
        if (err) { done(err); }
        else {
          assert.deepEqual({ 'hello': 'world' }, message);
          done();
        }
      });

      queue.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
      });
    });

    it('returns all items in order (tpop then push)', function(done) {
      queue.tpop(function(err, message, commit, rollback) {
        assert.deepEqual({ 'id': 1 }, message);
        commit(function(err) {
          if (err) { done(err); }
          queue.tpop(function(err, message, commit, rollback) {
            assert.deepEqual({ 'id': 2 }, message);
            commit(function(err) {
              if (err) { done(err); }
              done();
            });
          });
        });
      });

      queue.push({ 'id': 1}, function(err) {
        if (err) { done(err); }
        queue.push({ 'id': 2 }, function(err) {
          if (err) { done(err); }
        });
      });
    });

    it('returns all items in order (pop then push)', function(done) {
      queue.push({ 'id': 1}, function(err) {
        if (err) { done(err); }
        queue.push({ 'id': 2 }, function(err) {
          if (err) { done(err); }
          queue.push({ 'id': 3 }, function(err) {
            if (err) { done(err); }
            queue.push({ 'id': 4 }, function(err) {
              if (err) { done(err); }
              queue.pop(function(err, message, commit, rollback) {
                if (err) { done(err); }
                assert.deepEqual({ 'id': 1 }, message);
                queue.tpop(function(err, message, commit, rollback) {
                  if (err) { done(err); }
                  assert.deepEqual({ 'id': 2 }, message);
                  queue.tpop(function(err, message, commit, rollback) {
                    if (err) { done(err); }
                    assert.deepEqual({ 'id': 3 }, message);
                    queue.tpop(function(err, message, commit, rollback) {
                      if (err) { done(err); }
                      else {
                        assert.deepEqual({ 'id': 4 }, message);
                        done();
                      }
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });

  describe('#isRunning', function() {
    it('is running by default', function() {
      assert(queue.isRunning());
    });
  });

  describe('#stop', function() {
    it('is stopped after calling stop by default', function() {
      queue.stop();
      assert.equal(queue.isRunning(), false);
    });
  });
});
