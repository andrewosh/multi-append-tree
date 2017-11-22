var p = require('path')
var events = require('events')
var map = require('async-each')
var find = require('lodash.find')
var collect = require('stream-collector')
var lock = require('mutexify')
var messages = require('./messages')
var tree = require('append-tree')

module.exports = MultiTree

var PARENTS_ROOT = '/parents'
var ENTRIES_ROOT = '/entries'

function MultiTree (tree, factory, opts) {
  if (!(this instanceof MultiTree)) return new MultiTree(tree, factory, opts)
  if (!tree) throw new Error('Metadata append-tree must be non-null.')
  if (!factory || !(typeof factory === 'function')) throw new Error('Factory must be a non-null function that returns append-trees.')
  if (!opts) opts = {}
  this.opts = opts

  events.EventEmitter.call(this)

  this.checkout = opts.checkout
  this._factory = factory
  this._tree = tree
  this._lock = lock()

  // Set during initial indexing.
  this.version = null
  this.parents = []

  // Inflated archives.
  this.archives = {}

  // Link nodes we've encountered (minimizes link reads).
  this.links = {}

  this.ready(function (err) {
    if (!err) self.emit('ready')
  })
}

inherits(MultiTree, events.EventEmitter)

MultiTree.prototype._parentsPath = function (path) {
  return p.join(PARENTS_ROOT, path)
}

MultiTree.prototype._entriesPath = function (path) {
  return p.join(ENTRIES_ROOT, path)
}

MultiTree.prototype._inflateArchive = function (id, isMulti, cb) {
  var meta = this.archives[id]
  if (!meta) return cb(new Error('Trying to inflate a nonexistent link.'))
  var opts = meta.version ? : { version: meta.version } : null
  var appendTree = this.factory(meta.key, opts)
  var linkTree = (isMulti) ? MultiTree(appendTree, this._factory) : appendTree
  linkTree.ready(function (err) {
    if (err) return cb(err)
    meta.tree = linkTree
    return cb(null, linkTree)
  })
}

MultiTree.prototype._getParentTrees = function (cb) {
  var self = this
  // TODO: this path request and the subsequent list request should be atomic.
  // WARNING: possible race condition in _parentsNode if these ops aren't atomic.
  this._tree.path(PARENTS_ROOT, function (err, path) {
    if (self._parentsNode && (self._parentsNode === path[0])) {
      // The list of parents hasn't changed, so the node indices can be reused.
      return onnodes()
    }
    self._tree.list(PARENTS_ROOT, function (err, parentNames) {
      if (err) return cb(err)
      self._parentsNode = path[0]
      self._parents = []
      map(parentNames, function (name, next) {
        self._readLink(name, function (err, link) {
          if (err) return next(err)
          return next(null, link.node)
        })
      }, function (err, nodes) {
        if (err) return cb(err)
        self._parents = nodes
        return onnodes()
      })
    })
  })
  function onnodes () {
    return map(self._parents, function (parent, next) {
      self._getTreeForNode(parent, next)
    }, cb)
  }
}

MultiTree.prototype._getTreeForNode = function (node, cb) {
  var link = self.links[nodeIndex]
  if (link) return onlink(link)
  self._readLink(name, function (err, link) {
    if (err) return cb(err)
    return onlink(link)
  })
  function onlink (link) {
    if (link.tree) return cb(null, link.tree)  
    self._inflateArchive(link.key, link.version, function (err, tree) {
      if (err) return cb(err)
      link.tree = tree
      return cb(null, link.tree)
    })
  }
}

MultiTree.prototype._findLinkTrees = function (name, readParents, cb) {
  this.ready(function (err) {
    if (err) return cb(err)
    this._tree.path(name, function (err, path) {
      if (err) return cb(err)
      var nodeIndex = path[path.length - 1]
      self._getTreeForNode(nodeIndex, function (err, tree) {
        if (err) return cb(err)
        if (!readParents) return cb(null, [tree])
        self._getParentTrees(function (err, trees) {
          if (err) return cb(err)
          return cb(null, trees.push(tree))
        })
      })
  })
}

MultiTree.prototype._writeLink = function (name, target, cb) {
  var self = this
  this._tree.ready(function (err) {
    if (err) return cb(err)
    self._lock(function (err) {
      if (err) return cb(err)
      target.node = self._tree.version + 1
      self._tree.put(name, messages.LinkNode.encode(target), cb)
    })
  })
}

MultiTree.prototype._readLink = function (name, cb) {
  var self = this
  this._tree.ready(function (err) {
    if (err) return cb(err)
    self._tree.get(name, function (err, value) {
      if (err) return cb(err)
      var link = messages.LinkNode.decode(value)
      self.links[link.node] = link
      return cb(null, link)
    })
  })
}

MultiTree.prototype.ready = function (cb) {
  var self = this
  this._tree.ready(function (err) {
    if (err) return cb(err)
    if (self.opts.parents) {
      return map(self.opts.parents, function (parent, next) {
        self._writeLink(datEncoding.decode(parent.key), parent, function (err) {
          if (err) return cb(err)
        })
      }, function (err) {
        if (err) return cb(err)
        getparents()
      })
    }
    getparents()
  })
  function getparents () {
    // Index/inflate the parents eagerly (because this is required for every read).
    self._getParentTrees(function (err) {
      if (err) return cb(err)
      return cb(null)
    })
  }
}

MultiTree.prototype.link = function (name, target, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    return self._writeLink(name, target, cb)
  })
}

MultiTree.prototype._parentMap = function (trees, mapFunc, cb) {
  var parentTrees = trees.slice(0, self._parents.length)
  map(parentTrees, mapFunc, cb)
}

MultiTree.prototype._treesWrapper = function (name, includeParents, func) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self._findLinkTrees(name, includeParents, func)
  })
}

MultiTree.prototype.put = function (name, value, cb) {
  var self = this
  this._treesWrapper(name, false, function (err, trees) {
    if (trees.length === 0) return self._tree.put(name, value, cb)
    if (trees.length > 1) return cb(new Error('Trying to write to multiple symlinks.'))
    return trees[0].put(name, value, cb) 
  })
}

MultiTree.prototype.del = function (name, cb) {
  var self = this
  this._treesWrapper(name, false, function (err, trees) {
    if (trees.length === 0) return self._tree.del(name, cb)
    if (trees.length > 1) return cb(new Error('Trying to delete from multiple symlinks.'))
    return trees[0].del(name, cb) 
  })
}

MultiTree.prototype.unlink = MultiTree.prototype.del

MultiTree.prototype.list = function (name, opts, cb) {
  if (typeof opts === 'function') return this.list(name, {}, opts)
  var self = this
  this._treesWrapper(name, true, function (err, trees) {
    if (err) return cb(err)
    if (trees.length <= self._parents.length) {
      return map(trees.push(self._tree), function (tree, next) {
        return tree.list(next)
      }, function (err, lists) {
        if (err) return cb(err)
        // Take the union of the parents and self trees.
        // Note: merge conflicts can be handled by the user after `get`, not here.
        return cb(null, listUnion(lists))
      })
    }
    return trees[trees.length - 1].list(name, opts, cb)
  })
}

MultiTree.prototype.get = function (name, opts, cb) {
  if (typeof opts === 'function') return this.list(name, {}, opts)
  var self = this
  this._treesWrapper(name, true, function (err, trees) {
    if (err) return cb(err)
    if (trees.length > self._parents.length) {
      if (trees.length - self._parents.length > 1)
        return cb(new Error('Trying to get from multiple symlinks.'))
      return trees[trees.length - 1].get(name, opts, cb)
    }
    self._tree.get(name, opts, function (err, selfValue) {
      if (err && !err.notFound) return cb(err)
      if (selfValue) return cb(null, selfValue)
      map(trees, function (tree, next) {
        return tree.get(name, opts, function (err, parentResult) {
          if (err && !err.notFound) return cb(err)
          return cb(null, parentResult)
        }
      }, function (err, parentResults) {
        if (err) return cb(err)
        var nonNullResults = parentResults.filter(function (x) { return x })
        if (nonNullResults.length === 1) {
          // No possible conflict -- return single result.
          return cb(null, nonNullResults[0])
        }
        // Ensure that the result list is in parent order so that the user can trace
        // a result back to its corresponding parent.
        // Conflict resolution handled by user.
        return cb(null, nonNullResults)
      })
    }) 
  })
}

MultiTree.prototype.checkout = function (seq, opts) { }

MultiTree.prototype.head = function (opts, cb) { }

MultiTree.prototype.history = null

function listUnion (lists) {
  // TODO: probably too many allocations.
  return Array.from(new Set(lists.reduce(function (l, item) { return l.push(item) }, [])))
}
