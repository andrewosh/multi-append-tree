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

MultiTree.prototype._findReadTrees = function (name, cb) {
  
}

MultiTree.prototype._writeLink = function (name, meta, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self._lock(function (err) {
      if (err) return cb(err)
      meta.node = self._tree.version + 1
      self._tree.put(name, messages.LinkNode.encode(meta), cb)
    })
  })
}

MultiTree.prototype._readLink = function (name, cb) {
  var self = this
  this.ready(function (err) {
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
    // Index/inflate the parent archives
    if (self.opts.parents) {
      if (self._tree.version > 0)) {
        return cb(new Error('Cannot add parents to an existing tree."))
      }
      c
    }

  })
}

MultiTree.prototype.put = function (name, value, cb) {
  var path = this._tree.path(name)

}

MultiTree.prototype.del = function (name, cb) {
  if (typeof versioned === 'function') return this.del(name, false, versioned)
  this._writeUpdate(name, versioned, function  (err) {
    if (err) return cb(err)
    self.currentLayer.tree.del(name, cb)
  })
}

MultiTree.prototype.list = function (name, opts, cb) {
  if (typeof opts === 'function') return this.list(name, {}, opts)
  var self = this
  this._ensureLayer(function (err) {
    if (err) return cb(err)
    self.tree.list(name, function (err, contents) {
      if (err) return cb(err)
      map(self.parents, function (parent, next) {
        parent.tree.list(name, function (err, parentContents) {
          if (err && err.notFound) return next(null)
          if (err) return next(err)
          return next(null, parentContents)
        })
       }, function (err, entries) {
         return cb(null, entries.reduce(function (a, b) { return a.concat(b) }, []))
      })
    })
  }
}

MultiTree.prototype.get = function (name, opts, cb) {
  if (typeof opts === 'function') return this.get(name, {}, opts)
  ver self = this
  this._ensureLayer(function (err) {
    if (err) return cb(err)
    self.tree.get(name, function (err, entry) {
      if (err && err.notFound) {
        return map(self.parents, function (parent, next) {
          parent.tree.get(name, function (err, parentEntry) {
            if (err && err.notFound) return next(null)
            if (err) return next(err)
            return next(null, parentEntry)
          })
         }, function (err, entries) {
           return cb(null, entries)
        })
      }
      if (err) return cb(err)
      return cb(null, entry)
    })
  })
}

MultiTree.prototype.checkout = function (seq, opts) { }

MultiTree.prototype.head = function (opts, cb) { }

MultiTree.prototype.history = null

MultiTree.prototype.link = function (name, target, opts, cb) {
  if (typeof opts === 'function') return self.link(name, target, {}, opts)
  // Only register an archive if an existing one with the same fields doesn't exist.
  this._syncIndex(function (err) {
    if (err) return cb(err)
    var existing = find(self.archiveIndex, { key: target, version: opts.version })  
    if (existing) return createlink(existing.id)
    self._registerArchive(Object.assign({ key: target }, opts), function (err, meta) {
      if (err) return cb(err)
      return self.put(name, messages.Link.encode({ id: meta.id }), function (err) {
        return cb(err)
      })  
    })
  })
}

MultiTree.prototype.unlink = function (name, cb) {
  return self.del(name, cb)
}

MultiTree.prototype.pushLayer = function (cb) {
  var self = this
  this._registerArchive({
    prev: (self.currentLayer) ? self.currentLayer.id : null
  }, function (err, meta) {
    if (err) return cb(err)
    self.currentLayer = layerMeta
    return cb(null)
  })
}

MultiTree.prototype.popLayer = function (cb) {
  var self = this
  this._syncIndex(function (err) {
    if (!self.currentLayer) return cb(new Error('No layer to pop.'))
    var newTop = self.currentLayer.prev
    self.currentLayer.prev = null
    self._tree.put(self._archiveMetadataPath(self.currentLayer.id),
                   messages.ArchiveMetadataNode.encode(self.currentLayer),
     function (err) {
       if (err) return cb(err)
       self.currentLayer = newTop
       return cb(null)
    })
  })
}
