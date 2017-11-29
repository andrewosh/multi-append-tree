var p = require('path')
var events = require('events')
var map = require('async-each')
var thunky = require('thunky')
var lock = require('mutexify')
var inherits = require('inherits')
var codecs = require('codecs')
var tree = require('append-tree')
var datEncoding = require('dat-encoding')

var atMessages = require('append-tree/messages')
var messages = require('./messages')

module.exports = MultiTree

var PARENTS_ROOT = '/parents'
var ENTRIES_ROOT = '/entries'

function MultiTree (feed, factory, key, opts) {
  if (!(this instanceof MultiTree)) return new MultiTree(feed, factory, key, opts)
  if (!factory || !(typeof factory === 'function')) throw new Error('Factory must be a non-null function that returns hypercores')
  if (key && (!(key instanceof Buffer) && !(typeof key === 'string'))) return new MultiTree(feed, factory, null, key)

  if (!opts) opts = {}
  this.opts = opts
  this.subtreeOpts = Object.assign({}, this.opts)
  this.subtreeOpts.valueEncoding = null

  this.key = key

  this._codec = opts.codec || codecs(opts.valueEncoding)

  events.EventEmitter.call(this)

  this._factory = factory
  this.feed = feed || factory(this.key, this.subtreeOpts)
  this._tree = tree(this.feed, this.subtreeOpts)
  this._lock = lock()

  // Set during initial indexing.
  this._parents = []
  this.version = null
  this.feed = null

  // Inflated archives.
  this.archives = {}

  // Link nodes we've encountered (minimizes link reads).
  this.links = {}

  var self = this
  this.ready = thunky(this._open.bind(this))
  this.ready(function (err) {
    if (!err) self.emit('ready')
  })
}

inherits(MultiTree, events.EventEmitter)

MultiTree.prototype._parentsPath = function (path) {
  var relativePath = (path.startsWith(PARENTS_ROOT)) ? path : p.join(PARENTS_ROOT, path)
  return normalize(relativePath)
}

MultiTree.prototype._entriesPath = function (path) {
  var relativePath = (path.startsWith(ENTRIES_ROOT)) ? path : p.join(ENTRIES_ROOT, path)
  return normalize(relativePath)
}

MultiTree.prototype._inflateTree = function (key, version, opts, cb) {
  if (typeof opts === 'function') return this._inflateTree(key, version, {}, opts)
  var mergedOpts = Object.assign({}, this.opts, opts)
  mergedOpts.parents = null

  var t = MultiTree(this._factory(key, mergedOpts), this._factory, mergedOpts)

  if (version !== null && version > -1) {
    t.checkout(version)
  }

  t.ready(function (err) {
    if (err) return cb(err)
    return cb(null, t)
  })
}

MultiTree.prototype._getParentTrees = function (cb) {
  var self = this

  // TODO: this path request and the subsequent list request should be atomic.
  // WARNING: possible race condition in _parentsNode if these ops aren't atomic.
  this._tree.path(PARENTS_ROOT, function (err, path) {
    if (err && err.notFound) return cb(null, [])
    if (err) return cb(err)
    if (self._parentsNode && (self._parentsNode === path[0])) {
      // The list of parents hasn't changed, so the node indices can be reused.
      return onlinks()
    }

    self._tree.list(PARENTS_ROOT, function (err, parentNames) {
      if (err && err.notFound) return cb(null, [])
      if (err) return cb(err)
      self._parentsNode = path[0]
      self._parents = []

      map(parentNames, function (name, next) {
        self._readNode(p.join(PARENTS_ROOT, name), false, function (err, link) {
          if (err) return next(err)
          return next(null, self._extractData(link, true))
        })
      }, function (err, links) {
        if (err) return cb(err)
        self._parents = links
        return onlinks()
      })
    })
  })

  function onlinks () {
    if (self._parents.length === 0) return cb()
    return map(self._parents, function (parent, next) {
      self._getTreeForNode(parent.node, parent.name, true, next)
    }, cb)
  }
}

MultiTree.prototype._getTreeForNode = function (node, name, isParent, cb) {
  if (typeof isParent === 'function') return this._getTreeForNode(node, name, false, isParent)
  var self = this
  var link = this.links[node]
  if (link) return onlink(link)
  this._readNode(node, true, function (err, outerNode) {
    if (err && err.notFound) return cb(null)
    if (err) return cb(err)

    if (outerNode.type === messages.Node.Type.DATA) return cb(null)

    return onlink(messages.LinkNode.decode(outerNode.value))
  })
  function onlink (link) {
    if (!isParent && ((name === link.name) || !name.startsWith(link.name))) return cb(null)
    if (link.tree) return cb(null, link.tree)
    self._inflateTree(link.key, link.version, function (err, tree) {
      if (err) return cb(err)
      link.tree = tree

      // TODO: This should be done better.
      tree.nameTag = link.name
      tree.pathTag = link.path

      return cb(null, link.tree)
    })
  }
}

MultiTree.prototype._findLinkTrees = function (name, readParents, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self._tree.path(name, function (err, path) {
      if (err && err.notFound) return cb(null, [])
      if (err) return cb(err)

      var nodeIndex = path[path.length - 1]

      self._getTreeForNode(nodeIndex, name, function (err, tree) {
        if (err) return cb(err)
        var linkTreeList = (tree) ? [tree] : []
        if (!readParents) return cb(null, linkTreeList)

        self._getParentTrees(function (err, parentTrees) {
          if (err) return cb(err)
          var allTrees = parentTrees.concat(linkTreeList)
          return cb(null, allTrees)
        })
      })
    })
  })
}

MultiTree.prototype._writeData = function (name, value, isLink, cb) {
  var self = this
  this._tree.ready(function (err) {
    if (err) return cb(err)
    var data
    if (!isLink) {
      data = messages.Node.encode({
        type: messages.Node.Type.DATA,
        value: self._codec.encode(value)
      })
    } else {
      Object.assign(value, {
        node: self._tree.version + 1,
        name: name,
        key: datEncoding.decode(value.key),
        value: self._codec.encode(value.value)
      })
      data = messages.Node.encode({
        type: messages.Node.Type.LINK,
        value: messages.LinkNode.encode(value)
      })
    }
    self._tree.put(name, data, cb)
  })
}

MultiTree.prototype._extractData = function (node, keepLink) {
  var value
  switch (node.type) {
    case (messages.Node.Type.DATA):
      value = node.value
      break
    case (messages.Node.Type.LINK):
      var link = messages.LinkNode.decode(node.value)
      this.links[link.node] = link
      value = keepLink ? link : link.value
      break
    default:
      throw new Error('Invalid node type:', node.type)
  }
  return this._codec.decode(value)
}

MultiTree.prototype._readNode = function (name, isNode, cb) {
  var self = this
  console.log('reading:', name, 'isNode:', isNode)
  this._tree.ready(function (err) {
    if (err) return cb(err)
    if (!isNode) {
      return self._tree.get(name, function (err, value) {
        if (err) return cb(err)
        return onnode(value)
      })
    }
    console.log('about to get!')
    self._tree.feed.get(name, function (err, bytes) {
      console.log('err:', err)
      if (err) return cb(err)
      var outer = atMessages.Node.decode(bytes)
      console.log('outer:', outer)
      return onnode(outer.value)
    })
  })
  function onnode (rawNode) {
    if (!rawNode) {
      var err = new Error(name + ' not found')
      err.notFound = true
      return cb(err)
    }
    var node = messages.Node.decode(rawNode)
    return cb(null, node)
  }
}

MultiTree.prototype._open = function (cb) {
  var self = this
  this._tree.ready(function (err) {
    if (err) return cb(err)
    if (self.opts.parents) {
      // Ensure that all parent keys/versions are recorded in /parents.
      return map(self.opts.parents,
        function (parent, next) {
          self._writeData(self._parentsPath(datEncoding.encode(parent.key)), parent, true, next)
        }, function (err) {
          if (err) return cb(err)
          init()
        })
    }
    init()
  })
  function init () {
    self.key = self._tree.feed.key
    self.version = self._tree.version
    self.feed = self._tree.feed
    self._getParentTrees(cb)
  }
}

MultiTree.prototype.link = function (name, target, opts, cb) {
  if (typeof opts === 'function') return this.link(name, target, {}, opts)
  var self = this

  if (target.version !== 0) target.version = target.version || opts.version
  target.path = target.path || opts.path || '/'

  name = self._entriesPath(name)
  this.ready(function (err) {
    if (err) return cb(err)
    return self._writeData(name, target, true, cb)
  })
}

MultiTree.prototype._treesWrapper = function (name, includeParents, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self._lock(function (release) {
      self._findLinkTrees(name, includeParents, function (err, trees) {
        if (err) return release(cb, err)
        return release(cb, null, trees)
      })
    })
  })
}

MultiTree.prototype.put = function (name, value, cb) {
  var self = this
  name = self._entriesPath(name)
  this._treesWrapper(name, false, function (err, trees) {
    if (err) return cb(err)
    if (trees.length === 0) {
      return self._writeData(name, value, false, function (err) {
        if (err) return cb(err)
        self.version = self._tree.version
        return cb()
      })
    }
    if (trees.length > 1) return cb(new Error('Trying to write to multiple symlinks.'))
    return trees[0].put(relative(name, trees[0]), value, cb)
  })
}

MultiTree.prototype.del = function (name, cb) {
  var self = this
  name = self._entriesPath(name)
  this._treesWrapper(name, false, function (err, trees) {
    if (err) return cb(err)
    if (trees.length === 0) {
      return self._tree.del(name, function (err) {
        if (err) return cb(err)
        self.version = self._tree.version
        return cb()
      })
    }
    if (trees.length > 1) return cb(new Error('Trying to delete from multiple symlinks.'))
    return trees[0].del(relative(name, trees[0]), cb)
  })
}

MultiTree.prototype.unlink = MultiTree.prototype.del

MultiTree.prototype._list = function (name, opts, cb) {
  if (typeof opts === 'function') return this.list(name, {}, opts)
  var self = this
  this._treesWrapper(name, true, function (err, trees) {
    if (err) return cb(err)

    if (trees.length <= self._parents.length) {
      trees.push(self._tree)
      return map(trees, function (tree, next) {
        return tree.list(name, opts, next)
      }, function (err, lists) {
        if (err) return cb(err)

        // Take the union of the parents and self trees.
        // Note: merge conflicts can be handled by the user after `get`, not here.
        return cb(null, listUnion(lists))
      })
    }
    return trees[trees.length - 1].list(relative(name, trees[trees.length - 1]), opts, cb)
  })
}

MultiTree.prototype.list = function (name, opts, cb) {
  return this._list(this._entriesPath(name), opts, cb)
}

MultiTree.prototype.get = function (name, opts, cb) {
  if (typeof opts === 'function') return this.get(name, {}, opts)
  var self = this
  name = self._entriesPath(name)
  this._treesWrapper(name, true, function (err, trees) {
    if (err) return cb(err)

    // If the content path is within a symlink, traverse into that link.
    if (trees.length > self._parents.length) {
      if (trees.length - self._parents.length > 1) {
        return cb(new Error('Trying to get from multiple symlinks.'))
      }
      return trees[trees.length - 1].get(relative(name, trees[trees.length - 1]), opts, cb)
    }

    // Otherwise, first check if the content is in our local tree.
    self._readNode(name, false, function (selfErr, selfNode) {
      if (selfErr && !selfErr.notFound) return cb(selfErr)
      if (selfNode) return onnode(selfNode)

      if (trees.length === 0) {
        if (selfErr) return cb(selfErr)
      }

      // If the content isn't local, check the parents.
      map(trees, function (tree, next) {
        return tree.get(name, opts, function (err, parentResult) {
          if (err && !err.notFound) return next(err)
          return next(null, parentResult)
        })
      }, function (err, parentResults) {
        if (err) return cb(err)
        var nonNullResults = parentResults.filter(function (x) { return x })
        if (nonNullResults.length === 0) return cb(selfErr)
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

  function onnode (node) {
    return cb(null, self._extractData(node))
  }
}

MultiTree.prototype.checkout = function (seq, opts) {
  this._tree = this._tree.checkout(seq, opts)
}

MultiTree.prototype.head = function (opts, cb) { }

MultiTree.prototype.history = null

function listUnion (lists) {
  // TODO: probably too many allocations.
  return Array.from(new Set(lists.reduce(function (l, item) {
    return l.concat(item)
  })))
}

// Name/path tagging of subtrees is hacky indeed.
function relative (name, subtree) {
  var relativePath = p.join(name.slice(subtree.nameTag.length))
  if (subtree.pathTag) relativePath = p.join(subtree.pathTag, relativePath)
  return relativePath
}

function normalize (name) {
  if (name[0] !== '/') name = '/' + name
  return name
}
