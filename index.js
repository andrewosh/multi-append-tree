var p = require('path')
var events = require('events')
var map = require('async-each')
var thunky = require('thunky')
var lock = require('mutexify')
var inherits = require('inherits')
var datEncoding = require('dat-encoding')
var messages = require('./messages')

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
  var self = this
  var mergedOpts = Object.assign({}, this.opts, opts)
  console.log('mergedOpts:', mergedOpts)
  this._factory(key, version, mergedOpts, function (err, tree) {
    console.log('AFTER FACTORY, err:', err, 'tree.version:', tree.version)
    if (err) return cb(err)
    var linkTree = MultiTree(tree, self._factory)
    console.log('entering ready')
    linkTree.ready(function (err) {
      if (err) return cb(err)
      return cb(null, linkTree)
    })
  })
}

MultiTree.prototype._getParentTrees = function (cb) {
  var self = this
  console.log('in get parent rees')
  // TODO: this path request and the subsequent list request should be atomic.
  // WARNING: possible race condition in _parentsNode if these ops aren't atomic.
  this._tree.path(PARENTS_ROOT, function (err, path) {
    if (err && err.notFound) return cb(null, [])
    if (err) return cb(err)
    console.log('self._parentsNode:', self._parentsNode, 'path', path)
    if (self._parentsNode && (self._parentsNode === path[0])) {
      // The list of parents hasn't changed, so the node indices can be reused.
      console.log('REUSING PARENT NODE INDICES')
      return onlinks()
    }
    console.log('LISTING PARENTS ROOT')
    self._tree.list(PARENTS_ROOT, function (err, parentNames) {
      console.log('parentNames:', parentNames)
      if (err && err.notFound) return cb(null, [])
      if (err) return cb(err)
      self._parentsNode = path[0]
      self._parents = []
      console.log('self._parentsNode:', self._parentsNode)
      map(parentNames, function (name, next) {
        console.log('READING PARENT LINK NAME:', p.join(PARENTS_ROOT, name))
        self._readLink(p.join(PARENTS_ROOT, name), false, function (err, link) {
          if (err) return next(err)
          console.log('FOUND PARENT LINK:', link.name)
          return next(null, link)
        })
      }, function (err, links) {
        if (err) return cb(err)
        self._parents = links
        console.log('self._parents:', self._parents)
        return onlinks()
      })
    })
  })
  function onlinks () {
    console.log('in onlinks')
    if (self._parents.length === 0) return cb()
    console.log('BUILDING PARENT TREES!!')
    return map(self._parents, function (parent, next) {
      console.log('BUILDING A TREE')
      self._getTreeForNode(parent.node, parent.name, true, next)
    }, cb)
  }
}

MultiTree.prototype._getTreeForNode = function (node, name, isParent, cb) {
  if (typeof isParent === 'function') return this._getTreeForNode(node, name, false, isParent)
  var self = this
  var link = this.links[node]
  if (link) return onlink(link)
  console.log('READING NODE:', node, 'and name:', name)
  this._readLink(node, true, function (err, link) {
    if (err && err.notFound) return cb(null)
    if (err) return cb(err)
    return onlink(link)
  })
  function onlink (link) {
    console.log('GOT A LINK FOR NODE:', node, 'AND NAME:', name)
    if (!isParent && ((name === link.name) || !name.startsWith(link.name))) return cb(null)
    if (link.tree) return cb(null, link.tree)
    self._inflateTree(link.key, link.version, function (err, tree) {
      if (err) return cb(err)
      link.tree = tree
      // TODO: This should be done better.
      tree.nameTag = link.name
      return cb(null, link.tree)
    })
  }
}

MultiTree.prototype._findLinkTrees = function (name, readParents, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    console.log('LOOKING UP:', name, 'tree version:', self._tree.version, 'key:', self._tree.feed.key)
    self._tree.path(name, function (err, path) {
      console.log('path:', path, 'key:', self._tree.feed.key)
      if (err && err.notFound) return cb(null, [])
      if (err) return cb(err)

      var nodeIndex = path[path.length - 1]

      self._getTreeForNode(nodeIndex, name, function (err, tree) {
        if (err) return cb(err)
        console.log('TREE AT NODE:', nodeIndex, 'is not null?', tree && tree.version)
        var linkTreeList = (tree) ? [tree] : []
        console.log('linkTreeList.length:', linkTreeList.length)
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

MultiTree.prototype._writeLink = function (name, target, cb) {
  var self = this
  this._tree.ready(function (err) {
    if (err) return cb(err)
    self._lock(function (release) {
      if (err) return release(cb, err)
      target.node = self._tree.version + 1
      target.name = name
      console.log('WROTE LINK AT NODE:', target.node, 'with name:', target.name)
      self._tree.put(name, messages.LinkNode.encode(target), function (err) {
        if (err) return release(cb, err)
        self.version = self._tree.version
        return release(cb)
      })
    })
  })
}

MultiTree.prototype._readLink = function (name, isNode, cb) {
  var self = this
  this._tree.ready(function (err) {
    if (err) return cb(err)
    if (!isNode) {
      return self._tree.get(name, function (err, value) {
        if (err) return cb(err)
        return onlink(value)
      })
    }
    // TODO: this is probably not kosher...
    // This is the only spot where the append-tree abstraction breaks down.
    // messages.Node is copied from append-tree -- peer dependency makes more sense.
    self._tree.feed.get(name, function (err, bytes) {
      if (err) return cb(err)
      var outer = messages.Node.decode(bytes)
      return onlink(outer.value)
    })
  })
  function onlink (rawLink) {
    try {
      var link = messages.LinkNode.decode(rawLink)
    } catch (err) {
      // If there's an error, then this is not a link node.
      err.notFound = true
      return cb(err)
    }
    self.links[link.node] = link
    return cb(null, link)
  }
}

MultiTree.prototype._open = function (cb) {
  var self = this
  console.log('IN OPEN')
  this._tree.ready(function (err) {
    console.log('after error now, err:', err)
    if (err) return cb(err)
    if (self.opts.parents) {
      return map(self.opts.parents,
        function (parent, next) {
          self._writeLink(self._parentsPath(datEncoding.encode(parent.key)), parent, next)
        }, function (err) {
          if (err) return cb(err)
          console.log('SETTING FEED')
          init()
        })
    }
    console.log('CALLING INIT')
    init()
  })
  function init () {
    self.version = self._tree.version
    self.feed = self._tree.feed
    self._getParentTrees(cb)
  }
}

MultiTree.prototype.link = function (name, target, cb) {
  var self = this
  name = self._entriesPath(name)
  this.ready(function (err) {
    if (err) return cb(err)
    return self._writeLink(name, target, cb)
  })
}

MultiTree.prototype._treesWrapper = function (name, includeParents, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self._lock(function (release) {
      self._findLinkTrees(name, includeParents, function (err, trees) {
        if (err) return release(cb, err)
        console.log('In WRAPPER, trees.length:', trees.length)
        return release(cb, null, trees)
      })
    })
  })
}

MultiTree.prototype.put = function (name, value, cb) {
  var self = this
  name = self._entriesPath(name)
  console.log('PUTTING NAME:', name, 'value:', value, 'which is:', normalize(name))
  this._treesWrapper(name, false, function (err, trees) {
    if (err) return cb(err)
    if (trees.length === 0) {
      console.log('FINAL PART OF PUT:', name, 'key:', self._tree.feed.key)
      return self._tree.put(name, value, function (err) {
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
      console.log('TREES LENGTH:', trees.length)
      return map(trees, function (tree, next) {
        return tree.list(name, opts, next)
      }, function (err, lists) {
        if (err) return cb(err)
        console.log('RESULT LISTS:', lists)
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
    if (trees.length > self._parents.length) {
      if (trees.length - self._parents.length > 1) { return cb(new Error('Trying to get from multiple symlinks.')) }
      console.log('GETTING FROM SYMLINK')
      return trees[trees.length - 1].get(relative(name, trees[trees.length - 1]), opts, cb)
    }
    self._tree.get(name, opts, function (err, selfValue) {
      if (err && !err.notFound) return cb(err)
      if (selfValue || (trees.length === 0)) return cb(null, selfValue)
      map(trees, function (tree, next) {
        return tree.get(name, opts, function (err, parentResult) {
          if (err && !err.notFound) return cb(err)
          return cb(null, parentResult)
        })
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
  return Array.from(new Set(lists.reduce(function (l, item) {
    return l.concat(item)
  })))
}

// Name tagging of subtrees is hacky indeed.
function relative (name, subtree) {
  return name.slice(subtree.nameTag.length)
}

function normalize (name) {
  if (name[0] !== '/') name = '/' + name
  return name
}
