var p = require('path')
var events = require('events')
var map = require('async-each')
var find = require('lodash.find')
var collect = require('stream-collector')
var messages = require('./messages')
var tree = require('append-tree')

module.exports = MultiTree

var PARENT_ROOT = '/parents'
var ARCHIVE_METADATA_ROOT = '/metadata'
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
  this._parents = opts.parents || []
  this._offset = opts.offset || 0

  // Set during initial indexing.
  this.version = null
  this.currentLayer = null
  this.parents = []

  self._processed = 0
  this.linkIndex = []

  this.ready(function (err) {
    if (!err) self.emit('ready')
  })
}

inherits(MultiTree, events.EventEmitter)

MultiTree.prototype._parentPath = function (path) {
  return p.join(PARENT_ROOT, path)
}

MultiTree.prototype._archiveMetadataPath = function (path) {
  return p.join(ARCHIVE_METADATA_ROOT, path)
}

MultiTree.prototype._entriesPath = function (path) {
  return p.join(ENTRIES_ROOT, path)
}

MultiTree.prototype._syncIndex = function (cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    this._tree.list(ARCHIVE_METADATA_ROOT, function (err, archives) {
      if (err) return cb(err)
      // TODO: batch get?
      map(archives.slice(self._processed), function (archive, next) {
        self.get(archive, function (err, rawMeta) {
          if (err) return next(err)
          var archiveMeta = messages.ArchiveMetadataNode.decode(rawMeta)
          var existingMeta = self.archiveIndex[archiveMeta.id]
          if (!existingMeta) {
            self.archiveIndex[archiveMeta.id] = archiveMeta
          } else {
            Object.assign(existingMeta, archiveMeta)
          }
          if (archiveMeta.parent) {
            // Parents should be immediately inflated.
            self._inflateLink(archiveMeta.id, true, function (err) {
              if (err) return next(err)
              return next(null, archiveMeta)
            })
          }
          return next(null, archiveMeta)
        })
      }, function (err, archiveMetas) {
        if (err) return cb(err)
        // Do a second pass to construct layer/parents lists.
        archiveMetas.forEach(function (archive) {
          if (archive.parent) {
            self.parents.push(archive)
            return
          }
          if (archive.type === messages.ArchiveMetadataNode.Type.LAYER) {
            if (!self.currentLayer) self.currentLayer = archive
            else if (!archive.prev) {
              // If a layer update deletes it's prev pointer, it:
              // a) must be the current top layer
              // b) should be removed from the top and replaced by its current prev.
              if (archive.id !== self.currentLayer.id)
                throw new Error('Bad layer update: trying to pop interior layer.')
              var curPrev = self.archiveIndex[archive.prev]
              self.archiveIndex[archive.id].prev = null
              self.currentLayer = curPrev
            } else {
              // If a layer update declared a new prev, it is the new top layer.
              var oldTop = self.currentLayer
              self.currentLayer = archive
              self.currentLayer.prev = oldTop
            }
          }
        })
        self._processed += archiveMetas.length
      })
    })
  })
}

MultiTree.prototype._inflateLink = function (id, isMulti, cb) {
  var meta = this.linkIndex[id]
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

MultiTree.prototype._registerArchive = function (meta, cb) {
  if (typeof opts === 'function') return self._createNewArchive(meta, {}, opts)
  var self = this
  this._syncIndex(function (err) {
    if (err) return cb(err)
    var id = self.archiveIndex.length
    var subtree = (meta.key) ? self.factory(meta.key, meta.version) : self.factory()
    subtree.ready(function (err) {
      if (err) return cb(err)
      // TODO: maybe should not edit meta in place?
      if (!meta.key) meta.key = subtree.feed.key
      meta.id = id
      self.archiveIndex[id] = meta
      self._tree.put(self._archiveMetadataPath(id),
                     messages.ArchiveMetadataNode.encode(meta),
        function (err) {
          if (err) return cb(err)          
          meta.tree = subtree
          return cb(null, meta)
        })
    })
  })
}

MultiTree.prototype._ensureLayer = function (cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    if (!self.currentLayer) return self.pushLayer(cb)
    return cb(null)
  })
}

MultiTree.prototype._writeUpdate = function (name, versioned, cb) {
  var self = this
  this._ensureLayer(function (err) {
    if (err) return cb(err)
    // TODO: handle "versioned" writes, and come up with a better term.
    var update = messages.UpdateNode.encode({
      id: self.currentLayer.id
    })
    self.tree.put(name, update, function (err) {
      return cb(err)
    })
  }
}

MultiTree.prototype.ready = function (cb) {
  var self = this
  this._tree.ready(function (err) {
    if (err) return cb(err)
    self._syncIndex(function (err) {
      return cb(err)
    })
  })
}

MultiTree.prototype.put = function (name, value, versioned, cb) {
  if (typeof versioned === 'function') return this.put(name, value, false, versioned)
  this._writeUpdate(name, versioned, function  (err) {
    if (err) return cb(err)
    self.currentLayer.tree.put(name, value, cb)
  })
}

MultiTree.prototype.del = function (name, versioned, cb) {
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
