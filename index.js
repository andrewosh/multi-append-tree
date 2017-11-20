var p = require('path')
var events = require('events')
var each = require('async-each')
var series = require('async-series')
var collect = require('stream-collector')
var messages = require('./messages')
var tree = require('append-tree')

module.exports = MultiTree

var PARENT_ROOT = '/parents'
var LINK_METADATA_ROOT = '/metadata'
var LAYER_ROOT = '/layers'

function MultiTree (feed, treeFactory, opts) {
  if (!(this instanceof MultiTree)) return new MultiTree(feed, factory, opts)
  if (!feed) throw new Error('Feed must be non-null.')
  if (!factory || !(typeof factory === 'function')) throw new Error('Factory must be a non-null function that returns append-trees.')
  if (!opts) opts = {}
  this.opts = opts

  events.EventEmitter.call(this)

  this.checkout = opts.checkout
  this.feed = feed
  this._tree = tree(feed, opts)
  this._parents = opts.parents || []
  this._offset = opts.offset || 0

  // Set during initial indexing.
  this.version = null
  this.lastLayer = null
  this.linkIndex = []

  this.ready(function (err) {
    if (!err) self.emit('ready')
  })
}

inherits(MultiTree, events.EventEmitter)

MultiTree.prototype._parentPath = function (path) {
  return p.join(PARENT_ROOT, path)
}

MultiTree.prototype._linkMetadataPath = function (path) {
  return p.join(LINK_METADATA_ROOT, path)
}

MultiTree.prototype._layerPath = function (layer, path) {
  return p.join(LAYER_ROOT, layer, path)
}

MultiTree.prototype._addParentLayers = function (cb) {
  // Iff the feed is being created, add the parent layers.
  if (self._parents) {
    if ((self.feed.length - self._offset) > 0)
      return cb(new Error('Cannot add parents to an existing feed.'))
    each(self._parents, function (parent, next) {
      var path = self._parentPath(key)
      var obj = (typeof parent === 'string') ? { key: parent, live: false } : null
      self.tree.put(path, messages.Layer.encode(obj ? obj : parent), function (err) {
        if (err) return next(err)
      })
    }, function (err) {
      if (err) return cb(err)
      return onparents()
    }) 
  } else {
    return onparents()
  }

  function onparents () {
    tree.list(PARENT_ROOT, function (err, contents) {
      if (err) return cb(err)
      // The version is the number of updates, minus those updates that added parents
      self.version = self.tree.version - contents.length
      return cb()
    })
  }
}

// The link index is the only piece of metadata that cannot be sparsely synced, because
// the complete set of versioned links needs to be known at every write.
MultiTree.prototype._buildLinkIndex = function (cb) {
  var self = this
  this.tree.list(LINK_METADATA_ROOT, function (err, links) {
    if (err) return cb(err)
    // TODO: batch get?
    each(links, function (link, next) {
      self.get(link, function (err, rawMeta) {
        if (err) return next(err)
        var linkMeta = messages.LinkMetadataNode.decode(rawMeta)
        self.linkIndex[linkMeta.id] = linkMeta
        return next()
      })
    }, function (err) {
      return cb(err)
    })
  })
}

MultiTree.prototype._setLastLayer = function (cb) {
  var self = this
  this.tree.list(LAYER_ROOT, function (err, layers) {
    if (err) return cb(err)
    if (!layers) return self.pushLayer(cb) 
    self.lastLayer = messages.Layer.decode(layers[layers.length - 1]).id
    return cb(null, self.lastLayer)
  }
}

MultiTree.prototype._inflateLink = function (id, cb) {
  var meta = this.linkIndex[id]
  if (!meta) return cb(new Error('Trying to inflate a nonexistent link.'))
  var opts = meta.version ? : { version: meta.version } : null
  var linkTree = this.factory(meta.key, opts)
  linkTree.ready(function (err) {
    if (err) return cb(err)
    meta.tree = linkTree
    return cb(null, linkTree)
  })
}

MultiTree.prototype._writeToLastLayer = function (name, value, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    if (!self.lastLayer) 
  })
}

MultiTree.prototype.ready = function (cb) {
  var self = this
  this._tree.ready(function (err) {
    if (err) return cb(err)
    series([
      self._addParentLayers.bind(self),
      self._buildLinkIndex.bind(self),
      self._setLastLayer.bind(self)
    ], function (err) {
      return cb(err)
    })
  })
}

MultiTree.prototype.put = function (name, value, cb) {
  // Always insert into the most recent layer.
  // If there aren't any layers, create a base layer.
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    })
  })
}

MultiTree.prototype.list = function (name, opts, cb) { }

MultiTree.prototype.get = function (name, opts, cb) { }

MultiTree.prototype.checkout = function (seq, opts) { }

MultiTree.prototype.del = function (name, cb) { }

MultiTree.prototype.head = function (opts, cb) { }

MultiTree.prototype.history = null

MultiTree.prototype.link = null

MultiTree.prototype.removeLink = null

MultiTree.prototype.pushLayer = function (cb) {
  var self = this
  this.ready(function (err) {
    var linkId = self.linkIndex.length
    var layerTree = self.factory()
    layerTree.ready(function (err) {
      if (err) return cb(err)
      // TODO: These two puts really should be atomic, though if there's a failure after
      // only the metadata is inserted it's not a big problem.
      var linkMeta = self.linkIndex[linkId] = {
        id: linkId,
        key: layerTree.feed.key
      }
      self._tree.put(self._linkMetadataPath(linkId),
                     messages.LinkMetadataNode.encode(linkMeta),
        function (err) {
          if (err) return cb(null)
          linkMeta.tree = layerTree
          self._tree.put(self._layerPath(linkId), messages.Layer.encode({
            id: linkMeta.id
          }), function (err) {
            if (err) return cb(err)
          })
      })
    })
  })
}

MultiTree.prototype.popLayer = function (cb) {
  var self = this
  this.ready(function (err) {
    if (self.linkIndex.length === 0) return cb(new Error('No layers to pop.'))
    var linkId = self.linkIndex.length - 1
    self._tree.del(self._layerPath(linkId), function (err) {
      if (err) return cb(err)
      self.linkIndex.pop()
      return cb(null)
    })
  })
}
