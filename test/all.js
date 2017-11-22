var test = require('tape')
var async = require('async')
var multitree = require('..')

var datEncoding = require('dat-encoding')
var ram = require('random-access-memory')
var core = require('hypercore')
var tree = require('append-tree')

var trees = {}

function makeId (key, version) {
  var keystring = datEncoding.encode(key)
  return (version) ? keystring + '-' + version : keystring
}

function simpleFactory (key, version, opts) {
  opts = opts || {}
  opts.version = version
  var id = makeId(key, version)
  // reuse trees w/ same key and version instead of replicating
  if (trees[id]) {
    console.log('REUSING')
    return trees[id]
  }

  var newTree = tree(core(ram, key, opts), opts)
  newTree.ready(function (err) {
    if (err) throw err
    trees[id] = newTree
  })
  return newTree
}

function applyOps (tree, list, cb) {
  async.series(
    list.map(function (l) {
      return function (next) {
        switch (l.op) {
          case 'put':
            tree.put(l.name, l.value, next)
            break
          case 'del':
            tree.del(l.name, l.value, next)
            break
          case 'link':
            tree.link(l.name, l.target, next)
            break
          default:
            throw new Error('bad operation')
        }
      }
    }),
  function (err) {
    if (err) return cb(err)
    return cb()
  })
}

function create (opts, cb) {
  if (typeof opts === 'function') return create({}, opts)
  var t = multitree(tree(core(ram), opts), simpleFactory, opts)
  t.ready(function (err) {
    if (err) return cb(err)
    trees[makeId(t.feed.key, t.feed.version)] = t._tree
    return cb(null, t)
  })
}

function createTwo (opts, cb) {
  if (typeof opts === 'function') return createTwo({}, opts)
  var t1 = multitree(tree(core(ram), opts), simpleFactory)
  var t2 = multitree(tree(core(ram), opts), simpleFactory)
  t1.ready(function (err) {
    if (err) return cb(err)
    t2.ready(function (err) {
      if (err) return cb(err)
      trees[makeId(t1.feed.key, t1.feed.version)] = t1._tree
      trees[makeId(t2.feed.key, t2.feed.version)] = t2._tree
      return cb(null, t1, t2)
    })
  })
}

function getEqual (t, tree, name, value) {
  tree.get(name, function (err, result) {
    t.error(err)
    t.same(result, value)
  })
}

/*
function createWithParent (parentOps, cb) {
  var t1 = multitree(tree(core(ram)), simpleFactory)
  t1.ready(function (err) {
    if (err) return cb(err)
    applyOps(t1, parentOps, function (err) {
      if (err) return cb(err)
      var t2 = multitree(tree(core(ram)), simpleFactory, {
        parents: [
          { key: t1.feed.key, version: t1.version }
        ]
      })
      t2.ready(function (err) {
        if (err) return cb(err)
        return cb(null, t1, t2)
      })
    })
  })
}
*/

test('single archive get/put', function (t) {
  t.plan(4)
  create({ valueEncoding: 'utf-8' }, function (err, mt) {
    t.error(err)
    mt.put('/hey', 'there', function (err) {
      t.error(err)
      getEqual(t, mt, '/hey', 'there')
    })
  })
})

test('single archive list', function (t) {
  create(function (err, mt) {
    t.error(err)
    applyOps(mt, [
      { op: 'put', name: '/a/1', value: 'hello' },
      { op: 'put', name: '/a/2', value: 'world' },
      { op: 'put', name: '/a/3', value: 'goodbye' }
    ], function (err) {
      t.error(err)
      mt.list('/a', function (err, list) {
        t.error(err)
        t.deepEqual(list, ['1', '2', '3'])
        t.end()
      })
    })
  })
})

test('two archives symlinked', function (t) {
  t.plan(9)
  createTwo(function (err, mt1, mt2) {
    t.error(err)
    applyOps(mt1, [
      { op: 'put', name: '/a', value: 'hello' },
      { op: 'put', name: '/b', value: 'goodbye' }
    ], function (err) {
      t.error(err)
      applyOps(mt2, [
        { op: 'link', name: 'mt1', target: { key: mt1.feed.key } },
        { op: 'put', name: '/a', value: 'new hello' }
      ], function (err) {
        t.error(err)
        getEqual(t, mt2, '/a', Buffer.from('new hello'))
        getEqual(t, mt2, '/mt1/b', Buffer.from('goodbye'))
        getEqual(t, mt2, '/mt1/a', Buffer.from('hello'))
      })
    })
  })
})
test('two archives symlinked, symlink overwritten', function (t) {
  t.plan(11)
  createTwo(function (err, mt1, mt2) {
    t.error(err)
    applyOps(mt1, [
      { op: 'put', name: '/a', value: 'hello' },
      { op: 'put', name: '/b', value: 'goodbye' }
    ], function (err) {
      t.error(err)
      applyOps(mt2, [
        { op: 'link', name: 'mt1', target: { key: mt1.feed.key } },
        { op: 'put', name: '/a', value: 'new hello' },
        { op: 'put', name: 'mt1', value: 'overwrite' }
      ], function (err) {
        t.error(err)
        getEqual(t, mt2, '/a', Buffer.from('new hello'))
        getEqual(t, mt2, '/mt1/b', undefined)
        getEqual(t, mt2, '/mt1/a', undefined)
        getEqual(t, mt2, 'mt1', Buffer.from('overwrite'))
      })
    })
  })
})

test.skip('two archives symlinked, writing through outer archive', function (t) {
  t.plan(11)
  createTwo(function (err, mt1, mt2) {
    t.error(err)
    applyOps(mt1, [
      { op: 'put', name: '/a', value: 'hello' },
      { op: 'put', name: '/b', value: 'goodbye' }
    ], function (err) {
      t.error(err)
      applyOps(mt2, [
        { op: 'link', name: 'mt1', target: { key: mt1.feed.key } },
        { op: 'put', name: '/a', value: 'new hello' },
        { op: 'put', name: '/mt1/c', value: 'cat entry' }
      ], function (err) {
        t.error(err)
        getEqual(t, mt2, '/a', Buffer.from('new hello'))
        getEqual(t, mt2, '/mt1/b', Buffer.from('goodbye'))
        getEqual(t, mt2, '/mt1/a', Buffer.from('hello'))
        getEqual(t, mt2, '/mt1/c', Buffer.from('cat entry'))
      })
    })
  })
})
