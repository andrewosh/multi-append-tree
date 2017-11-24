var test = require('tape')
var async = require('async')
var multitree = require('..')

var ram = require('random-access-memory')
var core = require('hypercore')
var tree = require('append-tree')

var trees = {}

function simpleFactory (key, version, opts, cb) {
  opts = opts || {}
  // reuse trees w/ same key and version instead of replicating
  // console.log('TREES[KEY]:', trees[key], 'key:', key)
  if (trees[key]) console.log('REUSING TREE FOR KEY:', key)
  var topTree = (trees[key]) ? trees[key] : tree(core(ram, key, opts))
  console.log('VERSION IS:', version)
  if (trees[key]) {
    console.log('REUSING VERSION:', trees[key].version)
  }
  var t = (version) ? topTree.checkout(version, opts) : topTree
  t.ready(function (err) {
    if (err) return cb(err)
    if (!trees[key]) trees[key] = topTree
    return cb(null, t)
  })
}

function applyOps (tree, list, cb) {
  async.series(
    list.map(function (l) {
      return function (next) {
        console.log('l:', JSON.stringify(l))
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
    }), cb)
}

function create (opts, cb) {
  if (typeof opts === 'function') return create({}, opts)
  var it = tree(core(ram), opts)
  var t = multitree(it, simpleFactory, opts)
  t.ready(function (err) {
    if (err) return cb(err)
    if (!trees[it.feed.key]) trees[it.feed.key] = it
    return cb(null, t)
  })
}

function createTwo (opts, cb) {
  if (typeof opts === 'function') return createTwo({}, opts)
  var it1 = tree(core(ram), opts)
  var it2 = tree(core(ram), opts)
  var t1 = multitree(it1, simpleFactory)
  var t2 = multitree(it2, simpleFactory)
  t1.ready(function (err) {
    if (err) return cb(err)
    t2.ready(function (err) {
      if (err) return cb(err)
      if (!trees[it1.feed.key]) trees[it1.feed.key] = it1
      if (!trees[it2.feed.key]) trees[it2.feed.key] = it2
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

function createWithParent (parentOps, childOps, cb) {
  var it1 = tree(core(ram))
  var parent = multitree(it1, simpleFactory)
  parent.ready(function (err) {
    if (err) return cb(err)
    applyOps(parent, parentOps, function (err) {
      if (err) return cb(err)
      if (!trees[it1.feed.key]) trees[it1.feed.key] = it1
      var it2 = tree(core(ram))
      var child = multitree(it2, simpleFactory, {
        parents: [
          { key: parent.feed.key }
        ]
      })
      child.ready(function (err) {
        if (err) return cb(err)
        applyOps(child, childOps, function (err) {
          if (err) return cb(err)
          console.log('IT1 FEED KEY:', it1.feed.key)
          console.log('IT1 FEED LENGTH:', it1.feed.length)
          if (!trees[it2.feed.key]) trees[it2.feed.key] = it2
          return cb(null, parent, child)
        })
      })
    })
  })
}

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
        { op: 'link', name: '/mt1', target: { key: mt1.feed.key } },
        { op: 'put', name: '/a', value: 'new hello' },
        { op: 'put', name: '/mt1', value: 'overwrite' }
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

test('two archives symlinked, writing through outer archive', function (t) {
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

test('two archives with parent-child relationship, list root', function (t) {
  createWithParent([
    { op: 'put', name: '/a', value: 'hello' },
    { op: 'put', name: '/b', value: 'goodbye' }
  ], [
    { op: 'put', name: '/c', value: 'cat' }
  ], function (err, mt1, mt2) {
    t.error(err)
    console.log('BEFORE LIST')
    mt2.list('/', function (err, contents) {
      t.error(err)
      t.deepEqual(contents, ['a', 'b', 'c'])
    })
  })
})
