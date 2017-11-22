var test = require('tape')
var each = require('async-each')
var multitree = require('..')

var ram = require('random-access-memory')
var core = require('hypercore')
var tree = require('append-tree')

function simpleFactory (key, version, cb) {
  var t = tree(core(ram, key, { version: version }))
  t.ready(function (err) {
    if (err) return cb(err)
    return cb(null, t)
  })
}

function create (cb) {
  var t = multitree(tree(core(ram)), simpleFactory)
  t.ready(function (err) {
    if (err) return cb(err)
    return cb(null, t)
  })
}

function putList (tree, list, cb) {
  each(list, function (l, next) {
    tree.put(l, 'hey', next)
  }, function (err) {
    return cb(err)
  })
}

test('single archive get/put', function (t) {
  create(function (err, mt) {
    t.error(err)
    mt.put('/hey', 'there', function (err) {
      t.error(err)
      mt.get('/hey', { valueEncoding: 'utf-8' }, function (err, contents) {
        t.error(err)
        t.same(contents, Buffer.from('there'))
        t.end()
      })
    })
  })
})

test('single archive list', function (t) {
  create(function (err, mt) {
    t.error(err)
    putList(mt, ['/a', '/a/1', '/a/2', '/a/3', 'a/4'], function (err) {
      t.error(err)
      mt.list('/a', function (err, list) {
        t.error(err)
        t.deepEqual(list, ['1', '2', '3', '4'])
        t.end()
      })
    })
  })
})
