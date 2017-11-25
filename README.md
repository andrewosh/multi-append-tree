# multi-append-tree
[![CircleCI](https://circleci.com/gh/andrewosh/multi-append-tree/tree/master.svg?style=svg&circle-token=cb9131565fc0036cfa44bb7881c7157c7e7830a3)](https://circleci.com/gh/andrewosh/multi-append-tree/tree/master)

multi-append-tree is a wrapper around an [append-tree](https://github.com/mafintosh/append-tree) that supports nesting other append-trees within the main tree (or multi-append-trees). Tree, tree, tree:

1. Lightweight __forking__ and __layering__ operations are supported through a `parents` relationship. If an entry can't be found in the main tree, the search will recurse into all parents.
2. Cross-tree __symlinking__ (of both live and versioned trees) is supported through special `Link` entries. 

No external indexing is required, and subtrees are instantiated dynamically when first needed.

Oh, and since multiple parents are allowed, `get` or `list` operations might return conflicting entries -- resolution is left to you! This might be confusing, but allowing for multiple parents is useful during merges. 

### Installation
`npm i multi-append-tree`

### Usage
`multi-append-tree` wraps a set of nested `append-trees` and can b
```js
var hypercore = require('hypercore')
var tree = require('append-tree')
var multitree = require('multi-append-tree')
var ram = require('random-access-memory')

// An append-tree factory function is required, because multi-append-tree constructs subtrees dynamically.
var factory = function (key, version, cb) {
  var core = (key) ? hypercore(ram, key, { version: version }) : hypercore(ram)
  return cb(null, tree(core))
}

var baseTree = factory()
var mt = multitree(baseTree, factory)
```

### API
multi-append-tree implements the append-tree API, with the addition of the methods described below.

#### `var mt = multitree(tree, factory, opts)`

Since multi-append-trees need to dynamically create sub-append-trees, it needs to be provided with a factory function.

`baseTree` is the primary append-tree that stores content and links.
`factory` is a funtion that takes a key and (optionally) a version, and returns an append-tree:

`opts` are append-tree options, but with the important addition of:
```js
{ 
  parents: [] // A list of { key: key, version: version} objects specifying parent append-trees.
}
```

#### `link(name, target, opts cb)`

Create a Link record that references another append-tree (specified by `target`)

`target` can be either:
1. An object of the form `{ key: <key>, path: <path> }`
2. A string of the form `dat://<key>/<path>` or `<key>/<path>` (assuming the latter form corresponds to a propely encoded dat key).
`opts` is an optional object of the form:
```js
{
  version: <unspecified, as in latest> // The desired tree version, if the tree should be versioned (static).
}
```
#### `unlink(name, target, cb)`
