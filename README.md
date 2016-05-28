# feed-indexer

Based on [level-secondary](https://github.com/juliangruber/level-secondary) and [changes-index](https://github.com/substack/changes-index). 

Borrows API style from level-secondary, but unlike level-secondary, builds indexes on a feed rather than a database.

Borrows some index-processing logic from changes-index, but unlike changes-index, here you write to the feed directly, and can define custom indexing rules.

# Usage

```js
const changes = require('changes-feed')
const levelup = require('levelup')
const memdown = require('memdown')
const collect = require('stream-collector')
const indexer = require('feed-indexer')
const level = function (path, opts) {
  opts = opts || {}
  if (!opts.db) opts.db = memdown
  return levelup(path, opts)
}

const levelOpts = { valueEncoding: 'json' }
const feed = changes(level('feed', levelOpts))
const db = level('blah', levelOpts)
const indexedDB = indexer({
  primaryKey: 'id',
  feed: feed,
  db: db
})

// default: index by a property value
const byFirstName = indexedDB.by('firstName')
const byLastName = indexedDB.by('lastName')

// custom: index by whatever
const byFingerprint = indexedDB.by('fingerprint', function (val) {
  return val.keys.map(function (key) {
    return key.fingerprint + indexedDB.separator + val.id
  })
})

feed.append({
  id: 'pr',
  firstName: 'Patrick',
  lastName: 'Rothfuss',
  keys: [
    { fingerprint: 'abc' },
    { fingerprint: 'def' },
  ]
})

feed.append({
  id: 'ja',
  firstName: 'Joe',
  lastName: 'Abercrombie',
  keys: [
    { fingerprint: 'ghi' },
    { fingerprint: 'jkl' },
  ]
})

feed.append({
  id: 'ey',
  firstName: 'Eliezer',
  lastName: 'Yudkowsky',
  keys: [
    { fingerprint: 'mno' },
    { fingerprint: 'pqr' },
  ]
})

collect(byFirstName.createReadStream({ gte: 'Joe' }), function (err, results) {
  console.log('by first name >= Joe:')
  console.log(results)
  console.log()

  byFingerprint.findOne({ lt: 'fgh' }, function (err, result) {
    console.log('first by fingerprint < fgh:')
    console.log(result)
    console.log()

    byLastName.findOne('Yudkowsky', function (err, result) {
      console.log('by lastName === "Yudkowsky":')
      console.log(result)
      console.log()
    })
  })
})
```
