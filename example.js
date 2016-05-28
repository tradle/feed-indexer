
const extend = require('xtend')
const changes = require('changes-feed')
const levelup = require('levelup')
const memdown = require('memdown')
const collect = require('stream-collector')
const indexer = require('./')
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

const byFirstName = indexedDB.by('firstName')
const byLastName = indexedDB.by('lastName')
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
