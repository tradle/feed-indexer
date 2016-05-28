
const extend = require('xtend')
const changes = require('changes-feed')
const levelup = require('levelup')
const memdown = require('memdown')
const hydra = require('hydration')()
const indexer = require('./indexer')
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
const byKeys = indexedDB.by('fingerprint', function (val) {
  return val.firstName + val.lastName + '!' + val.id
})

// setTimeout(function () {
  // db.createReadStream().on('data', console.log)
  // indexedDB.createReadStream().on('data', console.log)

// console.log('eq Mark1')
// byFirstName.createReadStream({ eq: 'Mark1', live: true }).on('data', console.log)

// console.log('[Bill, Ted]')
// byFirstName.createReadStream({ gte: 'Bill', lte: 'Ted', live: true }).on('data', console.log)

// byFirstName.createReadStream({ gt: 'Mark1', live: true }).on('data', console.log)
// }, 100)

byFirstName.createReadStream({ gte: 'Ted', live: true }).on('data', console.log)

feed.append({
  id: 'mv1',
  firstName: 'Mark1',
  lastName: 'Markaa'
})

feed.append({
  id: 'mv2',
  firstName: 'Mark2',
  lastName: 'Markaa'
})

feed.append({
  id: 'gv',
  firstName: 'Gene',
  lastName: 'Genkaa'
})

feed.append({
  id: 'bsp',
  firstName: 'Bill',
  lastName: 'Preston'
})

feed.append({
  id: 'ttl',
  firstName: 'Ted',
  lastName: 'Logan'
})

feed.append({
  id: 'ttl',
  firstName: 'Teddy',
  lastName: 'Logan'
})

feed.append({
  id: 'ttd',
  firstName: 'Teddy',
  lastName: 'Doofus'
})
