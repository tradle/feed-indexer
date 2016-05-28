
var subdown = require('subleveldown')
var createChangesFeed = require('changes-feed')
var levelup = require('levelup')
var memdown = require('memdown')
var dogNames = require('dog-names')
var DB_COUNTER = 0
var dbOpts = { valueEncoding: 'json' }
var memdb = function () {
  return levelup('' + DB_COUNTER++, { db: memdown, valueEncoding: 'json' })
}

var indexer = require('./')
var feed = createChangesFeed(memdb())
var msgWorker = function (change, cb) {
  if (!change.value.status) return cb()

  var msg = change.value
  var map = {
    from: msg.from,
    status: msg.status
  }

  var batch = mapToBatch(map)
  cb(null, {
    type: 'put',
    key: msg.uid,
    value: msg,
    // db: msgDB.main
  }, batch)
}

var userWorker = function (change, cb) {
  if (!change.value.name) return cb()

  var map = {}

  var user = change.value
  var batch = user.pubkeys.map(key => {
    return {
      type: 'put',
      key: 'fingerprint',
      value: key.fingerprint
    }
  })
  .concat(user.pubkeys.map(key => {
    return {
      type: 'put',
      key: 'pubkey',
      value: key.pub
    }
  }))

  cb(null, {
    type: 'put',
    key: user.name,
    value: user,
    db: userDB.main
  }, batch)
}

var msgDB = createIndexedDB(feed, msgWorker)
var userDB = createIndexedDB(feed, userWorker)

var msgs = new Array(20).fill(null).map(function (val, i) {
  var from = dogNames.male[i / 5 | 0]
  var to = dogNames.female[i]
  return {
    uid: from + to,
    from: from,
    to: to,
    message: 'hey ' + to,
    status: i % 2 === 0 ? 'sent' : 'pending'
  }
})

var users = require('./users').slice(0, 10).map(function (u, i) {
  u = u.pub
  u.name = dogNames.male[i]
  return u
})


// msgDB.index.by('status', 'sent', { live: true }).on('data', console.log)
// msgDB.main.createReadStream({ live: true }).on('data', console.log)
// userDB.main.createReadStream({ live: true }).on('data', console.log)
userDB.index.by('fingerprint', users[0].pubkeys[0].fingerprint, { live: true }).on('data', console.log)

setTimeout(function () {
  users.concat(msgs).forEach(function (item) {
    feed.append(item)
  })
}, 200)

// msgDB.index.get('status', 'sent', console.log)

function createIndexedDB (feed, worker) {
  var db = memdb()
  return indexer({
    db: db,
    feed: feed,
    worker: worker
  })
}

function mapToBatch (map, db) {
  var batch = []
  for (var k in map) {
    batch.push({
      type: 'put',
      key: k,
      value: map[k],
      // db: db
    })
  }

  return batch
}
