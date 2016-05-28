var test = require('tape')
var changes = require('changes-feed')
var nextDB = require('./helpers').nextDB
var indexer = require('..')

test('multi', function(t) {
  t.plan(4)

  var db = nextDB()
  var feed = changes(nextDB())
  var indexed = indexer({
    primaryKey: 'id',
    db: db,
    feed: feed
  })

  var byFingerprint = indexed.by('fingerprint', function (val) {
    return val.keys.map(function (key) {
      return key.fingerprint + indexed.separator + val.id
    })
  })

  var identity = {
    id: '1',
    keys: [
      { fingerprint: 'a' },
      { fingerprint: 'b' }
    ]
  }

  feed.append(identity)

  ;['a', 'b'].forEach(finger => {
    byFingerprint.findOne(finger, function(err, result) {
      t.error(err)
      t.deepEqual(result, identity)
    })
  })
})
