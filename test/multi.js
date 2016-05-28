var test = require('tape')
var sub = require('subleveldown')
var memdown = require('memdown')
var levelup = require('levelup')
var changes = require('changes-feed')
var level = function (path, opts) {
  opts = opts || {}
  opts.valueEncoding = 'json'
  opts.db = memdown
  return levelup(path, opts)
}

var indexer = require('..')

test('multi', function(t) {
  t.plan(4)

  var db = level('db')
  var feed = changes(level('feed'))
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
