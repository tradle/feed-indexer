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

test('get', function(t) {
  t.plan(2)

  var db = level('db')
  var feed = changes(level('feed'))
  var indexed = indexer({
    primaryKey: 'id',
    db: db,
    feed: feed
  })

  var byTitle = indexed.by('title')
  var byLength = indexed.by('length', function(post){
    return post.body.length
  })

  var post = {
    id: '1337',
    title: 'a title',
    body: 'lorem ipsum'
  }

  feed.append(post)

  byLength.createReadStream({
    start: 10,
    end: 20
  })
  .on('data', function(data) {
    t.deepEqual(data, post)
  })

  byLength.createReadStream({
    start: 15,
    end: 20
  })
  .on('data', t.fail)
  .on('end', t.pass)
})
