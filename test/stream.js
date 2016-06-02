var test = require('tape')
var changes = require('changes-feed')
var nextDB = require('./helpers').nextDB
var indexer = require('..')

test('stream', function(t) {
  t.plan(2)

  var db = nextDB()
  var feed = changes(nextDB())
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
    end: 20,
    keys: false
  })
  .on('data', function(data) {
    t.deepEqual(data, post)
  })

  byLength.createReadStream({
    start: 15,
    end: 20,
    keys: false
  })
  .on('data', t.fail)
  .on('end', t.pass)
})

test.only('feed order', function(t) {
  t.plan(2)

  var db = nextDB()
  var feed = changes(nextDB())
  var indexed = indexer({
    primaryKey: 'id',
    db: db,
    feed: feed
  })

  var byTitle = indexed.by('title')
  var posts = [
    {
      id: '1337',
      title: 'a',
      body: 'lorem ipsum'
    },
    {
      id: '0',
      title: 'a',
      body: 'lorem shmipsum'
    }
  ]

  posts.forEach(post => feed.append(post))

  byTitle.createValueStream()
  .on('data', function(data) {
    t.deepEqual(data, posts.shift())
  })
})
