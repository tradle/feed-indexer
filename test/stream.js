var test = require('tape')
var changes = require('changes-feed')
var nextDB = require('./helpers').nextDB
var indexer = require('..')
var entryProp = '_'

test('stream', function(t) {
  t.plan(2)

  var db = nextDB()
  var feed = changes(nextDB())
  var indexed = indexer({
    primaryKey: 'id',
    db: db,
    feed: feed,
    entryProp: '_'
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
    delete data[entryProp]
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

test('feed order', function(t) {
  t.plan(2)

  var db = nextDB()
  var feed = changes(nextDB())
  var indexed = indexer({
    primaryKey: 'id',
    db: db,
    feed: feed,
    entryProp: entryProp
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
    delete data[entryProp]
    t.deepEqual(data, posts.shift())
  })
})

test('preprocess', function (t) {
  var db = nextDB()
  var feed = changes(nextDB())
  var indexed = indexer({
    primaryKey: 'preprocessed',
    db: db,
    feed: feed,
    entryProp: entryProp,
    preprocess: function (change, cb) {
      process.nextTick(function () {
        change.value.preprocessed = change.change
        cb(null, change)
      })
    },
    reduce: function (state, change) {
      t.equal(change.value.preprocessed, change.change)
      t.end()
      return change.value
    }
  })

  feed.append({
    id: 'dumb'
  })
})
