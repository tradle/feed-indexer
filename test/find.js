var test = require('tape')
var changes = require('changes-feed')
var nextDB = require('./helpers').nextDB
var indexer = require('..')

test('find', function(t) {
  t.plan(4)

  var db = nextDB()
  var feed = changes(nextDB())
  var entryProp = '_'
  var indexed = indexer({
    primaryKey: 'id',
    db: db,
    feed: feed,
    entryProp: entryProp
  })

  var byTitle = indexed.by('title')
  // , function (val) {
  //   return val.title + indexed.separator + val.id
  // })

  var posts = [
    {
      id: '1337',
      title: 'a title',
      body: 'lorem ipsum'
    },
    {
      id: '23',
      title: 'a title',
      body: 'booga'
    },
    {
      id: '2133',
      title: 'hey',
      body: 'ho'
    }
  ]

  posts.forEach(post => feed.append(post))

  byTitle.findOne('a title', function(err, result) {
    t.error(err)
    delete result[entryProp]
    t.deepEqual(result, posts[0])
  })

  byTitle.find('a title', function(err, results) {
    t.error(err)
    results.forEach(r => delete r[entryProp])
    t.deepEqual(results, posts.slice(0, 2))
  })
})
