var test = require('tape')
var collect = require('stream-collector')
var changes = require('changes-feed')
var nextDB = require('./helpers').nextDB
var indexer = require('..')

test('view', function(t) {
  t.plan(1)

  var db = nextDB()
  var feed = changes(nextDB())
  var indexed = indexer({
    primaryKey: 'id',
    db: db,
    feed: feed
  })

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
      id: '1337',
      title: 'hey',
      body: 'ho'
    }
  ]

  posts.forEach(post => feed.append(post))

  collect(indexed.createReadStream({ keys: false }), function (err, results) {
    if (err) throw err

    t.same(results, [posts[2], posts[1]])
  })
})
