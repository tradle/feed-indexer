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

test('find', function(t) {
  t.plan(4)

  var db = level('db')
  var feed = changes(level('feed'))
  var indexed = indexer({
    primaryKey: 'id',
    db: db,
    feed: feed
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
    t.deepEqual(result, posts[0])
  })

  byTitle.find('a title', function(err, results) {
    t.error(err)
    t.deepEqual(results, posts.slice(0, 2))
  })
})
