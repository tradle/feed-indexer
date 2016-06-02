
var memdown = require('memdown')
var levelup = require('levelup')
var counter = 0

exports.nextDB = function () {
  return levelup('' + counter++, {
    db: memdown,
    valueEncoding: 'json'
  })
}
