
const deepEquals = require('deep-equal')
const PassThrough = require('readable-stream').PassThrough
const pump = require('pump')
const subdown = require('subleveldown')
const collect = require('stream-collector')
const changeProcessor = require('level-change-processor')
const LiveStream = require('level-live-stream')
const wrap = require('level-option-wrap')
const through = require('through2')
const extend = require('xtend/mutable')
const END = new Buffer([0xff])
const SEPARATOR = '!'
const PREFIX = {
  view: 'v',
  index: 'x'
}

module.exports = exports = function createIndexedDB (opts) {
  const feed = opts.feed
  const worker = opts.worker
  const top = opts.db
  const idProp = opts.id
  const reduce = opts.reduce

  const counter = subdown(top, '~')
  const db = subdown(top, 'd', { valueEncoding: top.options.valueEncoding })
  LiveStream.install(db)

  const view = subdown(db, PREFIX.view, {
    valueEncoding: db.options.valueEncoding,
    separator: SEPARATOR
  })

  const index = subdown(db, PREFIX.index, { separator: SEPARATOR })

  const processor = db.processor = changeProcessor({
    feed: feed,
    db: counter,
    worker: processChange
  })

  // fix range
  const rangers = {
    view: getRanger(PREFIX.view),
    index: getRanger(PREFIX.index)
  }

  const indexAPI = {
    by: createIndexStream,
    raw: index
  }

  const viewAPI = {
    createReadStream: function (opts) {
      extend(opts, wrap(opts, rangers.view))
      return pump(
        upToDateStream(db, processor, opts),
        unprefixer(opts, PREFIX.view)
      )
    },
    get: getRow,
    raw: view
  }

  function processChange (change, cb) {
    const key = change[idProp]
    // ignore changes that we can't process
    if (!key) return cb()

    worker(change, function (err, update) {
      if (err) return cb(err)

      const rowKey = change[idProp]
      const viewRow = {
        type: 'put',
        key: update.key || rowKey,
        value: update.value || omit(change, idProp)
      }

      let batch = []
      const rowIndex = update.index
      if (rowIndex) {
        batch = batch.concat(Object.keys(rowIndex).map(function (key) {
          return {
            type: 'put',
            key: [key, rowIndex[key], rowKey]
          }
        }))
      }

      const rowIndexKey = getRowIndexKey(rowKey)
      index.get(rowIndexKey, function (err, prev) {
        if (prev) {
          batch = batch.concat(Object.keys(prev).map(key => {
            return {
              type: 'del',
              key: [key, prev[key], rowKey]
            }
          }))
        }

        if (batch.length) {
          // save latest index
          batch.push({
            type: 'put',
            key: rowIndexKey
          })
        }
      })

      db.batch(batch, function (err) {
        if (update.cb) update.cb(err)

        cb(err)
      })
    })
  }

  function getRowIndexKey (rowKey) {
    return SEPARATOR + rowKey
  }

  function createIndexStream (prop, value, opts) {
    const prefix = prefixKey(getIndexKey(prop, value), PREFIX.index)
    const ranger = getRanger(prefix)
    extend(opts, wrap(opts, ranger))
    opts.keys = opts.values = true
    const source = upToDateStream(db, processor, opts)
    return collect(pump(
      source,
      through.obj(function (data, enc, cb) {
        if (data.type === 'del') cb()
        else getRow(data.value, cb)
      })
    ))
  }

  function getRow (key, opts, cb) {
    processor.onLive(function () {
      db.get(prefixKey(key, PREFIX.view), opts, cb)
    })
  }

  return {
    view: viewAPI,
    index: indexAPI
  }
}

exports.notReadyStream = notReadyStream
exports.upToDateStream = upToDateStream
exports.mapToBatch = mapToBatch
exports.getIndexBatch = getIndexBatch
exports.getUpdateBatch = getUpdateBatch

function getUpdateBatch (oldVal, newVal, props) {
  oldVal = oldVal || {}
  newVal = newVal || {}
  const oldMap = pick(oldVal, props)
  const newMap = pick(newVal, props)
  for (var k in newMap) {
    if (k in oldMap) {
      // ignore what didn't change
      if (deepEquals(oldMap[k], newMap[k])) {
        delete oldMap[k]
        delete newMap[k]
      }
    }
  }

  return mapToBatch(oldMap, 'del').concat(mapToBatch(newMap, 'put'))
}

function getIndexBatch (obj, props, op) {
  op = op || 'put'
  return props.map(prop => {
    return {
      type: op,
      key: prop,
      value: obj[prop]
    }
  })
}

function mapToBatch (map, op) {
  op = op || 'put'
  var batch = []
  for (var k in map) {
    batch.push({
      type: op,
      key: k,
      value: map[k]
    })
  }

  return batch
}

function prefixKeys (batch, prefix) {
  return batch.map(row => {
    return {
      type: row.type,
      key: prefixKey(row.key, prefix),
      value: row.value
    }
  })
}

function prefixKey (key, prefix) {
  return subdownPrefix(prefix) + key
}

function subdownPrefix (prefix) {
  return SEPARATOR + prefix + SEPARATOR
}

function unprefixKey (key, prefix) {
  return key.slice(prefix.length + 2 * SEPARATOR.length)
}

function getIndexKey (key, val, docId) {
  var ikey = key
  if (val != null) {
    ikey += SEPARATOR + val
    if (docId != null) {
      ikey += SEPARATOR + docId
    }
  }

  return ikey
}

function upToDateStream (db, processor, opts) {
  opts = opts || {}
  return notReadyStream(function (cb) {
    processor.onLive(function () {
      const method = opts.live ? 'liveStream' : 'createReadStream'
      const stream = db[method].call(db, opts)
      cb(null, stream)
    })
  })
}

// function liveGet (db, processor /*, key, value, cb */) {
//   var rest = [].slice.call(arguments, 2)
//   processor.onLive(function () {
//     db.get.apply(db, rest)
//   })
// }

/**
 * @param  {Function} fn function that calls back with a stream
 */
function notReadyStream (fn) {
  var paused = new PassThrough({ objectMode: true })
  var source
  paused.destroy = function () {
    if (source) source.destroy()
    else this.end()
  }

  paused.pause()

  fn(function (err, stream) {
    if (err) return paused.destroy()

    source = stream
    pump(source, paused)
    paused.resume()
  })

  return paused
}

function concat (prefix, key, force) {
  if (typeof key === 'string' && (force || key.length)) return prefix + key
  if (Buffer.isBuffer(key) && (force || key.length)) return Buffer.concat([new Buffer(prefix), key])
  return key
}

function getRanger (prefix) {
  if (prefix[0] !== SEPARATOR) prefix = SEPARATOR + prefix
  if (prefix[prefix.length - 1] !== SEPARATOR) prefix += SEPARATOR

  return {
    gt: function (x) {
      return concat(prefix, x || '', true)
    },
    lt: function (x) {
      if (Buffer.isBuffer(x) && !x.length) x = END
      return concat(prefix, x || '\xff')
    }
  }
}

function unprefixer (opts, prefix) {
  return through.obj(function (data, enc, cb) {
    if (opts.keys !== false) {
      if (opts.values === false) {
        data = unprefixKey(data, PREFIX.index)
      } else {
        data.key = unprefixKey(data.key, PREFIX.index)
      }
    }

    cb(null, data)
  })
}

// function getStream (db) {
//   if (!db.isOpen()) {
//     stream = utils.notReadyStream(function (cb) {
//       db.once('open', function () {
//         updateOpts()
//         cb(null, source.createReadStream(opts))
//       })
//     })
//   } else {
//     updateOpts()
//     stream = source.createReadStream(opts)
//   }

//   return stream
// }

function pick (obj, props) {
  const subset = {}
  props.forEach(prop => {
    if (prop in obj) {
      subset[prop] = obj[prop]
    }
  })

  return subset
}

function omit (obj) {
  const subset = {}
  const props = [].slice.call(arguments, 1)
  for (var p in obj) {
    if (props.indexOf(p) === -1) {
      subset[p] = obj[p]
    }
  }

  return subset
}
