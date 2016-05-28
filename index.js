'use strict'

const PassThrough = require('readable-stream').PassThrough
const pump = require('pump')
const subdown = require('subleveldown')
const collect = require('stream-collector')
const changeProcessor = require('level-change-processor')
const LiveStream = require('level-live-stream')
const wrap = require('level-option-wrap')
const through = require('through2')
const extend = require('xtend/mutable')
const clone = require('xtend')
const SEPARATOR = '!'
const MAX_CHAR = '\xff'
const PREFIX = {
  view: 'v',
  index: 'x'
}

module.exports = exports = createIndexedDB
exports.SEPARATOR = SEPARATOR
exports.upToDateStream = upToDateStream

function createIndexedDB (opts) {
  const feed = opts.feed
  if (!feed.count) {
    throw new Error('use mafintosh/changes-feed or mvayngrib/changes-feed with "count" method')
  }

  // const worker = opts.worker
  const top = opts.db
  const idProp = opts.primaryKey || 'key'
  const filter = opts.filter || alwaysTrue
  const stateReducer = opts.reduce || mergeReducer
  const sep = opts.separator || SEPARATOR
  const indexReducers = {}

  const counter = subdown(top, '~')
  const db = subdown(top, 'd', { valueEncoding: top.options.valueEncoding, separator: sep })
  LiveStream.install(db)

  const view = subdown(db, PREFIX.view, { valueEncoding: top.options.valueEncoding, separator: sep })
  const index = subdown(db, PREFIX.index, { separator: sep, valueEncoding: top.options.valueEncoding })

  const processor = db.processor = changeProcessor({
    feed: feed,
    db: counter,
    worker: processChange
  })

  function mergeReducer (state, change) {
    state = clone(state || {}, change || {})
    // delete state[idProp]
    return state
  }

  function processChange (change, cb) {
    change = change.value
    if (!filter(change)) return cb()

    const rowKey = change[idProp]
    // ignore changes that we can't process
    if (rowKey == null) return cb()

    var togo = 2
    var rowIndexKey = getRowIndexKey(rowKey)
    var newState
    var prevIndex
    view.get(rowKey, function (err, state) {
      newState = stateReducer(state, change)
      next()
    })

    index.get(rowIndexKey, function (err, indexMap) {
      prevIndex = indexMap || {}
      next()
    })

    function next () {
      if (--togo) return

      const del = Object.keys(prevIndex).map(key => {
        return {
          type: 'del',
          key: key + sep + prevIndex[key]
        }
      })

      const newIndex = {}
      for (let key in indexReducers) {
        newIndex[key] = indexReducers[key](newState)
      }

      const put = Object.keys(newIndex)
        .map(key => {
          // support multiple generated indices for one prop
          // e.g. an identity might have multiple keys and want
          // multiple indices for the `fingerprint` property
          let vals = newIndex[key]
          vals = Array.isArray(vals) ? vals : [vals]
          return vals.map(function (val) {
            return {
              type: 'put',
              key: key + sep + val,
              // key: defaultIndexReducer(key, newState[key], rowKey),
              value: rowKey
            }
          })
        })
        // flatten
        .reduce(function (flat, next) {
          return flat.concat(next)
        }, [])

      const indexBatch = del.concat(put)
      indexBatch.push({
        type: 'put',
        key: rowIndexKey,
        value: newIndex
      })

      const batch = prefixKeys(indexBatch, PREFIX.index)

      // update view
      batch.push({
        type: 'put',
        key: prefixKey(rowKey, PREFIX.view),
        value: newState
      })

      db.batch(batch, cb)
    }
  }

  function getRowIndexKey (rowKey) {
    return MAX_CHAR + rowKey
  }

  function createIndexStream (prop, opts) {
    opts = opts || {}
    if ('eq' in opts) {
      opts.lte = opts.gte = opts.eq
    }

    const prefix = prefixKey(prop, PREFIX.index)
    extend(opts, wrap(opts, {
      gt: function (x) {
        // e.g. ['!x!firstName', 'Bill', MAX_CHAR]
        return [prefix, x || '', opts.gte ? '' : MAX_CHAR].join(sep)
      },
      lt: function (x) {
        // e.g. ['!x!firstName', 'Ted', MAX_CHAR]
        return [prefix, x || MAX_CHAR, opts.lte ? MAX_CHAR : ''].join(sep)
      }
    }))

    opts.keys = opts.values = true
    return pump(
      upToDateStream(db, processor, opts),
      through.obj(function (data, enc, cb) {
        if (data.type === 'del') cb()
        else getOldRow(data.value, cb)
      })
    )
  }

  function getLiveRow (key, opts, cb) {
    processor.onLive(() => getOldRow(key, opts, cb))
  }

  function getOldRow (key, cb) {
    view.get(key, cb)
  }

  function indexBy (prop, reduce) {
    function find (opts, cb) {
      if (typeof opts !== 'object') {
        opts = { eq: opts }
      }

      collect(createIndexStream(prop, opts), function (err, results) {
        if (err) return cb(err)
        if (!results.length) return NotFoundErr()
        cb(null, results)
      })
    }

    function findOne (opts, cb) {
      find(opts, function (err, results) {
        if (err) return cb(err)
        cb(null, results[0])
      })
    }

    indexReducers[prop] = reduce || defaultIndexReducer(prop)
    return {
      find: find,
      findOne: findOne,
      createReadStream: createIndexStream.bind(null, prop)
    }
  }

  // function getIndexKey (state, key) {
  //   return key + sep + indexReducers[key](newState)
  // }

  function defaultIndexReducer (prop) {
    return function indexReducer (val) {
      return val[prop] + sep + val[idProp]
    }
  }

  function prefixKeys (batch, prefix) {
    return batch.map(row => {
      const prefixed = {
        type: row.type,
        key: prefixKey(row.key, prefix),
      }

      if ('value' in row) prefixed.value = row.value

      return prefixed
    })
  }

  function prefixKey (key, prefix) {
    return SEPARATOR + prefix + SEPARATOR + key
  }

  return {
    separator: sep,
    by: indexBy,
    get: function (key, opts, cb) {
      processor.onLive(() => view.get(key, opts, cb))
    },
    createReadStream: function (opts) {
      return upToDateStream(view, processor, opts)
    }
  }
}

function upToDateStream (db, processor, opts) {
  opts = opts || {}
  var tr = new PassThrough({ objectMode: true })
  processor.onLive(function () {
    const method = opts.live ? 'liveStream' : 'createReadStream'
    const source = db[method].call(db, opts)
    source.pipe(tr)
  })

  return tr
}

function NotFoundErr () {
  const err = new Error('NotFoundErr')
  err.notFound = true
  err.name = err.type = 'notFound'
  return err
}

function alwaysTrue () {
  return true
}
