'use strict'

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
const clone = require('xtend')
const SEPARATOR = '!'
const MAX_CHAR = '\xff'
const PREFIX = {
  view: 'v',
  index: 'x'
}

module.exports = exports = function createIndexedDB (opts) {
  const feed = opts.feed
  // const worker = opts.worker
  const top = opts.db
  const idProp = opts.primaryKey || 'key'
  const reduce = opts.reduce || mergeReducer
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
    const rowKey = change[idProp]
    // ignore changes that we can't process
    if (rowKey == null) return cb()

    var togo = 2
    var rowIndexKey = getRowIndexKey(rowKey)
    var newState
    var prevIndex
    view.get(rowKey, function (err, state) {
      newState = reduce(state, change)
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
          return {
            type: 'put',
            key: key + sep + newIndex[key],
            // key: defaultIndexReducer(key, newState[key], rowKey),
            value: rowKey
          }
        })

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

  function getOldRow (key, opts, cb) {
    db.get(prefixKey(key, PREFIX.view), opts, cb)
  }

  function indexBy (prop, reduce) {
    function find (indexVal, cb) {
      collect(createIndexStream(prop, { eq: indexVal }), function (err, results) {
        if (err) return cb(err)
        if (!results.length) return NotFoundErr()
        cb(null, results)
      })
    }

    function findOne (indexVal, cb) {
      find(indexVal, function (err, results) {
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

  function getIndexKey (state, key) {
    return key + sep + indexReducers[key](newState)
  }

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

exports.SEPARATOR = SEPARATOR
exports.upToDateStream = upToDateStream

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

function getter (prop) {
  return function (obj) {
    obj[prop]
  }
}

function NotFoundErr () {
  const err = new Error('NotFoundErr')
  err.notFound = true
  err.name = err.type = 'notFound'
  return err
}
