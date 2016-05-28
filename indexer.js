
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
  const worker = opts.worker
  const top = opts.db
  const idProp = opts.primaryKey
  const reduce = opts.reduce || mergeReducer
  const indices = {}

  const counter = subdown(top, '~')
  const db = subdown(top, 'd', { valueEncoding: top.options.valueEncoding, separator: SEPARATOR })
  LiveStream.install(db)

  const view = subdown(db, PREFIX.view, { valueEncoding: top.options.valueEncoding, separator: SEPARATOR })
  const index = subdown(db, PREFIX.index, { separator: SEPARATOR, valueEncoding: top.options.valueEncoding })

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
          key: defaultGetIndexKey(key, prevIndex[key], rowKey)
        }
      })

      const newIndex = pick(newState, Object.keys(indices))
      const put = Object.keys(newIndex)
        .map(key => {
          return {
            type: 'put',
            // key: indices[key](newState),
            key: defaultGetIndexKey(key, newState[key], rowKey),
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

      // console.log(batch)
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

    const prefix = prefixKey(defaultGetIndexKey(prop), PREFIX.index)
    extend(opts, wrap(opts, {
      gt: function (x) {
        // e.g. ['!x!firstName', 'Bill', MAX_CHAR]
        return [prefix, x || '', opts.gte ? '' : MAX_CHAR].join(SEPARATOR)
      },
      lt: function (x) {
        // e.g. ['!x!firstName', 'Ted', MAX_CHAR]
        return [prefix, x || MAX_CHAR, opts.lte ? MAX_CHAR : ''].join(SEPARATOR)
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
    indices[prop] = reduce || defaultGetIndexKey
    return {
      createReadStream: createIndexStream.bind(null, prop)
    }
  }

  function defaultGetIndexKey (key, val, docId) {
    var ikey = key
    if (val != null) {
      ikey += SEPARATOR + val
      if (docId != null) {
        ikey += SEPARATOR + docId
      }
    }

    return ikey
  }

  return {
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
