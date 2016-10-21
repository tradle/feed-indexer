'use strict'

const EventEmitter = require('events').EventEmitter
const PassThrough = require('readable-stream').PassThrough
const pump = require('pump')
const subdown = require('subleveldown')
const collect = require('stream-collector')
const changeProcessor = require('level-change-processor')
const LiveStream = require('level-live-stream')
const wrap = require('level-option-wrap')
const errors = require('level-errors')
const through = require('through2')
const extend = require('xtend/mutable')
const clone = require('xtend')
const lexint = require('lexicographic-integer')
const SEPARATOR = '!'
const MAX_CHAR = '\xff'
const ENTRY_PROP = '_'
const PREFIX = {
  view: 'v',
  index: 'x'
}

module.exports = exports = createIndexedDB
// exports.SEPARATOR = SEPARATOR
// exports.merge = merge

function createIndexedDB (opts) {
  const feed = opts.feed
  if (!feed.count) {
    throw new Error('feed.count method not found, use mafintosh/changes-feed or mvayngrib/changes-feed')
  }

  const top = opts.db
  const primaryKey = opts.primaryKey || 'key'
  const entryProp = opts.entryProp || ENTRY_PROP
  const filter = opts.filter || alwaysTrue
  const preprocess = opts.preprocess
  // const custom = opts.custom
  const stateReducer = opts.reduce || mergeReducer
  const sep = opts.separator || SEPARATOR
  const indexReducers = {}
  const indexEmitters = {}

  const counter = subdown(top, '~')
  const dbOpts = { valueEncoding: top.options.valueEncoding, separator: sep }
  const db = subdown(top, 'd', dbOpts)
  LiveStream.install(db)

  const view = subdown(db, PREFIX.view, dbOpts)
  const index = subdown(db, PREFIX.index, dbOpts)

  const processor = db.processor = changeProcessor({
    feed: feed,
    db: counter,
    worker: worker
  })

  processor.setMaxListeners(0)

  const emitter = new EventEmitter()

  function worker (change, cb) {
    // change = change.value
    if (!preprocess) return workerHelper(change, cb)

    preprocess(change, function (err, processed) {
      if (err) return cb(err)

      workerHelper(processed || change, cb)
    })
  }

  function workerHelper (change, cb) {
    const changeVal = change.value
    if (!filter(changeVal)) return cb()

    const rowKey = getPrimaryKey(changeVal, primaryKey)
    // ignore changes that we can't process
    if (rowKey == null) return cb()

    let togo = 2
    const rowIndexKey = getRowIndexKey(rowKey)
    let newState
    let oldState
    let prevIndex
    view.get(rowKey, function (err, state) {
      oldState = state
      stateReducer(state, change, function (err, _newState) {
        if (err) return cb(err)

        newState = _newState
        if (newState && !(entryProp in newState)) {
          newState[entryProp] = change.change
        }

        next()
      })
    })

    index.get(rowIndexKey, function (err, indexMap) {
      prevIndex = indexMap || {}
      next()
    })

    function next () {
      if (--togo) return

      let del = Object.keys(prevIndex).map(key => {
        return {
          type: 'del',
          key: key + sep + prevIndex[key]
        }
      })

      let put
      let newIndex = {}
      // we may be in a delete-only op
      if (newState) {
        for (let key in indexReducers) {
          let iVal = indexReducers[key](newState, change)
          if (iVal != null) newIndex[key] = iVal
        }

        put = Object.keys(newIndex)
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
          .concat({
            type: 'put',
            key: rowIndexKey,
            value: newIndex
          })

        del = del.filter(delRow => {
          return put.every(putRow => {
            return putRow.key !== delRow.key
          })
        })
      }

      const indexBatch = del.concat(put || [])
      const batch = prefixKeys(indexBatch, PREFIX.index)

      // update view
      batch.push({
        type: newState ? 'put' : 'del',
        key: prefixKey(rowKey, PREFIX.view),
        value: newState || oldState
      })

      db.batch(batch, function (err) {
        if (err) return cb(err)

        cb()

        // announce changes
        emitter.emit('change', change, newState, oldState)
        for (let prop in newIndex) {
          indexEmitters[prop].emit('change', change, newIndex[prop])
        }
      })
    }
  }

  function getRowIndexKey (rowKey) {
    return MAX_CHAR + rowKey
  }

  function createIndexStream (prop, opts) {
    opts = normalizeOpts(opts)

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

    const noKeys = opts.keys === false
    opts.keys = opts.values = true
    return pump(
      upToDateStream(db, processor, opts),
      through.obj(function (data, enc, cb) {
        if (data.type === 'del') return cb()

        getOldRow(data.value, function (err, val) {
          if (err) return cb(err)

          // removeLogPointer(val, { keys: false })
          if (!noKeys) val = { key: data.key, value: val }

          cb(null, val)
        })
      })
    )
  }

  function getLiveRow (key, opts, cb) {
    processor.onLive(() => getOldRow(key, opts, cb))
  }

  function getOldRow (key, cb) {
    view.get(key, cb)
  }

  function createIndex (prop, reduce) {
    function createReadStream (opts) {
      return createIndexStream(prop, opts)
    }

    function find (opts, cb) {
      collect(createReadStream(opts), function (err, results) {
        if (err) return cb(err)
        if (!results.length) return cb(new errors.NotFoundError())

        cb(null, results)
      })
    }

    function findOne (opts, cb) {
      opts = normalizeOpts(opts)
      opts.limit = 1
      find(opts, function (err, results) {
        if (err) return cb(err)
        cb(null, results[0])
      })
    }

    indexReducers[prop] = reduce || defaultIndexReducer(prop)
    indexEmitters[prop] = new EventEmitter()
    return extend(indexEmitters[prop], {
      find: find,
      findOne: findOne,
      createReadStream: createReadStream,
      createKeyStream: newKeyStreamCreator(createReadStream),
      createValueStream: newValueStreamCreator(createReadStream),
    })
  }

  function defaultIndexReducer (prop) {
    return function indexReducer (state, change) {
      if (prop in state) {
        return state[prop] + sep +
          // for identical values,
          // order by change index in feed
          lexint.pack(change.change, 'hex') + sep +
          getPrimaryKey(state, primaryKey)
      }
    }
  }

  // function logOrderIndexReducer (prop) {
  // }

  function getPrimaryKey (obj, propOrFn) {
    return typeof propOrFn === 'function' ? propOrFn(obj) : obj[propOrFn]
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

  // function logPointerRemover (opts) {
  //   return through.obj(function (data, enc, cb) {
  //     removeLogPointer(data, opts)
  //     cb(null, data)
  //   })
  // }

  // function removeLogPointer (data, opts) {
  //   const val = opts.keys === false ? data :
  //     opts.values !== false ? data.value : null

  //   if (val) delete val[entryProp]
  // }

  function mergeReducer (state, change, cb) {
    // delete state[primaryKey]
    cb(null, merge(state, change))
  }

  function merge (state, change) {
    const newState = clone(state || {}, change.value || {})
    if (!state) {
      if (entryProp in newState) throw new Error(`"${entryProp}" is a reserved property`)
      newState[entryProp] = change.change
    }

    return newState
  }

  function createViewReadStream (opts) {
    opts = opts || {}
    extend(opts, wrap(opts, {
      gt: function (x) {
        return prefixKey(x || '', PREFIX.view)
      },
      lt: function (x) {
        return prefixKey(x || '', PREFIX.view) + '\xff'
      }
    }))

    return pump(
      upToDateStream(db, processor, opts),
      unprefixer(PREFIX.view, opts)
      // logPointerRemover(opts)
    )
  }

  return extend(emitter, {
    onLive: cb => processor.onLive(cb),
    separator: sep,
    merge: merge,
    by: createIndex,
    get: function (key, opts, cb) {
      processor.onLive(() => view.get(key, opts, cb))
    },
    createReadStream: createViewReadStream,
    createKeyStream: newKeyStreamCreator(createViewReadStream),
    createValueStream: newValueStreamCreator(createViewReadStream),
  })
}

function upToDateStream (db, processor, opts) {
  opts = opts || {}
  const tr = new PassThrough({ objectMode: true })
  processor.onLive(function () {
    const method = opts.live ? 'liveStream' : 'createReadStream'
    const source = db[method].call(db, opts)
    source.pipe(tr)
  })

  return tr
}

function alwaysTrue () {
  return true
}

function unprefixer (prefix, opts) {
  return through.obj(function (data, enc, cb) {
    if (opts.keys === false) return cb(null, data)
    if (opts.values === false) return cb(null, data.slice(prefix.length))

    cb(null, {
      type: data.type,
      key: data.key.slice(prefix.length),
      value: data.value
    })
  })
}

function newKeyStreamCreator (createReadStream) {
  return function createKeyStream (opts) {
    opts = opts || {}
    opts.values = false
    return createReadStream(opts)
  }
}

function newValueStreamCreator (createReadStream) {
  return function createValueStream (opts) {
    opts = opts || {}
    opts.keys = false
    return createReadStream(opts)
  }
}

function normalizeOpts (opts) {
  if (typeof opts === 'string') {
    return { eq: opts, keys: false }
  }

  return opts ? clone(opts) : {}
}
