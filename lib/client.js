'use strict'

const concat = require('simple-concat')
const Backoff = require('backo')
const parseMs = require('ms')
const deepSet = require('deep-set')
const http = require('http')
const https = require('https')
const zlib = require('zlib')
const escapeString = require('sql-escape-string')
const once = require('once')
const EventEmitter = require('events').EventEmitter
const QueryStream = require('./query-stream')
const concatArray = require('./concat-array')
const PrestoError = require('./presto-error')
const debug = require('debug')('lento')
const VERSION = require('../package.json').version

module.exports = class Client extends EventEmitter {
  constructor (opts) {
    super()

    if (!opts) opts = {}

    this.hostname = opts.hostname || 'localhost'
    this.port = opts.port ? parseInt(opts.port, 10) : 8080
    this.protocol = opts.protocol || 'http:'

    this.catalog = opts.catalog || null
    this.schema = opts.schema || null
    this.user = opts.user || null
    this.timezone = opts.timezone || null

    this.pollInterval = msOption('pollInterval', opts.pollInterval || 1e3)
    this.source = 'lento'
    this.userAgent = `${this.source} ${VERSION}`
    this.maxRetries = opts.maxRetries == null ? 10 : opts.maxRetries
    this._session = new Map()
  }

  // Note: mutates opts.
  // TODO: make this an internal method.
  request (opts, callback) {
    if (!opts.hostname) opts.hostname = this.hostname
    if (!opts.port) opts.port = this.port
    if (!opts.expectStatusCode) opts.expectStatusCode = 200
    if (!opts.protocol) opts.protocol = this.protocol

    const headers = opts.headers || (opts.headers = {})
    const catalog = opts.catalog || this.catalog
    const schema = opts.schema || this.schema
    const timezone = opts.timezone || this.timezone
    const user = opts.user || this.user

    // TODO: these may not be necessary for GET /v1/statement/{queryId}/{token}
    if (catalog) headers['X-Presto-Catalog'] = catalog
    if (schema) headers['X-Presto-Schema'] = schema
    if (timezone) headers['X-Presto-Time-Zone'] = timezone

    if (user) headers['X-Presto-User'] = user
    if (opts.json) headers['Accept'] = 'application/json'

    // TODO: move outside this function, only add on initial statement request.
    if (opts.method === 'POST' && this._session.size > 0) {
      // "Statements submitted following SET SESSION statements should include
      // any key-value pairs (returned by the servers X-Presto-Set-Session) in
      // the header X-Presto-Session. Multiple pairs can be comma-separated and
      // included in a single header".
      const pairs = Array.from(this._session.values())
      headers['X-Presto-Session'] = pairs.join(',')
    } else if ('X-Presto-Session' in headers) {
      delete headers['X-Presto-Session']
    }

    headers['X-Presto-Source'] = this.source
    headers['User-Agent'] = this.userAgent
    headers['Connection'] = 'keep-alive'
    headers['Accept-Encoding'] = 'gzip, deflate, identity'

    this._makeRequest(opts, new Backoff(), callback)
  }

  _makeRequest (opts, backoff, callback_) {
    const callback = once(callback_)

    const retry = (err, min, max) => {
      if (callback.called) {
        return
      }

      if (backoff.attempts >= this.maxRetries) {
        callback(err)
      } else {
        backoff.ms = min
        backoff.max = max

        // Use original callback to avoid copying #called
        const fn = this._makeRequest.bind(this, opts, backoff, callback_)
        const duration = backoff.duration()

        debug('%s %s %s. Retry in %dms', opts.method, opts.path, err, duration)
        this.emit('retry', duration)
        setTimeout(fn, duration)
      }
    }

    // Private event for test purposes
    this.emit('_request', opts)
    debug('%s %s%s', opts.method, opts.path, backoff.attempts > 0 ? ` (${backoff.attempts + 1})` : '')

    const req = (opts.protocol === 'http:' ? http : https).request(opts, (res) => {
      const headers = res.headers
      const contentType = headers['content-type']
      const contentEncoding = headers['content-encoding']
      const statusCode = res.statusCode

      // "If HTTP 503, sleep 50-100ms and try again"
      if (statusCode === 503) {
        // TODO: use `http-errors` to attach status code to error
        return retry(new Error('http 503'), 50, 100)
      }

      if (contentEncoding === 'gzip') {
        res.on('error', callback)
        res = res.pipe(zlib.createGunzip())
      } else if (contentEncoding === 'deflate') {
        res.on('error', callback)
        res = res.pipe(zlib.createInflate())
      }

      concat(res, (err, buf) => {
        if (err) return callback(err)

        // "If anything other than HTTP 200 with Content-Type application/json, then consider the query failed"
        if (statusCode !== opts.expectStatusCode) {
          // TODO: use `http-errors` to attach status code to error
          const reason = contentType === 'text/plain' ? buf.toString().trim() : ''
          const prefix = `http ${statusCode}`
          const msg = reason ? `${prefix}: ${reason}` : prefix

          return callback(new Error(msg))
        } else if (!opts.json) {
          return callback(null, buf)
        } else if (contentType !== 'application/json') {
          return callback(new Error('unexpected content type: ' + contentType))
        }

        try {
          var result = JSON.parse(buf)
        } catch (err) {
          return callback(err)
        }

        if (result.error) {
          return callback(new PrestoError(result.error))
        }

        // TODO: move this logic to set() and reset()
        if (result.updateType === 'SET SESSION' && headers['x-presto-set-session']) {
          // "SET SESSION statements:
          // - Will return an updateType (top-level key of the returned JSON) of
          //   "SET SESSION", and a single "true" in the data list
          // - Will return the key-value pair in the header X-Presto-Set-Session
          //   in the format key=value
          // - The updateType and header may exist in the first response, or a
          //   subsequent response from a nextUri"
          const kv = headers['x-presto-set-session']
          const key = kv.split('=')[0]

          this._session.set(key, kv)
        } else if (result.updateType === 'RESET SESSION') {
          // "RESET SESSION statements:
          // - Will return an updateType (top-level key of the returned JSON) of
          //   "RESET SESSION", and a single "true" in the data list
          // - Will return the session key in the header X-Presto-Clear-Session
          // - The updateType and header may exist in the first response, or a
          //   subsequent response from a nextUri"
          this._session.delete(headers['x-presto-clear-session'])
        }

        callback(null, result)
      })
    })

    req.on('error', (err) => {
      if (err.code === 'ECONNREFUSED') {
        retry(err, 1e3, 10e3)
      } else {
        callback(err)
      }
    })

    if (opts.method === 'POST' && opts.body) {
      req.write(opts.body)
    }

    req.end()
  }

  createPageStream (sql, opts) {
    if (typeof sql !== 'string' && !Buffer.isBuffer(sql)) {
      throw new TypeError('First argument "sql" must be a string or Buffer')
    } else if (sql.length === 0) {
      throw new Error('First argument "sql" must not be empty')
    }

    return new QueryStream(this, sql, opts)
  }

  createRowStream (sql, opts) {
    const pageOpts = Object.assign({}, opts, {
      paging: false,
      highWaterMark: 0
    })

    // TODO: optimize
    return this.createPageStream(sql, pageOpts)
  }

  query (sql, opts, callback) {
    if (typeof opts === 'function') {
      callback = opts
      opts = undefined
    }

    // TODO: maybe decouple the fetching of pages from the stream,
    // so that we don't need a stream for the callback variant.
    concatArray(this.createRowStream(sql, opts), callback)
  }

  session (opts, callback) {
    if (typeof opts === 'function') {
      callback = opts
      opts = undefined
    }

    this.query('show session', opts, (err, rows) => {
      if (err) return callback(err)

      const session = {}

      for (const row of rows) {
        const key = row.Name
        const type = row.Type
        const description = row.Description

        let value = row.Value
        let def = row.Default

        if (type === 'boolean') {
          value = value === 'true' ? true : value === 'false' ? false : null
          def = def === 'true' ? true : def === 'false' ? false : null
        } else if (type === 'integer') {
          value = parseInt(value, 10)
          def = parseInt(def, 10)
        }

        deepSet(session, key, { key, value, default: def, type, description })
      }

      callback(null, session)
    })
  }

  // TODO: support bigint values
  set (key, value, opts, callback) {
    if (typeof opts === 'function') {
      callback = opts
      opts = undefined
    }

    assertValidSessionKey(key)

    if (typeof value === 'string') {
      // Wraps in single quotes and escapes single quotes via ''
      value = escapeString(value)
    } else if (typeof value === 'number') {
      if (!Number.isFinite(value)) {
        throw new TypeError('number value must be finite')
      }

      value = String(value)
    } else if (typeof value === 'boolean') {
      value = String(value)
    } else {
      throw new TypeError('value must be a string, number or boolean')
    }

    this.query(`SET SESSION ${key}=${value}`, opts, (err, rows) => {
      if (err) {
        callback(err)
      } else if (rows.length !== 1 || rows[0].result !== true) {
        callback(new Error('failed to set session property'))
      } else if (!this._session.has(key)) {
        // Dev check. Response handling needs refactoring.
        callback(new Error('did not receive x-presto-set-session'))
      } else {
        callback()
      }
    })
  }

  reset (key, opts, callback) {
    if (typeof opts === 'function') {
      callback = opts
      opts = undefined
    }

    assertValidSessionKey(key)

    this.query(`RESET SESSION ${key}`, opts, (err, rows) => {
      if (err) {
        callback(err)
      } else if (rows.length !== 1 || rows[0].result !== true) {
        callback(new Error('failed to reset session property'))
      } else if (this._session.has(key)) {
        // Dev check. Response handling needs refactoring.
        callback(new Error('did not receive x-presto-clear-session'))
      } else {
        callback()
      }
    })
  }

  setTimeout (msOrDuration, opts, callback) {
    if (typeof opts === 'function') {
      callback = opts
      opts = undefined
    }

    if (typeof msOrDuration === 'number') {
      if (!Number.isFinite(msOrDuration) || msOrDuration <= 0) {
        throw new TypeError('duration must be finite and positive')
      }

      msOrDuration = msOrDuration + 'ms'
    } else if (typeof msOrDuration !== 'string' || msOrDuration === '') {
      throw new TypeError('duration must be a number or non-empty string')
    }

    this.set('query_max_run_time', msOrDuration, opts, callback)
  }

  resetTimeout (opts, callback) {
    this.reset('query_max_run_time', opts, callback)
  }
}

function assertValidSessionKey (key) {
  if (typeof key !== 'string' || key === '') {
    throw new TypeError('session key must be a string and not empty')
  }

  if (!/^[a-z]+[a-z_.]*[a-z]+$/.test(key)) {
    throw new TypeError('session key contains, starts or ends with illegal character(s)')
  }
}

function msOption (name, value) {
  const n = typeof value === 'number' ? value : parseMs(value)

  if (!Number.isInteger(n)) throw new Error(name + ' must be an integer, optionally with a unit (e.g. "50ms")')
  if (n <= 0) throw new Error(name + ' must be > 0 milliseconds')

  return n
}
