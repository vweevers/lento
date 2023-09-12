'use strict'

const concat = require('simple-concat')
const Backoff = require('backo')
const parseMs = require('ms')
const deepSet = require('dset').dset
const herr = require('http-errors')
const http = require('http')
const https = require('https')
const zlib = require('zlib')
const escapeString = require('sql-escape-string')
const once = require('once')
const catering = require('catering')
const EventEmitter = require('events').EventEmitter
const QueryStream = require('./query-stream')
const concatArray = require('./concat-array')
const PrestoError = require('./presto-error')
const debug = require('debug')('lento')
const VERSION = require('../package.json').version
const URL = require('url').URL
const hasOwnProperty = Object.prototype.hasOwnProperty

module.exports = class Client extends EventEmitter {
  constructor (opts) {
    super()

    if (!opts) opts = {}

    this.hostname = opts.hostname || 'localhost'
    this.port = opts.port ? parseInt(opts.port, 10) : 8080
    this.protocol = opts.protocol || 'http:'
    this.headers = opts.headers || null

    if (!Number.isInteger(this.port) || this.port <= 0) {
      throw new TypeError('The "port" option must be a positive integer')
    }

    this.catalog = opts.catalog || null
    this.schema = opts.schema || null
    this.user = opts.user || null
    this.timezone = opts.timezone || null
    this.parametricDatetime = opts.parametricDatetime || false

    this.pollInterval = msOption('pollInterval', opts.pollInterval || 1e3)
    this.socketTimeout = msOption('socketTimeout', opts.socketTimeout || 120e3)
    this.source = 'lento'
    this.userAgent = `${this.source} ${VERSION}`
    this.maxRetries = opts.maxRetries == null ? 10 : opts.maxRetries
    this._session = new Map()
  }

  // Note: mutates opts.
  // TODO: make this an internal method.
  request (opts, callback) { /* eslint-disable dot-notation */
    if (!opts.hostname) opts.hostname = this.hostname
    if (!('port' in opts)) opts.port = this.port
    if (!opts.expectStatusCode) opts.expectStatusCode = 200
    if (!opts.protocol) opts.protocol = this.protocol

    const headers = {}
    const catalog = opts.catalog || this.catalog
    const schema = opts.schema || this.schema
    const timezone = opts.timezone || this.timezone
    const user = opts.user || this.user
    const parametricDatetime = opts.parametricDatetime || this.parametricDatetime

    // TODO: these may not be necessary for GET /v1/statement/{queryId}/{token}
    if (catalog) headers['x-presto-catalog'] = catalog
    if (schema) headers['x-presto-schema'] = schema
    if (timezone) headers['x-presto-time-zone'] = timezone
    if (parametricDatetime) headers['x-presto-client-capabilities'] = 'PARAMETRIC_DATETIME'

    if (user) headers['x-presto-user'] = user
    if (opts.json) headers['accept'] = 'application/json'

    // TODO: move outside this function, only add on initial statement request.
    if (opts.method === 'POST' && this._session.size > 0) {
      // "Statements submitted following SET SESSION statements should include
      // any key-value pairs (returned by the servers X-Presto-Set-Session) in
      // the header X-Presto-Session. Multiple pairs can be comma-separated and
      // included in a single header".
      const pairs = Array.from(this._session.values())
      headers['x-presto-session'] = pairs.join(',')
    }

    headers['x-presto-source'] = this.source
    headers['user-agent'] = this.userAgent
    headers['connection'] = 'keep-alive'
    headers['accept-encoding'] = 'gzip, deflate, identity'

    for (const src of [this.headers, opts.headers]) {
      if (src == null) continue

      for (const k in src) {
        if (!hasOwnProperty.call(src, k)) continue
        if (/^x-presto-session$/i.test(k) && opts.method !== 'POST') continue

        headers[k.toLowerCase()] = src[k]
      }
    }

    opts.headers = headers
    this._makeRequest(opts, new Backoff({ min: 1e3, max: 10e3 }), callback)
  }

  _makeRequest (opts, backoff, callback_) {
    const callback = once(callback_)

    const abort = (err) => {
      if (!req.aborted) req.abort()
      if (callback.called) return

      if (backoff.attempts < this.maxRetries && retryable(err)) {
        // Ignore further signals
        callback.called = true

        // Use original callback to avoid copying #called
        const fn = this._makeRequest.bind(this, opts, backoff, callback_)
        const duration = backoff.duration()

        debug('%s %s %s. Retry in %dms', opts.method, opts.path, err, duration)
        this.emit('retry', duration)
        setTimeout(fn, duration)

        return
      }

      callback(err)
    }

    // Private event for test purposes
    this.emit('_request', opts)
    debug('%s %s%s', opts.method, opts.path, backoff.attempts > 0 ? ` (${backoff.attempts + 1})` : '')

    const req = (opts.protocol === 'http:' ? http : https).request(opts, (res) => {
      const headers = res.headers
      const contentType = headers['content-type']
      const contentEncoding = headers['content-encoding']
      const statusCode = res.statusCode

      // Follow HTTP 307 redirects
      if (statusCode === 307) {
        const location = headers.location
        let redirectUrl

        if (!location) {
          return abort(new Error('HTTP 307 redirect is missing "location" header'))
        }

        try {
          redirectUrl = new URL(location)
        } catch (err) {
          return abort(new Error('HTTP 307 redirect has invalid "location" header: ' + location))
        }

        if (opts.protocol !== redirectUrl.protocol) {
          return abort(new Error('HTTP 307 redirect protocol switch is not allowed'))
        }

        // Ignore further signals
        callback.called = true
        req.abort()

        return this._makeRequest(
          Object.assign({}, opts, {
            hostname: redirectUrl.hostname,
            port: redirectUrl.port ? parseInt(redirectUrl.port, 10) : undefined,
            path: redirectUrl.pathname + redirectUrl.search
          }),
          backoff,
          callback_
        )
      }

      // "If HTTP 503, sleep 50-100ms [we sleep longer] and try again"
      if (statusCode === 503) {
        return abort(herr(statusCode, { expose: false }))
      }

      if (contentEncoding === 'gzip') {
        res.on('error', abort)
        res = res.pipe(zlib.createGunzip())
      } else if (contentEncoding === 'deflate') {
        res.on('error', abort)
        res = res.pipe(zlib.createInflate())
      }

      concat(res, (err, buf) => {
        if (err && err.code === 'Z_DATA_ERROR') return abort(zError(contentEncoding, err))
        if (err) return abort(err)
        if (callback.called) return

        // "If anything other than HTTP 200 with Content-Type application/json, then consider the query failed"
        if (statusCode !== opts.expectStatusCode) {
          if (statusCode > 399 && statusCode < 600) {
            const msg = contentType === 'text/plain' ? buf.toString().trim() : ''
            const err = herr(statusCode, msg || null, { expose: false })

            return callback(err)
          }

          return callback(new Error('Unexpected HTTP status code: ' + statusCode))
        } else if (!opts.json) {
          return callback(null, buf)
        } else if (contentType !== 'application/json') {
          return callback(new Error('Unexpected HTTP content type: ' + contentType))
        }

        let result
        try {
          result = JSON.parse(buf)
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

    req.setTimeout(this.socketTimeout, () => {
      const err = new Error('ETIMEDOUT')
      err.code = 'ETIMEDOUT'
      abort(err)
    })

    req.on('error', abort)

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

    callback = catering.fromCallback(callback)

    // TODO: maybe decouple the fetching of pages from the stream,
    // so that we don't need a stream for the callback variant.
    concatArray(this.createRowStream(sql, opts), callback)

    return callback.promise
  }

  session (opts, callback) {
    if (typeof opts === 'function') {
      callback = opts
      opts = undefined
    }

    callback = catering.fromCallback(callback)

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
          value = value === '' ? null : parseInt(value, 10)
          def = def === '' ? null : parseInt(def, 10)
        }

        deepSet(session, key, { key, value, default: def, type, description })
      }

      callback(null, session)
    })

    return callback.promise
  }

  // TODO: support bigint values
  set (key, value, opts, callback) {
    if (typeof opts === 'function') {
      callback = opts
      opts = undefined
    }

    assertValidSessionKey(key)
    callback = catering.fromCallback(callback)

    if (typeof value === 'string') {
      // Wraps in single quotes and escapes single quotes via ''
      value = escapeString(value)
    } else if (typeof value === 'number') {
      if (!Number.isFinite(value)) {
        throw new TypeError('Number value of session property must be finite')
      }

      value = String(value)
    } else if (typeof value === 'boolean') {
      value = String(value)
    } else {
      throw new TypeError('Value of session property must be a string, number or boolean')
    }

    this.query(`SET SESSION ${key}=${value}`, opts, (err, rows) => {
      if (err) {
        callback(err)
      } else if (rows.length !== 1 || rows[0].result !== true) {
        callback(new Error('Failed to set session property'))
      } else if (!this._session.has(key)) {
        // Dev check. Response handling needs refactoring.
        callback(new Error('Did not receive x-presto-set-session'))
      } else {
        callback()
      }
    })

    return callback.promise
  }

  reset (key, opts, callback) {
    if (typeof opts === 'function') {
      callback = opts
      opts = undefined
    }

    assertValidSessionKey(key)
    callback = catering.fromCallback(callback)

    this.query(`RESET SESSION ${key}`, opts, (err, rows) => {
      if (err) {
        callback(err)
      } else if (rows.length !== 1 || rows[0].result !== true) {
        callback(new Error('Failed to reset session property'))
      } else if (this._session.has(key)) {
        // Dev check. Response handling needs refactoring.
        callback(new Error('Did not receive x-presto-clear-session'))
      } else {
        callback()
      }
    })

    return callback.promise
  }

  setTimeout (msOrDuration, opts, callback) {
    if (typeof opts === 'function') {
      callback = opts
      opts = undefined
    }

    if (typeof msOrDuration === 'number') {
      if (!Number.isFinite(msOrDuration) || msOrDuration <= 0) {
        throw new TypeError('Timeout duration must be finite and positive')
      }

      msOrDuration = msOrDuration + 'ms'
    } else if (typeof msOrDuration !== 'string' || msOrDuration === '') {
      throw new TypeError('Timeout duration must be a number or non-empty string')
    }

    return this.set('query_max_run_time', msOrDuration, opts, callback)
  }

  resetTimeout (opts, callback) {
    return this.reset('query_max_run_time', opts, callback)
  }
}

function assertValidSessionKey (key) {
  if (typeof key !== 'string' || key === '') {
    throw new TypeError('Session key must be a string and not empty')
  }

  if (!/^[a-z]+[a-z_.]*[a-z]+$/.test(key)) {
    throw new TypeError('Session key contains, starts or ends with illegal character(s)')
  }
}

function msOption (name, value) {
  const n = typeof value === 'number' ? value : parseMs(value)

  if (!Number.isInteger(n)) {
    throw new Error('The "' + name + '" option must be an integer, optionally with a unit (e.g. "50ms")')
  }

  if (n <= 0) throw new Error('The "' + name + '" option must be > 0 milliseconds')

  return n
}

function retryable (err) {
  return err.code === 'ECONNREFUSED' || err.code === 'ECONNRESET' || err.statusCode === 503
}

function zError (enc, err) {
  err.message = 'Unable to decode ' + enc + ' content: ' + err.message
  return err
}
