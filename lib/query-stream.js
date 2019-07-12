'use strict'

const Readable = require('readable-stream').Readable
const AggregateError = require('aggregate-error')
const Backoff = require('backo')
const URL = require('url').URL
const deserialize = require('./deserialize')
const debug = require('debug')('lento')
const noop = function () {}

const RETRY_ERROR_CODES = new Set([
  'SERVER_STARTING_UP',
  'HIVE_METASTORE_ERROR',
  'TOO_MANY_REQUESTS_FAILED',
  'PAGE_TRANSPORT_TIMEOUT'
])

// Docs: https://github.com/prestodb/presto/blob/master/presto-docs/src/main/sphinx/rest/statement.rst
module.exports = class QueryStream extends Readable {
  // TODO: support per-query catalog and schema options.
  constructor (client, sql, opts) {
    if (!opts) opts = {}

    super({ objectMode: true, highWaterMark: opts.highWaterMark || 0 })

    this._client = client
    this._asObjects = opts.rowFormat !== 'array'
    this._paging = opts.paging !== false
    this._pageSize = opts.pageSize != null ? opts.pageSize : 500
    this._backoff = new Backoff({ min: 1e3, max: 300e3 })
    this._sql = sql

    this._reset(false)
    this._takeoff = this._takeoff.bind(this)
    this._descend = this._descend.bind(this)
  }

  _reset (reading) {
    this._timeout = undefined
    this._retryTimeout = undefined
    this._queryId = undefined
    this._columns = undefined
    this._prevPath = undefined
    this._buffer = undefined
    this._bufferIndex = 0

    this._inflight = false
    this._upstreamFinished = false
    this._received = false
    this._errored = false
    this._reading = reading
    this._lastUpstreamState = undefined

    this._requestOptions = {
      path: '/v1/statement',
      method: 'POST',
      body: this._sql,
      json: true,
      expectStatusCode: 200
    }
  }

  _read (size) {
    // https://github.com/nodejs/readable-stream/issues/353#issuecomment-414791903
    if (this.destroyed) return

    // See https://github.com/nodejs/node/issues/3203
    if (this._reading) return
    else this._reading = true

    if (this._paging) {
      // Push arrays of rows
      while (this._buffer !== undefined && this._buffer.length > 0) {
        let chunk

        if (this._pageSize <= 0 || this._buffer.length <= this._pageSize) {
          chunk = this._buffer // Avoid a copy
          this._buffer = undefined
        } else {
          chunk = this._buffer.splice(0, this._pageSize)
        }

        if (!this.push(chunk)) {
          this._reading = false
          return
        }
      }
    } else {
      // Push single rows
      while (this._buffer !== undefined && this._bufferIndex < this._buffer.length) {
        const chunk = this._buffer[this._bufferIndex++]

        if (!this.push(chunk)) {
          this._reading = false
          return
        }
      }
    }

    if (this._upstreamFinished) {
      this.push(null)
    } else {
      this._buffer = undefined
      this._bufferIndex = 0
      this._taxi()
    }
  }

  _taxi () {
    // If URL did not change, wait for server-side state change.
    if (this._prevPath === this._requestOptions.path) {
      this._timeout = setTimeout(this._takeoff, this._client.pollInterval)
    } else {
      this._takeoff()
    }
  }

  _takeoff () {
    this._inflight = true
    this.emit('request', this._requestOptions)
    this._client.request(this._requestOptions, this._descend)
  }

  _descend (err, result) {
    this._land(err, result)
  }

  _land (err, result) {
    this._inflight = false

    if (err) {
      // Retry query if we didn't receive data yet (to prevent emitting duplicates).
      if (!this._received && RETRY_ERROR_CODES.has(err.code) && this._backoff.attempts < this._client.maxRetries) {
        this._reset(true)

        const duration = this._backoff.duration()
        debug('Query %s %s. Retry in %dms', this._queryId || '[n/a]', err, duration)
        this._retryTimeout = setTimeout(this._takeoff, duration)
      } else {
        debug('Query %s %s', this._queryId || '[n/a]', err)

        // Skip cancelation
        this._errored = true
        this.destroy(err)
      }

      return
    }

    if (this._queryId === undefined && result.id) {
      this._queryId = result.id
      this.emit('id', this._queryId)

      if (result.infoUri) {
        this.emit('info', result.infoUri)
      }
    }

    // "The document will contain columns if available"
    if (this._columns === undefined && result.columns) {
      this._columns = result.columns
      this.emit('columns', result.columns)
    }

    if (result.stats) {
      this.emit('stats', result.stats)

      if (result.stats.state && result.stats.state !== this._lastUpstreamState) {
        this._lastUpstreamState = result.stats.state
        this.emit('state_change', this._lastUpstreamState)
      }
    }

    // "The document will contain data if available"
    if (result.data && result.data.length > 0) {
      this.emit('raw_page_size', result.data.length)

      for (let i = 0; i < result.data.length; i++) {
        const row = result.data[i]
        const obj = this._asObjects ? {} : undefined

        for (let i = 0; i < this._columns.length; i++) {
          row[i] = deserialize(this._columns[i], row[i])

          if (this._asObjects) {
            obj[this._columns[i].name] = row[i]
          }
        }

        if (this._asObjects) {
          result.data[i] = obj
        }
      }

      this._received = true
      this._buffer = result.data
    }

    // "If there is no nextUri link, then the query is finished (either
    // successfully completed or failed)"
    if (!result.nextUri) {
      debug('Query %s finished upstream', this._queryId || '[n/a]')
      this._upstreamFinished = true
    } else {
      // "Otherwise, keep following the nextUri link"
      const nextUri = new URL(result.nextUri)
      const path = nextUri.pathname + nextUri.search

      this._prevPath = this._requestOptions.path

      this._requestOptions = {
        // Follow host redirects, but don't switch protocols.
        hostname: nextUri.hostname,
        path: path,
        method: 'GET',
        json: true,
        expectStatusCode: 200
      }

      if (nextUri.port) {
        this._requestOptions.port = parseInt(nextUri.port, 10)
      }
    }

    this._reading = false
    this._read()
  }

  _destroy (err, callback) {
    clearTimeout(this._timeout)
    clearTimeout(this._retryTimeout)

    if (this._upstreamFinished || this._errored) {
      this._cancel(null, err, callback)
    } else if (this._queryId) {
      this._cancel(this._queryId, err, callback)
    } else if (this._inflight) {
      // Wait until we (maybe) get a query id.
      this._land = (ignoredResponseErr, result) => {
        this._cancel(result && result.id, err, callback)
      }
    } else {
      this._cancel(null, err, callback)
    }
  }

  _cancel (queryId, err, callback) {
    // Ignore result of inflight request if any
    this._land = noop

    // If we didn't get a query id, there's nothing to cancel.
    if (!queryId) return process.nextTick(callback, err)

    const request = {
      method: 'DELETE',
      path: `/v1/query/${queryId}`,
      expectStatusCode: 204
    }

    this.emit('request', request)
    this.emit('cancel')

    this._client.request(request, (cancelationErr) => {
      if (err && cancelationErr) callback(new AggregateError([err, cancelationErr]))
      else callback(err || cancelationErr)
    })
  }
}
