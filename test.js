'use strict'

const test = require('tape')
const nock = require('nock')
const lento = require('.')
const noop = () => {}
const VERSION = require('./package.json').version

test('sets body and headers', function (t) {
  t.plan(7)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(function (uri, requestBody, cb) {
      t.is(requestBody, 'select 1')
      t.is(this.req.headers['user-agent'], `lento ${VERSION}`)
      t.is(this.req.headers['x-presto-source'], 'lento')
      t.is(this.req.headers['connection'], 'keep-alive')
      t.is(this.req.headers['accept-encoding'],  'gzip, deflate, identity')
      t.is(this.req.headers['accept'], 'application/json')

      cb(null, [200, {
        id: 'q1',
        columns: [{ name: 'a' }],
        data: [[1]]
      }])
    })

  lento().query('select 1', (err) => {
    t.ifError(err, 'no query error')
  })
})

test('Buffer query', function (t) {
  t.plan(2)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(function (uri, requestBody, cb) {
      t.is(requestBody, 'select 2')

      cb(null, [200, {
        id: 'q1',
        columns: [{ name: 'a' }],
        data: [[1]]
      }])
    })

  lento().query(Buffer.from('select 2'), (err) => {
    t.ifError(err, 'no query error')
  })
})

test('rejects query that is of wrong type or empty', function (t) {
  t.plan(6)

  const client = lento()

  try {
    client.query()
  } catch (err) {
    t.is(err.name, 'TypeError')
    t.is(err.message, 'First argument "sql" must be a string or Buffer')
  }

  try {
    client.query('')
  } catch (err) {
    t.is(err.name, 'Error')
    t.is(err.message, 'First argument "sql" must not be empty')
  }

  try {
    client.query(Buffer.alloc(0))
  } catch (err) {
    t.is(err.name, 'Error')
    t.is(err.message, 'First argument "sql" must not be empty')
  }
})

test('row stream', function (t) {
  t.plan(1)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(200, {
      id: 'q1',
      columns: [
        { name: 'a' },
        { name: 'b' }
      ],
      data: [ [0, 0], [1, 1] ]
    })

  const stream = lento().createRowStream('select 1')
  const emitted = []

  stream.on('data', function (row) {
    emitted.push(row)
  })

  stream.on('end', function () {
    t.same(emitted, [ { a: 0, b: 0 }, { a: 1, b: 1 } ])
  })
})

test('page stream', function (t) {
  t.plan(1)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(200, {
      id: 'q1',
      columns: [
        { name: 'a' },
        { name: 'b' }
      ],
      data: [ [0, 0], [1, 1] ]
    })

  const stream = lento().createPageStream('select 1')
  const emitted = []

  stream.on('data', function (rows) {
    emitted.push(rows)
  })

  stream.on('end', function () {
    t.same(emitted, [[ { a: 0, b: 0 }, { a: 1, b: 1 } ]])
  })
})

test('row stream: http error', function (t) {
  t.plan(1)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(500, {
      id: 'q1',
      columns: [
        { name: 'a' },
        { name: 'b' }
      ],
      data: [ [0, 0], [1, 1] ]
    })

  lento()
    .createRowStream('select 1')
    .on('data', function (row) {
      t.fail('not expecting data')
    })
    .on('error', function (err) {
      t.is(err && err.message, 'http 500')
    })
})

test('retries HTTP 503', function (t) {
  t.plan(1)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .times(3)
    .reply(503)

  lento({ maxRetries: 2 })
    .createRowStream('select 1')
    .on('data', noop)
    .on('error', function (err) {
      t.is(err && err.message, 'http 503')
    })
})

test('does not retry HTTP 503 if maxRetries is 0', function (t) {
  t.plan(1)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(503)

  lento({ maxRetries: 0 })
    .createRowStream('select 1')
    .on('data', noop)
    .on('error', function (err) {
      t.is(err && err.message, 'http 503')
    })
})

test('retries ECONNREFUSED', function (t) {
  t.plan(2)

  let retries = 0

  lento({ maxRetries: 1, port: 9999 })
    .on('retry', () => { retries++ })
    .createRowStream('select 1')
    .on('data', noop)
    .on('error', function (err) {
      t.is(err.code, 'ECONNREFUSED')
      t.is(retries, 1)
    })
})

test('does not retry ECONNREFUSED if maxRetries is 0', function (t) {
  t.plan(2)

  let retries = 0

  lento({ maxRetries: 0, port: 9999 })
    .on('retry', () => { retries++ })
    .createRowStream('select 1')
    .on('data', noop)
    .on('error', function (err) {
      t.is(err.code, 'ECONNREFUSED')
      t.is(retries, 0)
    })
})

test('row stream: presto error', function (t) {
  t.plan(4)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(200, {
      error: {
        message: 'Query exceeded maximum time limit of 1.00ms',
        errorName: 'EXCEEDED_TIME_LIMIT',
        errorType: 'INSUFFICIENT_RESOURCES'
      }
    })

  lento()
    .createRowStream('select 1')
    .on('data', function (row) {
      t.fail('not expecting data')
    })
    .on('error', function (err) {
      t.is(err.message, 'EXCEEDED_TIME_LIMIT: Query exceeded maximum time limit of 1.00ms')
      t.is(err.code, 'EXCEEDED_TIME_LIMIT')
      t.is(err.type, 'INSUFFICIENT_RESOURCES')
      t.is(err.name, 'PrestoError')
    })
})

test('no cancelation after upstream is finished', function (t) {
  t.plan(3)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(200, {
      id: 'q1',
      columns: [{ name: 'a' }],
      data: [[1], [2]]
    })

  const stream = lento().createRowStream('select 1', { pageSize: 1 })
  const order = []

  stream.on('cancel', () => {
    t.fail('should not cancel')
  })

  stream.on('close', () => {
    order.push('close')
    t.same(order, ['data', 'close'])
  })

  stream.on('data', (row) => {
    order.push('data')

    t.is(stream._upstreamFinished, true, 'upstream finished')
    t.same(row, { a: 1 }, 'data ok')

    stream.pause()
    stream.destroy()
  })
})
