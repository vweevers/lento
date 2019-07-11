'use strict'

const test = require('tape')
const nock = require('nock')
const lento = require('..')
const noop = () => {}
const VERSION = require('../package.json').version

test('sets body and headers', function (t) {
  t.plan(7)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(function (uri, requestBody, cb) {
      t.is(requestBody, 'select 1')
      t.is(this.req.headers['user-agent'], `lento ${VERSION}`)
      t.is(this.req.headers['x-presto-source'], 'lento')
      t.is(this.req.headers['connection'], 'keep-alive')
      t.is(this.req.headers['accept-encoding'], 'gzip, deflate, identity')
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
      data: [[0, 0], [1, 1]]
    })

  const stream = lento().createRowStream('select 1')
  const emitted = []

  stream.on('data', function (row) {
    emitted.push(row)
  })

  stream.on('end', function () {
    t.same(emitted, [{ a: 0, b: 0 }, { a: 1, b: 1 }])
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
      data: [[0, 0], [1, 1]]
    })

  const stream = lento().createPageStream('select 1')
  const emitted = []

  stream.on('data', function (rows) {
    emitted.push(rows)
  })

  stream.on('end', function () {
    t.same(emitted, [[{ a: 0, b: 0 }, { a: 1, b: 1 }]])
  })
})

test('follows nextUri', function (t) {
  t.plan(1)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(200, { id: 'q1', nextUri: 'http://localhost:8080/two' })
    .get('/two')
    // lento does not change the initial protocol (in this case, http)
    .reply(200, { nextUri: 'https://other-host:8081/three' })

  nock('http://other-host:8081')
    .get('/three')
    .reply(200, {})

  const stream = lento().createRowStream('select 1')
  const requests = []

  stream.on('request', (requestOptions) => {
    requests.push({ port: requestOptions.port, path: requestOptions.path })
  })

  stream.on('data', function () {
    t.fail('not expecting data')
  })

  stream.on('end', function () {
    t.same(requests, [
      { port: undefined, path: '/v1/statement' },
      { port: 8080, path: '/two' },
      { port: 8081, path: '/three' }
    ])
  })
})

test('retries query after presto error and having followed a nextUri', function (t) {
  t.plan(3)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(200, { id: 'q1', nextUri: 'http://localhost:8080/q1' })
    .get('/q1')
    .reply(200, {
      error: {
        message: 'not relevant',
        errorName: 'SERVER_STARTING_UP',
        errorType: 'not relevant'
      }
    })
    .post('/v1/statement')
    .reply(200, { id: 'q2', nextUri: 'http://localhost:8080/q2' })
    .get('/q2')
    .reply(200, {
      columns: [{ name: 'a' }],
      data: [[0], [1]]
    })

  const client = lento()
  const stream = client.createPageStream('select 1')
  const requests = []
  const ids = []
  const uniqueRequests = new Set()

  client.on('_request', (requestOptions) => {
    if (uniqueRequests.has(requestOptions)) {
      t.fail('should not reuse requestOptions')
    } else {
      uniqueRequests.add(requestOptions)
    }

    // Clone because http module mutates requestOptions object
    const clone = JSON.parse(JSON.stringify(requestOptions))

    delete clone.host
    delete clone.proto

    requests.push(clone)
  })

  stream.on('id', (queryId) => {
    ids.push(queryId)
  })

  stream.on('data', (page) => {
    t.same(page, [{ a: 0 }, { a: 1 }])
  })

  const headers = {
    Accept: 'application/json',
    'X-Presto-Source': 'lento',
    'User-Agent': `lento ${VERSION}`,
    Connection: 'keep-alive',
    'Accept-Encoding': 'gzip, deflate, identity'
  }

  stream.on('end', function () {
    t.same(ids, ['q1', 'q2'])
    t.same(requests, [{
      protocol: 'http:',
      hostname: 'localhost',
      port: 8080,
      path: '/v1/statement',
      method: 'POST',
      body: 'select 1',
      json: true,
      expectStatusCode: 200,
      headers: headers
    }, {
      protocol: 'http:',
      hostname: 'localhost',
      port: 8080,
      path: '/q1',
      method: 'GET',
      json: true,
      expectStatusCode: 200,
      headers: headers
    }, {
      protocol: 'http:',
      hostname: 'localhost',
      port: 8080,
      path: '/v1/statement',
      method: 'POST',
      body: 'select 1',
      json: true,
      expectStatusCode: 200,
      headers: headers
    }, {
      protocol: 'http:',
      hostname: 'localhost',
      port: 8080,
      path: '/q2',
      method: 'GET',
      json: true,
      expectStatusCode: 200,
      headers: headers
    }])
  })
})

test('does not retry query after presto error and nextUri if data was received', function (t) {
  t.plan(1)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(200, {
      id: 'q1',
      nextUri: 'http://localhost:8080/q1',
      columns: [{ name: 'a' }],
      data: [[0], [1]]
    })
    .get('/q1')
    .reply(200, {
      error: {
        message: 'not relevant',
        errorName: 'SERVER_STARTING_UP',
        errorType: 'not relevant'
      }
    })

  const client = lento()
  const stream = client.createPageStream('select 1')

  stream
    .on('data', noop)
    .on('cancel', () => {
      t.fail('should not cancel')
    })
    .on('error', (err) => {
      t.is(err.code, 'SERVER_STARTING_UP')
    })
})

test('does not retry query after presto error and nextUri if maxRetries is 0', function (t) {
  t.plan(1)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(200, { id: 'q1', nextUri: 'http://localhost:8080/q1' })
    .get('/q1')
    .reply(200, {
      error: {
        message: 'not relevant',
        errorName: 'SERVER_STARTING_UP',
        errorType: 'not relevant'
      }
    })

  const client = lento({ maxRetries: 0 })
  const stream = client.createPageStream('select 1')

  stream.on('data', (page) => {
    t.fail('not expecting data')
  })

  stream.on('error', (err) => {
    t.is(err.code, 'SERVER_STARTING_UP')
  })
})

test('does not retry query after presto error if maxRetries is 0', function (t) {
  t.plan(1)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(200, {
      error: {
        message: 'not relevant',
        errorName: 'SERVER_STARTING_UP',
        errorType: 'not relevant'
      }
    })

  const client = lento({ maxRetries: 0 })
  const stream = client.createPageStream('select 1')

  stream.on('data', (page) => {
    t.fail('not expecting data')
  })

  stream.on('error', (err) => {
    t.is(err.code, 'SERVER_STARTING_UP')
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
      data: [[0, 0], [1, 1]]
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
  t.plan(3)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .times(3)
    .reply(503)

  const uniqueRequests = new Set()
  const allRequests = []

  lento({ maxRetries: 2 })
    .on('_request', (requestOptions) => {
      uniqueRequests.add(requestOptions)
      allRequests.push(requestOptions)
    })
    .createRowStream('select 1')
    .on('data', noop)
    .on('error', function (err) {
      t.is(err && err.message, 'http 503')
      t.is(uniqueRequests.size, 1)
      t.is(allRequests.length, 3)
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

test('skip cancelation after stream is destroyed', function (t) {
  t.plan(3)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(200, {
      error: {
        message: 'not relevant',
        errorName: 'TEST',
        errorType: 'not relevant'
      }
    })

  const client = lento()
  const stream = client.createPageStream('select 1')

  stream
    .on('data', noop)
    .on('error', (err) => {
      t.is(err.code, 'TEST', 'got code')
      t.is(stream.destroyed, true, 'is destroyed')

      stream._destroy = function () {
        t.fail('should not be called again')
      }

      stream.destroy()
    })
    .on('cancel', () => {
      t.fail('should not cancel')
    })
    .on('close', () => {
      t.pass('closed')
    })
})

test('SET SESSION', function (t) {
  t.plan(6)

  const client = lento()
  const kv = 'test_key=false'

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(function (uri, requestBody, cb) {
      t.is(requestBody, `SET SESSION ${kv}`, 'got session query')
      t.is(this.req.headers['x-presto-session'], undefined, 'no session')

      cb(null, [200, {
        updateType: 'SET SESSION',
        columns: [{ name: 'result', type: 'boolean' }],
        data: [[true]]
      }, {
        'x-presto-set-session': kv
      }])
    })
    .post('/v1/statement')
    .reply(function (uri, requestBody, cb) {
      t.is(requestBody, 'select 1', 'got regular query')
      t.is(this.req.headers['x-presto-session'], kv, 'got session')

      cb(null, [200, {
        id: 'q1',
        columns: [{ name: 'a' }],
        data: [['1']]
      }])
    })

  client.set('test_key', false, (err) => {
    t.ifError(err, 'no set error')

    client.query('select 1', (err) => {
      t.ifError(err, 'no query error')
    })
  })
})
