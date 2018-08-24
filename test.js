'use strict'

const test = require('tape')
const nock = require('nock')
const lento = require('.')
const noop = () => {}
const VERSION = require('./package.json').version

test('sets headers', function (t) {
  t.plan(6)

  nock('http://localhost:8080')
    .post('/v1/statement')
    .reply(function (uri, requestBody, cb) {
      t.is(this.req.headers['user-agent'], `lento ${VERSION}`)
      t.is(this.req.headers['x-presto-source'], 'lento')
      t.is(this.req.headers['connection'], 'keep-alive')
      t.is(this.req.headers['accept-encoding'],  'gzip, deflate, identity')
      t.is(this.req.headers['accept'], 'application/json')

      cb(null, [200, {
        id: 'q1',
        columns: [
          { name: 'a' },
          { name: 'b' }
        ],
        data: [ [0, 0], [1, 1] ]
      }])
    })

  lento().query('select "foo"', (err) => {
    t.ifError(err, 'no query error')
  })
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

  const stream = lento().createRowStream()
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

  const stream = lento().createPageStream()
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
    .createRowStream()
    .on('data', function (row) {
      t.fail('not expecting data')
    })
    .on('error', function (err) {
      t.is(err && err.message, 'http 500')
    })
})
