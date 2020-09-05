'use strict'

const test = require('tape')
const host = require('docker-host')
const lento = require('../../lento')

const config = {
  hostname: host().host,
  port: 8080,
  user: 'test',
  schema: 'test',
  catalog: 'memory',
  timezone: 'UTC'
}

const factory = (opts) => lento({ ...config, ...opts })

test('setup', function (t) {
  factory().query(`CREATE SCHEMA IF NOT EXISTS ${config.schema}`, (err) => {
    t.ifError(err, 'no query error')
    t.end()
  })
})

test('basic', function (t) {
  t.plan(4)

  const client = factory()
  const table = 'basic_' + Date.now()

  client.query(`CREATE TABLE ${table} (name varchar(30))`, (err) => {
    t.ifError(err, 'no query error')

    client.query(`INSERT INTO ${table} VALUES ('a'), ('b')`, (err) => {
      t.ifError(err, 'no query error')

      client.query(`SELECT * FROM ${table}`, (err, rows) => {
        t.ifError(err, 'no query error')
        t.same(rows, [{ name: 'a' }, { name: 'b' }])
      })
    })
  })
})

test('timestamp is returned with default precision of 3', function (t) {
  t.plan(4)

  const client = factory()
  const table = 'timestamp_' + Date.now()

  client.query(`CREATE TABLE ${table} (ts timestamp(6))`, (err) => {
    t.ifError(err, 'no query error')

    client.query(`INSERT INTO ${table} VALUES (timestamp '2020-09-05 10:01:48.123456')`, (err) => {
      t.ifError(err, 'no query error')

      client.query(`SELECT * FROM ${table}`, { deserialize: false }, (err, rows) => {
        t.ifError(err, 'no query error')
        t.same(rows, [{ ts: '2020-09-05 10:01:48.123' }])
      })
    })
  })
})

test('enable PARAMETRIC_DATETIME capability', function (t) {
  t.plan(4)

  const client = factory({ parametricDatetime: true })
  const table = 'parametric_datetime_' + Date.now()

  client.query(`CREATE TABLE ${table} (ts timestamp(6))`, (err) => {
    t.ifError(err, 'no query error')

    client.query(`INSERT INTO ${table} VALUES (timestamp '2020-09-05 10:01:48.123456')`, (err) => {
      t.ifError(err, 'no query error')

      client.query(`SELECT * FROM ${table}`, { deserialize: false }, (err, rows) => {
        t.ifError(err, 'no query error')
        t.same(rows, [{ ts: '2020-09-05 10:01:48.123456' }])
      })
    })
  })
})
