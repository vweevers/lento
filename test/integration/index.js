'use strict'

const test = require('tape')
const host = require('docker-host')
const lento = require('../../lento')

const config = {
  hostname: host().host,
  port: 8080,
  user: 'test',
  schema: 'test',
  catalog: 'hive',
}

const factory = () => lento(config)

test('setup', function (t) {
  t.plan(5)

  const client = factory()
  const table = 'test_' + Date.now()

  // Dummy test.
  client.query(`CREATE SCHEMA IF NOT EXISTS ${config.schema}`, (err) => {
    t.ifError(err, 'no query error')

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
})

test('teardown', function (t) {
  t.pass()
  t.end()
})
