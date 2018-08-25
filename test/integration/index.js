'use strict'

const test = require('tape')
const config = require('rc')('lento-test')
const lento = require('lento')
const factory = () => lento(config.presto)

test('setup', function (t) {
  t.plan(1)

  factory().query(`CREATE SCHEMA IF NOT EXISTS ${config.presto.schema}`, (err) => {
    t.ifError(err, 'no query error')
  })
})

test('teardown', function (t) {
  t.pass()
  t.end()
})
