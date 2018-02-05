'use strict'

const Client = require('./lib/client')

module.exports = function lento (opts) {
  return new Client(opts)
}
