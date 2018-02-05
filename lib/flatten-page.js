'use strict'

const through2 = require('through2')

module.exports = through2.ctor({ objectMode: true }, function (rows, enc, next) {
  for (let row of rows) {
    this.push(row)
  }

  next()
})
