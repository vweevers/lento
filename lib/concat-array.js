'use strict'

// Adapted from feross/simple-concat
module.exports = function (stream, callback) {
  const chunks = []

  stream.on('data', function (chunk) {
    chunks.push(chunk)
  })

  stream.on('end', function () {
    if (callback) callback(null, chunks)
    callback = null
  })

  stream.on('error', function (err) {
    if (callback) callback(err)
    callback = null
  })
}
