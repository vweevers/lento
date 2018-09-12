'use strict'

module.exports = class PrestoError extends Error {
  constructor (error) {
    const { message, errorName, errorType, failureInfo } = error

    super(errorName + ': ' + message)

    // There's also error.ErrorCode, a numeric representation of
    // errorName, but in Node.js we prefer the code to be a string.
    Object.defineProperty(this, 'code', { value: errorName })
    Object.defineProperty(this, 'type', { value: errorType })
    Object.defineProperty(this, 'name', { value: 'PrestoError' })

    if (failureInfo) {
      // The failureInfo property is an object, not always available.
      Object.defineProperty(this, 'info', { value: failureInfo })
    }
  }
}
