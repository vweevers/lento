'use strict'

module.exports = class PrestoError extends Error {
  constructor (error) {
    // Non-null strings
    const { message, errorName, errorType } = error

    // Optional int, object
    const { errorCode, failureInfo } = error

    super(errorName + ': ' + message)

    // ErrorCode is an integer representation of errorName, an
    // enum of sorts. In Node.js, prefer code to be a string.
    Object.defineProperty(this, 'code', { value: errorName })
    Object.defineProperty(this, 'type', { value: errorType })
    Object.defineProperty(this, 'name', { value: 'PrestoError' })

    if (failureInfo) {
      Object.defineProperty(this, 'info', { value: failureInfo })
    }
  }
}
