'use strict'

module.exports = function (column, value) {
  if (value === null) {
    return value
  } else if (column.type === 'timestamp') {
    // Quick hack
    const isoString = value.replace(' ', 'T') + 'Z'
    return new Date(isoString)
  } else {
    return value
  }
}
