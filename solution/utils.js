function sortLogs(logWithInfoA, logWithInfoB) {
    return logWithInfoA.log.date - logWithInfoB.log.date
}

module.exports = {
    sortLogs
}