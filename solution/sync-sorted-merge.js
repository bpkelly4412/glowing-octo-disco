"use strict";

const { Heap } = require('heap-js')
const { sortLogs } = require('./utils')

// Print all entries, across all of the sources, in chronological order.

module.exports = (logSources, printer) => {
  const logMinHeap = new Heap(sortLogs)
  // initialize heap with the least recent entry from each logSource
  for (let i = 0; i < logSources.length; i++) {
    const log = logSources[i].pop();
    if (log) {
      logMinHeap.push({ logSourceIndex: i, log })
    }
  }

  while (!logMinHeap.isEmpty()) {
    // remove and print most recent entry from heap
    const leastRecentLogWithInfo = logMinHeap.pop();
    printer.print(leastRecentLogWithInfo.log)

    // add from source that was just removed
    const nextLog = logSources[leastRecentLogWithInfo.logSourceIndex].pop();
    if (nextLog) {
      logMinHeap.push({ logSourceIndex: leastRecentLogWithInfo.logSourceIndex, log: nextLog })
    }
  }

  printer.done();
  return console.log("Sync sort complete.");
};
