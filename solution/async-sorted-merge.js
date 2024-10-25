"use strict";

const { Heap } = require('heap-js')
const { sortLogs } = require('./utils')

async function populateHeapWithActiveLogSources(activeLogSourcesWithId, logMinHeap, logCountBySource) {
  const logs = await Promise.all(activeLogSourcesWithId.map(async ls => {
    const log = await ls.logSource.popAsync();
    if (log) {
      logCountBySource[ls.id]++;
    }
    return { logSourceId: ls.id, log };
  }));
  // prevent falsy logs from entering heap
  const filteredLogs = logs.filter(logWithInfo => !!logWithInfo.log);

  logMinHeap.push(...filteredLogs)
  // filter out any drained log sources
  activeLogSourcesWithId = activeLogSourcesWithId.filter(ls => !ls.logSource.drained)
}

module.exports = (logSources, printer) => {
  return new Promise(async (resolve, reject) => {
    try {
      // keep track of active log sources so we don't try to popAsync from an drained log source
      const activeLogSourcesWithId = logSources.map((ls, i) => ({ id: i, logSource: ls }))
      const logMinHeap = new Heap(sortLogs);
      // keep count of logs in the heap by log source so we can determine if we need to fetch another round
      const logCountBySource = activeLogSourcesWithId.reduce((prev, curr) => {
        prev[curr.id] = 0;
        return prev;
      }, {})

      // initialize heap with the least recent entry from each logSource
      await populateHeapWithActiveLogSources(activeLogSourcesWithId, logMinHeap, logCountBySource);

      while (!logMinHeap.isEmpty()) {
        // remove and print most recent entry from heap, decrement count in heap
        const leastRecentLogWithInfo = logMinHeap.pop();
        printer.print(leastRecentLogWithInfo.log)
        logCountBySource[leastRecentLogWithInfo.logSourceId]--;

        // if there are no remaining entries for the logSource in the heap, then fetch from all logSources again
        if (logCountBySource[leastRecentLogWithInfo.logSourceId] === 0) {
          await populateHeapWithActiveLogSources(activeLogSourcesWithId, logMinHeap, logCountBySource);
        }
      }

      printer.done();
      resolve(console.log("Async sort complete."));

    } catch (err) {
      reject(err);
    }
  });
};
