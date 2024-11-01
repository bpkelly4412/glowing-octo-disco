"use strict";

const { Heap } = require('heap-js')
const { sortLogs } = require('./utils')

const MAX_BUFFER_SIZE = 10;

async function populateHeapWithActiveLogSources(logSourcesById, logMinHeap) {
  // filter out any drained log sources and full buffers before fetching
  const logs = await Promise.all(Object.values(logSourcesById).filter(ls => !ls.logSource.drained && ls.numLogsInHeap < MAX_BUFFER_SIZE).map(async ls => {
    const log = await ls.logSource.popAsync();
    if (log) {
      logSourcesById[ls.id].numLogsInHeap++;
    }
    return { logSourceId: ls.id, log };
  }));
  // prevent falsy logs from entering heap
  const filteredLogs = logs.filter(logWithInfo => !!logWithInfo.log);

  logMinHeap.push(...filteredLogs)
}

module.exports = (logSources, printer) => {
  return new Promise(async (resolve, reject) => {
    try {
      // keep track of active log sources so we don't try to popAsync from an drained log source
      const logSourcesById = logSources.reduce((acc, curr, i) => ({
        [i]: {
          id: i,
          buffer: [],
          logSource: curr,
          numLogsInHeap: 0
        },
        ...acc
      }), {});
      const logMinHeap = new Heap(sortLogs);


      // initialize heap with the least recent entry from each logSource
      await populateHeapWithActiveLogSources(logSourcesById, logMinHeap);

      while (!logMinHeap.isEmpty()) {
        // remove and print most recent entry from heap, decrement count in heap
        const leastRecentLogWithInfo = logMinHeap.pop();
        printer.print(leastRecentLogWithInfo.log)
        logSourcesById[leastRecentLogWithInfo.logSourceId].numLogsInHeap--;

        // if there are no remaining entries for the logSource in the heap, then fetch from all logSources again
        if (logSourcesById[leastRecentLogWithInfo.logSourceId].numLogsInHeap === 0) {
          await populateHeapWithActiveLogSources(logSourcesById, logMinHeap);
        }
      }

      printer.done();
      resolve(console.log("Async sort complete."));

    } catch (err) {
      reject(err);
    }
  });
};
