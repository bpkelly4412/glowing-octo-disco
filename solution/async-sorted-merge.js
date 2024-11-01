"use strict";

const { Heap } = require('heap-js')
const { sortLogs } = require('./utils');

const MAX_BUFFER_SIZE = 10;
let filterCount = 0;
let waitingBufferCount = 0;

// returns true if heap has log from every active source, returns false otherwise
function populateHeapFromBuffers(logSourceNeededInHeap, logMinHeap) {
  // filter out any drained log sources with empty buffers
  if (!logSourceNeededInHeap) {
    return true;
  }

  if (logSourceNeededInHeap.buffer.length === 0) {
    waitingBufferCount++;
    return false;
  }
  const log = logSourceNeededInHeap.buffer.pop();
  logMinHeap.push(log);
  logSourceNeededInHeap.numLogsInHeap++;
  return true;
}

let bufferFilterCount = 0;
let totalSourcesFetched = 0

async function populateBuffers(logSourcesById) {
  // filter out any drained log sources and full buffers before fetching
  while (!Object.values(logSourcesById).every(ls => ls.isDrained)) {
    if (Object.values(logSourcesById).filter(ls => !ls.isDrained && ls.buffer.length < MAX_BUFFER_SIZE).length > 0) {
      bufferFilterCount++;
      totalSourcesFetched += Object.values(logSourcesById).filter(ls => !ls.isDrained && ls.buffer.length < MAX_BUFFER_SIZE).length
      await Promise.all(Object.values(logSourcesById).filter(ls => !ls.isDrained && ls.buffer.length < MAX_BUFFER_SIZE).map(async ls => {
        const log = await ls.logSource.popAsync();
        if (log) {
          logSourcesById[ls.id].buffer.unshift({ logSourceId: ls.id, log });
        } else {
          logSourcesById[ls.id].isDrained = true;
        }
      }));
    }
    await new Promise(resolve => setTimeout(resolve, 0));  // Yield to the event loop
  }
}

async function populateHeapWithActiveLogSources(logSourcesById, logMinHeap) {
  // filter out any drained log sources and full buffers before fetching
  const logs = await Promise.all(Object.values(logSourcesById).filter(ls => !ls.isDrained && ls.numLogsInHeap < MAX_BUFFER_SIZE).map(async ls => {
    const log = await ls.logSource.popAsync();
    if (log) {
      logSourcesById[ls.id].numLogsInHeap++;
    } else {
      logSourcesById[ls.id].isDrained = true;
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
          numLogsInHeap: 0,
          isDrained: false
        },
        ...acc
      }), {});
      const logMinHeap = new Heap(sortLogs);
      let logSourceIdNeededInHeap = -1;


      // initialize heap with the least recent entry from each logSource
      await populateHeapWithActiveLogSources(logSourcesById, logMinHeap);

      populateBuffers(logSourcesById);

      while (!logMinHeap.isEmpty()) {
        // populate buffers from logSources (no await)
        // populateBuffers(logSourcesById);

        // populate heap from buffers for any logs without entries in heap
        if (populateHeapFromBuffers(logSourcesById[logSourceIdNeededInHeap], logMinHeap)) {
          // if every active log source has one entry in heap
          // remove and print most recent entry from heap, decrement count in heap
          const leastRecentLogWithInfo = logMinHeap.pop();
          printer.print(leastRecentLogWithInfo.log)
          logSourcesById[leastRecentLogWithInfo.logSourceId].numLogsInHeap--;
          if (logSourcesById[leastRecentLogWithInfo.logSourceId].numLogsInHeap === 0 && !logSourcesById[leastRecentLogWithInfo.logSourceId].isDrained) {
            logSourceIdNeededInHeap = leastRecentLogWithInfo.logSourceId;
          } else {
            logSourceIdNeededInHeap = -1;
          }
        }
        await new Promise(resolve => setTimeout(resolve, 0));  // Yield to the event loop
      }

      console.log('bufferFilterCount', bufferFilterCount)
      console.log('sourcePerFetch', totalSourcesFetched / bufferFilterCount)
      console.log('waitingBufferCount', waitingBufferCount)
      printer.done();
      resolve(console.log("Async sort complete."));
    } catch (err) {
      reject(err);
    }
  });
};
