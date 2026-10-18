// Shared by the upload page and the results page: polling a batch's status.
//
// Polling, not websockets: progress changes every few seconds at most, and a
// GET every 3 seconds is simple, cacheable-proof (the API sends no-store),
// and needs nothing extra on the server or in AWS.

'use strict';

const POLL_INTERVAL_MS = 3_000;
const POLL_MAX_BACKOFF_MS = 30_000;
const numberFormat = new Intl.NumberFormat();

// pollBatch calls onUpdate(batch) with each GET /api/batches/{id} response,
// onUpdate({state: 'waiting'}) while the batch doesn't exist yet (the S3 event
// and the planner take a few seconds after upload), and onUpdate(null, err) on
// errors. It pauses while the tab is hidden, backs off when the server is
// unreachable, and stops by itself once the batch is completed or failed.
// It returns a function that stops it early.
function pollBatch(id, onUpdate) {
  let timer = null;
  let running = false;
  let stopped = false;
  let failures = 0;

  async function tick() {
    timer = null;
    if (stopped || running || document.hidden) return; // a hidden tab resumes on visibilitychange
    running = true;
    let delay = POLL_INTERVAL_MS;
    try {
      const res = await fetch(`/api/batches/${encodeURIComponent(id)}`, { cache: 'no-store' });
      if (res.status === 404) {
        onUpdate({ state: 'waiting' });
      } else if (!res.ok) {
        throw new Error(`status request failed with HTTP ${res.status}`);
      } else {
        const batch = await res.json();
        failures = 0;
        onUpdate(batch);
        if (batch.state === 'completed' || batch.state === 'failed') {
          stop();
        }
      }
    } catch (err) {
      failures++;
      delay = Math.min(POLL_MAX_BACKOFF_MS, POLL_INTERVAL_MS * 2 ** failures);
      onUpdate(null, err);
    } finally {
      running = false;
    }
    if (!stopped) timer = setTimeout(tick, delay);
  }

  function onVisibility() {
    if (!document.hidden && !timer && !running && !stopped) tick();
  }

  function stop() {
    stopped = true;
    clearTimeout(timer);
    document.removeEventListener('visibilitychange', onVisibility);
  }

  document.addEventListener('visibilitychange', onVisibility);
  tick();
  return stop;
}

// describeBatch turns a status response into what both pages display.
function describeBatch(b) {
  const n = (x) => numberFormat.format(x);
  const handled = b.processed + b.failed + b.skipped;
  const percent = b.total > 0 ? Math.floor((handled / b.total) * 100) : 100;
  switch (b.state) {
    case 'waiting':
      return { pill: 'Queued', tone: 'waiting', percent: 0, headline: 'Waiting for processing to start…' };
    case 'failed':
      return { pill: 'Failed', tone: 'failed', percent: 0, headline: b.error || 'The zip could not be processed.' };
    case 'unpacking':
      return {
        pill: 'Unpacking', tone: 'uploading', percent,
        headline: `Unpacking the zip: ${n(b.chunks.done)} of ${n(b.chunks.total)} ${b.chunks.total === 1 ? 'chunk' : 'chunks'} done`,
      };
    case 'processing':
      return {
        pill: 'Processing', tone: 'uploading', percent,
        headline: `${n(b.processed)} of ${n(b.total - b.skipped)} documents processed`,
      };
    default:
      return {
        pill: 'Complete', tone: 'done', percent: 100,
        headline: `All ${n(b.total)} ${b.total === 1 ? 'entry' : 'entries'} handled`,
      };
  }
}
