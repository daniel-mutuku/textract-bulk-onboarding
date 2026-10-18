// Uploads a zip straight to S3 with a presigned multipart upload:
//   1. POST /api/uploads              -> the server opens the upload and picks the part size
//   2. for each part, a few at a time:
//        POST .../part-url            -> a URL signed for just this part
//        PUT  <url> with the bytes    -> S3 stores the part and returns its ETag
//   3. POST .../complete with ETags   -> S3 joins the parts, in order, into one object
// If the user cancels or a part keeps failing: POST .../abort, so S3 discards
// every stored part instead of keeping (and billing for) them.

'use strict';

// How many parts are in flight at once. More helps on fast links, but each
// one holds a connection and a part's worth of bytes.
const CONCURRENCY = 4;
const MAX_ATTEMPTS = 5; // per request, counting the first try
const BACKOFF_BASE_MS = 1_000;
const BACKOFF_CAP_MS = 20_000;
const STALL_TIMEOUT_MS = 60_000; // a PUT that sends nothing for this long is retried

class RequestError extends Error {
  constructor(message, retryable) {
    super(message);
    this.name = 'RequestError';
    this.retryable = retryable;
  }
}

class CancelledError extends Error {
  constructor() {
    super('Upload cancelled');
    this.name = 'CancelledError';
  }
}

const $ = (id) => document.getElementById(id);
const ui = {
  dropzone: $('dropzone'),
  input: $('file'),
  panel: $('panel'),
  name: $('name'),
  meta: $('meta'),
  pill: $('pill'),
  bar: $('bar'),
  barFill: $('bar-fill'),
  percent: $('stat-percent'),
  transferred: $('stat-transferred'),
  speed: $('stat-speed'),
  eta: $('stat-eta'),
  parts: $('parts'),
  partsMap: $('parts-map'),
  partsSummary: $('parts-summary'),
  notice: $('notice'),
  cancel: $('cancel'),
  again: $('again'),
  foot: $('foot'),
  processing: $('processing'),
  procPill: $('proc-pill'),
  procHeadline: $('proc-headline'),
  procBar: $('proc-bar'),
  procFill: $('proc-fill'),
  procProcessed: $('proc-processed'),
  procPending: $('proc-pending'),
  procSkipped: $('proc-skipped'),
  procFailed: $('proc-failed'),
  procNote: $('proc-note'),
  resultsLink: $('results-link'),
};

// The upload in progress: { controller, up }. `up` is the start response.
let current = null;

// ---------- Wiring ----------

ui.foot.textContent =
  `Presigned S3 multipart · ${CONCURRENCY} parts in parallel · up to ${MAX_ATTEMPTS} attempts per part`;

ui.input.addEventListener('change', () => {
  const file = ui.input.files[0];
  ui.input.value = '';
  if (file) upload(file);
});

for (const type of ['dragenter', 'dragover']) {
  ui.dropzone.addEventListener(type, (e) => {
    e.preventDefault();
    ui.dropzone.classList.add('is-dragging');
  });
}
for (const type of ['dragleave', 'drop']) {
  ui.dropzone.addEventListener(type, () => ui.dropzone.classList.remove('is-dragging'));
}
ui.dropzone.addEventListener('drop', (e) => {
  const file = e.dataTransfer.files[0];
  if (file) upload(file);
});
// A file dropped anywhere else would make the browser navigate to it.
for (const type of ['dragover', 'drop']) {
  window.addEventListener(type, (e) => e.preventDefault());
}

ui.cancel.addEventListener('click', () => {
  if (!current) return;
  ui.cancel.disabled = true;
  ui.cancel.textContent = 'Cancelling…';
  current.controller.abort(new CancelledError());
});

ui.again.addEventListener('click', () => view.reset());

window.addEventListener('beforeunload', (e) => {
  if (current) e.preventDefault(); // asks "Leave site?"
});

// If they leave anyway, tell the server to abort. sendBeacon is the one request
// a browser still delivers while the page is being torn down.
window.addEventListener('pagehide', () => {
  if (current?.up) {
    const { id, uploadId } = current.up;
    navigator.sendBeacon(`/api/uploads/${id}/abort`, JSON.stringify({ uploadId }));
  }
});

// ---------- Upload flow ----------

async function upload(file) {
  if (current) return;
  const controller = new AbortController();
  const { signal } = controller;
  current = { controller, up: null };
  view.begin(file);

  let up = null;
  let progress = null;
  let retries = 0;
  try {
    if (!/\.zip$/i.test(file.name)) {
      throw new RequestError('Only .zip files can be uploaded.', false);
    }
    // No signal on purpose: if the user cancels while this is in flight, we
    // still want the upload ID back so the abort below can clean it up.
    up = await withRetry(() => api('/api/uploads', {
      filename: file.name,
      size: file.size,
      contentType: file.type,
    }));
    current.up = up;
    signal.throwIfAborted();
    view.planned(up);

    progress = new Progress(file.size, view.progress);
    const onRetry = (partNumber, err, attempt, delay) => {
      retries++;
      view.retrying(partNumber, err, attempt, delay, retries);
    };
    const parts = await runPool(up.partCount, CONCURRENCY, controller,
      (partNumber) => uploadPart(file, up, partNumber, progress, signal, onRetry));
    progress.stop();

    // Past the point of no return: cancelling now would race the assembly.
    view.finishing(up.partCount);
    const done = await withRetry(() => api(`/api/uploads/${up.id}/complete`, {
      uploadId: up.uploadId,
      parts,
    }));
    view.done(done.key, retries);
    // The batch ID is the upload ID, so polling can start right away.
    processing.start(up.id);
  } catch (err) {
    controller.abort(err); // stop anything still running; no-op if already aborted
    progress?.stop();
    const cancelled = signal.reason instanceof CancelledError;
    const cleaned = up ? await abortUpload(up) : true;
    view.stopped(cancelled ? null : err, Boolean(up), cleaned);
  } finally {
    current = null;
  }
}

async function uploadPart(file, up, partNumber, progress, signal, onRetry) {
  // slice() reads nothing yet: the Blob only records a byte range of the file
  // on disk, and the browser streams it from disk while the PUT is sent.
  const start = (partNumber - 1) * up.partSize;
  const blob = file.slice(start, Math.min(start + up.partSize, file.size));

  try {
    const etag = await withRetry(async () => {
      view.part(partNumber, 'active');
      progress.update(partNumber, 0); // a retry resends the whole part
      // A fresh URL every attempt: the previous one may have expired while we
      // were backing off.
      const { url } = await api(`/api/uploads/${up.id}/part-url`, {
        uploadId: up.uploadId,
        partNumber,
      }, signal);
      return putPart(url, blob, signal, (loaded) => progress.update(partNumber, loaded));
    }, {
      signal,
      onRetry: (err, attempt, delay) => {
        view.part(partNumber, 'retrying');
        onRetry(partNumber, err, attempt, delay);
      },
    });
    progress.update(partNumber, blob.size);
    view.part(partNumber, 'done');
    return { partNumber, etag };
  } catch (err) {
    // If the upload was already stopped, this part is collateral, not the cause.
    if (signal.aborted) {
      view.part(partNumber, 'pending');
      throw err;
    }
    view.part(partNumber, 'failed');
    err.message = `part ${partNumber} failed: ${err.message}`;
    throw err;
  }
}

// PUT one part to its presigned URL and resolve with the ETag S3 returns.
// XMLHttpRequest rather than fetch because only XHR reports upload progress.
function putPart(url, blob, signal, onProgress) {
  return new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const xhr = new XMLHttpRequest();
    let stalled = false;
    let stallTimer;

    const watchForStall = () => {
      clearTimeout(stallTimer);
      stallTimer = setTimeout(() => {
        stalled = true;
        xhr.abort();
      }, STALL_TIMEOUT_MS);
    };
    const onAbort = () => xhr.abort();
    const settle = (fn, value) => {
      clearTimeout(stallTimer);
      signal.removeEventListener('abort', onAbort);
      fn(value);
    };
    signal.addEventListener('abort', onAbort, { once: true });

    xhr.open('PUT', url);
    xhr.upload.onprogress = (e) => {
      watchForStall();
      onProgress(e.loaded);
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        // Readable only because the bucket's CORS rule exposes the ETag header.
        const etag = xhr.getResponseHeader('ETag');
        if (etag) {
          settle(resolve, etag);
        } else {
          settle(reject, new RequestError(
            'S3 sent no ETag; the bucket CORS rule must expose the ETag header', false));
        }
        return;
      }
      const { code, message } = parseS3Error(xhr.responseText);
      settle(reject, new RequestError(
        `S3 returned HTTP ${xhr.status}${code ? ` (${code})` : ''}`,
        isRetryableS3Error(xhr.status, code, message)));
    };
    // A CORS rejection also lands here: the browser hides the real reason.
    xhr.onerror = () => settle(reject, new RequestError(
      'network error reaching S3 (if this persists, check the bucket CORS allows this origin)', true));
    xhr.onabort = () => settle(reject, stalled
      ? new RequestError(`no progress for ${STALL_TIMEOUT_MS / 1000}s`, true)
      : signal.reason);

    watchForStall();
    xhr.send(blob);
  });
}

function isRetryableS3Error(status, code, message) {
  if (status >= 500 || status === 408 || status === 429) return true;
  if (code === 'RequestTimeout' || code === 'SlowDown') return true;
  // An expired URL: the retry fetches a freshly signed one.
  return status === 403 && /expired/i.test(message);
}

// Run worker(1..count) with at most `limit` at once and return results in
// order. Each slot takes the next part as soon as its current one finishes,
// so one slow part doesn't hold the rest back. On the first failure it aborts
// the controller and waits for every slot to stop before throwing, so no part
// is still uploading when the caller aborts the multipart upload.
async function runPool(count, limit, controller, worker) {
  const results = new Array(count);
  let next = 1;

  async function slot() {
    while (!controller.signal.aborted && next <= count) {
      const n = next++;
      try {
        results[n - 1] = await worker(n);
      } catch (err) {
        controller.abort(err); // only the first abort sets the reason
        return;
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, count) }, slot));
  controller.signal.throwIfAborted();
  return results;
}

// Call fn until it succeeds, it fails with a non-retryable error, or attempts
// run out. Waits between attempts use exponential backoff with full jitter: a
// random delay up to 1s, 2s, 4s, 8s… capped at 20s. The randomness keeps parts
// that failed together (say, a network blip) from all retrying at the same moment.
async function withRetry(fn, { signal, onRetry } = {}) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (signal?.aborted) throw signal.reason;
      if (!err.retryable || attempt >= MAX_ATTEMPTS) {
        if (attempt > 1) err.message += ` after ${attempt} attempts`;
        throw err;
      }
      const delay = Math.random() * Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** (attempt - 1));
      onRetry?.(err, attempt, delay);
      await sleep(delay, signal);
    }
  }
}

async function abortUpload(up) {
  try {
    await withRetry(() => api(`/api/uploads/${up.id}/abort`, { uploadId: up.uploadId }));
    return true;
  } catch {
    return false;
  }
}

async function api(path, body, signal) {
  let res;
  try {
    res = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal,
    });
  } catch (err) {
    if (signal?.aborted) throw signal.reason;
    throw new RequestError('could not reach the server', true);
  }
  if (res.status === 204) return null;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    // 4xx means the request itself is wrong and will fail again; 5xx may not.
    throw new RequestError(data.error || `HTTP ${res.status}`, res.status >= 500 || res.status === 429);
  }
  return data;
}

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    signal?.throwIfAborted();
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

// S3 errors are XML: <Error><Code>…</Code><Message>…</Message></Error>.
function parseS3Error(xml) {
  const doc = new DOMParser().parseFromString(xml || '', 'application/xml');
  return {
    code: doc.querySelector('Code')?.textContent ?? '',
    message: doc.querySelector('Message')?.textContent ?? '',
  };
}

// ---------- Progress ----------

// Sums bytes sent across the parts in flight and renders at most once a frame.
// Speed is measured over the last few seconds so it reacts to stalls and retries.
class Progress {
  constructor(total, render) {
    this.total = total;
    this.render = render;
    this.loadedByPart = new Map();
    this.loaded = 0;
    this.samples = [];
    this.pending = false;
    this.stopped = false;
  }

  // No more renders, so a late frame can't overwrite the final state.
  stop() {
    this.stopped = true;
  }

  update(partNumber, loaded) {
    this.loaded += loaded - (this.loadedByPart.get(partNumber) ?? 0);
    this.loadedByPart.set(partNumber, loaded);
    if (this.pending) return;
    this.pending = true;
    requestAnimationFrame(() => {
      this.pending = false;
      if (!this.stopped) this.render(this.snapshot());
    });
  }

  snapshot() {
    const now = performance.now();
    this.samples.push({ t: now, loaded: this.loaded });
    while (this.samples.length > 2 && now - this.samples[0].t > 5_000) this.samples.shift();
    const first = this.samples[0];
    const seconds = (now - first.t) / 1000;
    const rate = seconds > 0.5 ? Math.max(0, (this.loaded - first.loaded) / seconds) : 0;
    return {
      loaded: this.loaded,
      total: this.total,
      fraction: this.loaded / this.total,
      rate,
      secondsLeft: rate > 0 ? (this.total - this.loaded) / rate : null,
    };
  }
}

// ---------- View ----------

const view = {
  file: null,
  cells: [],
  partsDone: 0,
  partCount: 0,
  retries: 0,

  reset() {
    processing.reset();
    ui.panel.hidden = true;
    ui.dropzone.hidden = false;
    ui.input.focus();
  },

  begin(file) {
    this.file = file;
    this.cells = [];
    this.partsDone = 0;
    this.partCount = 0;
    this.retries = 0;
    ui.dropzone.hidden = true;
    ui.panel.hidden = false;
    ui.name.textContent = file.name;
    ui.name.title = file.name;
    ui.meta.textContent = formatBytes(file.size);
    this.setState('uploading', 'Starting');
    this.setBar(0);
    ui.percent.textContent = '0%';
    ui.transferred.textContent = formatRatio(0, file.size);
    this.startedAt = performance.now();
    ui.speed.textContent = '–';
    ui.eta.textContent = '–';
    ui.parts.hidden = true;
    ui.partsMap.replaceChildren();
    this.notice(null);
    ui.cancel.hidden = false;
    ui.cancel.disabled = false;
    ui.cancel.textContent = 'Cancel upload';
    ui.again.hidden = true;
  },

  planned(up) {
    this.partCount = up.partCount;
    ui.meta.textContent =
      `${formatBytes(this.file.size)} · ${up.partCount} ${plural(up.partCount, 'part')} of ${formatBytes(up.partSize)}`;
    this.setState('uploading', 'Uploading');

    const fragment = document.createDocumentFragment();
    this.cells = Array.from({ length: up.partCount }, (_, i) => {
      const cell = document.createElement('span');
      cell.className = 'part';
      cell.dataset.state = 'pending';
      cell.title = `Part ${i + 1}`;
      fragment.append(cell);
      return cell;
    });
    ui.partsMap.append(fragment);
    ui.parts.hidden = false;
    this.summary();
  },

  part(partNumber, state) {
    const cell = this.cells[partNumber - 1];
    if (!cell) return;
    if (state === 'done' && cell.dataset.state !== 'done') this.partsDone++;
    cell.dataset.state = state;
    this.summary();
  },

  summary(retries) {
    if (retries !== undefined) this.retries = retries;
    const parts = `${this.partsDone} of ${this.partCount} stored`;
    ui.partsSummary.textContent = this.retries
      ? `${parts} · ${this.retries} ${plural(this.retries, 'retry', 'retries')}`
      : parts;
  },

  progress: (p) => {
    // Hold below 100 until S3 confirms the assembled object.
    const percent = Math.min(99, Math.floor(p.fraction * 100));
    view.setBar(percent);
    ui.percent.textContent = `${percent}%`;
    ui.transferred.textContent = formatRatio(p.loaded, p.total);
    ui.speed.textContent = p.rate > 0 ? `${formatBytes(p.rate)}/s` : '–';
    ui.eta.textContent = p.secondsLeft == null ? '–' : formatDuration(p.secondsLeft);
  },

  retrying(partNumber, err, attempt, delay, retries) {
    this.summary(retries);
    this.notice('warn',
      `Part ${partNumber}: ${err.message}. Retrying in ${(delay / 1000).toFixed(1)}s ` +
      `(attempt ${attempt + 1} of ${MAX_ATTEMPTS}).`);
  },

  finishing(partCount) {
    this.setState('finishing', 'Finishing');
    ui.cancel.disabled = true;
    ui.eta.textContent = '–';
    this.notice('info', `All parts stored. Asking S3 to assemble ${partCount} ${plural(partCount, 'part')} into one object…`);
  },

  done(key, retries) {
    this.setState('done', 'Complete');
    this.setBar(100);
    ui.percent.textContent = '100%';
    ui.transferred.textContent = formatRatio(this.file.size, this.file.size);
    const seconds = (performance.now() - this.startedAt) / 1000;
    ui.speed.textContent = `${formatBytes(this.file.size / seconds)}/s avg`;
    ui.eta.textContent = 'Done';
    const extra = retries ? ` ${retries} ${plural(retries, 'part retry', 'part retries')} along the way.` : '';
    this.notice('success', 'Stored as ', key, `.${extra}`);
    ui.cancel.hidden = true;
    ui.again.hidden = false;
    ui.again.focus();
  },

  // err is null for a user cancel. started says whether S3 had an upload to clean up.
  stopped(err, started, cleaned) {
    const cancelled = err === null;
    this.setState(cancelled ? 'cancelled' : 'failed', cancelled ? 'Cancelled' : 'Failed');
    ui.speed.textContent = '–';
    ui.eta.textContent = '–';
    let text = cancelled ? 'Upload cancelled.' : capitalize(err.message);
    if (!/[.!?]$/.test(text)) text += '.';
    if (started) {
      text += cleaned
        ? ' The parts already sent were discarded.'
        : ' The parts already sent could not be discarded; the bucket lifecycle rule removes them within 7 days.';
    }
    this.notice(cancelled ? 'info' : 'error', text);
    ui.cancel.hidden = true;
    ui.again.hidden = false;
    ui.again.focus();
  },

  setState(state, label) {
    ui.panel.dataset.state = state;
    ui.pill.dataset.state = state;
    ui.pill.textContent = label;
  },

  setBar(percent) {
    ui.barFill.style.width = `${percent}%`;
    ui.bar.setAttribute('aria-valuenow', String(percent));
  },

  // notice(tone, text, code?, after?) shows a message; `code` renders monospace.
  notice(tone, text, code, after) {
    if (!tone) {
      ui.notice.hidden = true;
      return;
    }
    ui.notice.dataset.tone = tone;
    ui.notice.replaceChildren(text);
    if (code) {
      const el = document.createElement('code');
      el.textContent = code;
      ui.notice.append(el, after ?? '');
    }
    ui.notice.hidden = false;
  },
};

// ---------- Processing ----------

// After the upload, S3 notifies the pipeline and the Lambdas take over. This
// panel polls the batch's status until every document is handled.
const processing = {
  stopPolling: null,
  waitingSince: 0,

  start(id) {
    ui.processing.hidden = false;
    ui.resultsLink.href = `batch.html?id=${encodeURIComponent(id)}`;
    this.waitingSince = Date.now();
    this.render({ state: 'waiting' });
    this.stopPolling = pollBatch(id, (b, err) => this.render(b, err));
  },

  reset() {
    this.stopPolling?.();
    this.stopPolling = null;
    ui.processing.hidden = true;
  },

  render(b, err) {
    if (err) {
      this.note(`Can't reach the server (${err.message}); still trying.`);
      return;
    }
    const d = describeBatch(b);
    ui.processing.dataset.state = d.tone;
    ui.procPill.dataset.state = d.tone;
    ui.procPill.textContent = d.pill;
    ui.procHeadline.textContent = d.headline;
    ui.procFill.style.width = `${d.percent}%`;
    ui.procBar.setAttribute('aria-valuenow', String(d.percent));

    const n = (x) => (b.state === 'waiting' ? '–' : numberFormat.format(x));
    ui.procProcessed.textContent = n(b.processed);
    ui.procPending.textContent = n(b.pending);
    ui.procSkipped.textContent = n(b.skipped);
    ui.procFailed.textContent = n(b.failed);

    if (b.state === 'waiting' && Date.now() - this.waitingSince > 60_000) {
      this.note('Still waiting after a minute. Check that the pipeline is deployed (S3 notification, SQS and the unzip Lambdas).');
    } else {
      this.note(null);
    }
  },

  note(text) {
    ui.procNote.hidden = !text;
    ui.procNote.textContent = text ?? '';
  },
};

function formatBytes(n) {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

// "1.2 / 4.8 GB", or "512.0 MB / 4.8 GB" when the units differ.
function formatRatio(done, total) {
  const [doneValue, doneUnit] = formatBytes(done).split(' ');
  const totalText = formatBytes(total);
  return totalText.endsWith(` ${doneUnit}`)
    ? `${doneValue} / ${totalText}`
    : `${doneValue} ${doneUnit} / ${totalText}`;
}

function formatDuration(seconds) {
  if (seconds < 1) return '< 1s';
  const s = Math.round(seconds);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (h) return `${h}h ${m}m`;
  if (m) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

function plural(n, one, many = `${one}s`) {
  return n === 1 ? one : many;
}

function capitalize(s) {
  return s.charAt(0).toUpperCase() + s.slice(1);
}
