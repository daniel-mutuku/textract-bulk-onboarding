// Results page: batch.html?id=<batchId>. Shows the batch's progress (polled)
// and its documents with the fields Textract extracted.

'use strict';

const PAGE_SIZE = 50;
const LOW_CONFIDENCE = 80;

// Labels in form order. Keys are the Textract query aliases from
// internal/extract/fields.go.
const FIELDS = [
  ['member_number', 'M No'],
  ['full_name', 'Name'],
  ['date_of_birth', 'Date of birth'],
  ['id_passport_number', 'ID / Passport No'],
  ['kra_pin', 'PIN No'],
  ['mobile_number', 'Mobile'],
  ['email', 'Email'],
  ['postal_address', 'Postal address'],
  ['town', 'Town'],
  ['occupation', 'Occupation'],
  ['account_name', 'Account name'],
  ['account_number', 'Account No'],
  ['bank_name', 'Bank'],
  ['bank_branch', 'Branch'],
  ['total_amount_invested', 'Total invested'],
];

const $ = (id) => document.getElementById(id);
const batchID = new URLSearchParams(location.search).get('id');

// Documents loaded so far, by seq, and the cursor for the next page.
const docs = new Map();
let next = 0; // 0 = start; null = no more pages
let filter = 'all';
let lastCounts = '';

if (!batchID) {
  $('name').textContent = 'No batch selected';
  $('headline').textContent = 'Open this page from the upload page, or add ?id=<batch id> to the URL.';
} else {
  document.title = `Batch ${batchID.slice(0, 8)}`;
  pollBatch(batchID, renderBatch);
  loadMore();
}

$('more').addEventListener('click', loadMore);
for (const tab of document.querySelectorAll('.tab')) {
  tab.addEventListener('click', () => {
    filter = tab.dataset.filter;
    for (const t of document.querySelectorAll('.tab')) t.setAttribute('aria-selected', String(t === tab));
    renderDocs();
  });
}

// ---------- Batch summary ----------

function renderBatch(b, err) {
  if (err) {
    showNote(`Can't reach the server (${err.message}); still trying.`);
    return;
  }
  showNote(null);
  const d = describeBatch(b);
  $('pill').dataset.state = d.tone;
  $('pill').textContent = d.pill;
  $('headline').textContent = d.headline;
  $('bar-fill').style.width = `${d.percent}%`;
  $('bar').setAttribute('aria-valuenow', String(d.percent));
  document.querySelector('main.card').dataset.state = d.tone;

  if (b.state === 'waiting') {
    $('name').textContent = 'Waiting for the batch…';
    return;
  }
  $('name').textContent = b.originalFilename || 'upload.zip';
  $('meta').textContent = `Batch ${b.id} · started ${new Date(b.createdAt).toLocaleString()}`;
  const n = (x) => numberFormat.format(x);
  $('s-total').textContent = n(b.total);
  $('s-processed').textContent = n(b.processed);
  $('s-pending').textContent = n(b.pending);
  $('s-skipped').textContent = n(b.skipped);
  $('s-failed').textContent = n(b.failed);

  // When the counts move, refresh the documents already on screen so their
  // statuses and fields catch up, without losing the user's place.
  const counts = `${b.processed}/${b.failed}/${b.skipped}/${b.chunks.done}`;
  if (counts !== lastCounts) {
    lastCounts = counts;
    refreshLoaded();
  }
}

function showNote(text) {
  $('note').hidden = !text;
  $('note').textContent = text ?? '';
}

// ---------- Documents ----------

async function fetchPage(after) {
  const res = await fetch(`/api/batches/${encodeURIComponent(batchID)}/documents?after=${after}&limit=${PAGE_SIZE}`, { cache: 'no-store' });
  if (res.status === 404 || res.status === 400) return { documents: [], next: null };
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function loadMore() {
  if (next === null) return;
  $('more').disabled = true;
  try {
    const page = await fetchPage(next);
    for (const d of page.documents) docs.set(d.seq, d);
    next = page.next;
    renderDocs();
  } catch (err) {
    showNote(`Could not load documents: ${err.message}`);
  } finally {
    $('more').disabled = false;
  }
}

// Re-fetch from the start up to as many documents as are already shown (at
// least one page), so statuses and fields catch up without losing the
// user's place. Pages are keyed by seq, so the same request returns the same
// documents, just in their newer state.
async function refreshLoaded() {
  const want = Math.max(docs.size, PAGE_SIZE);
  let after = 0;
  let loaded = 0;
  try {
    for (;;) {
      const page = await fetchPage(after);
      for (const d of page.documents) docs.set(d.seq, d);
      loaded += page.documents.length;
      next = page.next;
      if (next === null || loaded >= want) break;
      after = next;
    }
    renderDocs();
  } catch {
    // The next poll tries again.
  }
}

function needsReview(d) {
  return d.status === 'extracted' && FIELDS.some(([key]) => !d.fields?.[key] || d.fields[key].confidence < LOW_CONFIDENCE);
}

// The documents currently listed, in order: what the viewer's prev/next walk.
let shownSeqs = [];

function renderDocs() {
  const all = [...docs.values()].sort((a, b) => a.seq - b.seq);
  const counts = {
    all: all.length,
    review: all.filter(needsReview).length,
    failed: all.filter((d) => d.status === 'failed').length,
    skipped: all.filter((d) => d.status === 'skipped').length,
  };
  for (const [k, v] of Object.entries(counts)) $(`c-${k}`).textContent = v ? numberFormat.format(v) : '';

  const shown = all.filter((d) =>
    filter === 'all' ? true : filter === 'review' ? needsReview(d) : d.status === filter);
  shownSeqs = shown.map((d) => d.seq);
  $('docs').replaceChildren(...shown.map(docRow));

  $('empty').hidden = shown.length > 0;
  $('empty').textContent = all.length === 0 ? 'No documents yet. They appear here as the zip is unpacked.' : 'Nothing in this filter among the documents loaded.';
  $('more').hidden = next === null;

  // A poll may have updated the document being viewed.
  if (viewer.open) viewer.refresh();
}

function docRow(d) {
  const row = el('button', { class: 'doc', type: 'button', 'data-seq': d.seq, 'data-status': d.status },
    el('span', { class: 'doc-seq' }, `#${d.seq}`),
    el('span', { class: 'doc-title' },
      el('span', { class: 'doc-name' }, docName(d)),
      // For a skipped or failed document, the reason is the useful subtitle.
      el('span', { class: 'doc-file' }, d.status === 'skipped' || d.status === 'failed' ? d.reason || d.filename : d.filename)),
    el('span', { class: 'doc-conf' }, lowestConfidence(d) !== null ? confBadge(lowestConfidence(d), 'lowest') : ''),
    statusPill(d.status));
  row.addEventListener('click', () => viewer.show(d.seq));
  return row;
}

function docName(d) {
  return d.fields?.full_name?.value || basename(d.filename);
}

function lowestConfidence(d) {
  if (d.status !== 'extracted' || !d.fields) return null;
  return Math.min(...FIELDS.map(([key]) => d.fields[key]?.confidence ?? 0));
}

function statusPill(status) {
  return el('span', { class: 'pill pill-sm', 'data-state': pillState(status) }, statusLabel(status));
}

// ---------- Document viewer ----------

// A modal with the scan on the left and the extracted fields on the right.
// It is a native <dialog> opened with showModal(), which gives a backdrop,
// focus trapping, Esc to close, and focus returned to the row on close.
const viewer = {
  dialog: $('viewer'),
  seq: null,

  get open() {
    return this.dialog.open;
  },

  show(seq) {
    this.seq = seq;
    this.render(true);
    if (!this.open) this.dialog.showModal();
  },

  step(delta) {
    const i = shownSeqs.indexOf(this.seq) + delta;
    if (i >= 0 && i < shownSeqs.length) this.show(shownSeqs[i]);
  },

  // Re-render the fields after a poll, leaving the image alone (no flicker).
  refresh() {
    if (docs.has(this.seq)) this.render(false);
  },

  render(withMedia) {
    const d = docs.get(this.seq);
    const i = shownSeqs.indexOf(this.seq);
    $('v-name').textContent = docName(d);
    $('v-file').textContent = `#${d.seq} · ${d.filename}`;
    $('v-pos').textContent = i >= 0 ? `${i + 1} of ${shownSeqs.length}` : '';
    $('v-prev').disabled = i <= 0;
    $('v-next').disabled = i < 0 || i >= shownSeqs.length - 1;

    const lowest = lowestConfidence(d);
    const meta = [statusPill(d.status)];
    if (lowest !== null) meta.push(confBadge(lowest, 'lowest'));
    if (d.key) meta.push(el('a', { class: 'viewer-link', href: fileURL(d), target: '_blank', rel: 'noopener' }, 'Open original'));
    $('v-meta').replaceChildren(...meta);

    if (d.status === 'extracted') {
      $('v-body').replaceChildren(fieldList(d));
    } else {
      $('v-body').replaceChildren(el('p', { class: 'viewer-reason' },
        d.reason || (d.status === 'pending' ? 'Waiting for extraction. This updates by itself.' : '')));
    }
    if (withMedia) this.renderMedia(d);
  },

  renderMedia(d) {
    const box = $('v-media');
    box.dataset.zoom = 'fit';
    box.scrollTo(0, 0);
    if (!d.key) {
      box.replaceChildren(el('p', { class: 'viewer-empty' }, `No file is stored for this entry: it was ${d.status}.`));
      return;
    }
    if (d.contentType === 'application/pdf') {
      box.replaceChildren(el('iframe', { class: 'viewer-pdf', src: fileURL(d), title: d.filename }));
      return;
    }
    if (d.contentType === 'image/tiff') {
      // Most browsers can't render TIFF in an <img>.
      box.replaceChildren(el('p', { class: 'viewer-empty' }, 'This browser can’t preview TIFF images. ',
        el('a', { href: fileURL(d), target: '_blank', rel: 'noopener' }, 'Download it'), '.'));
      return;
    }
    const img = el('img', { src: fileURL(d), alt: `Scan of ${d.filename}` });
    box.dataset.loading = 'true';
    img.addEventListener('load', () => delete box.dataset.loading);
    img.addEventListener('error', async () => {
      delete box.dataset.loading;
      const msg = el('p', { class: 'viewer-empty' }, 'The image could not be loaded.');
      box.replaceChildren(msg);
      msg.textContent = await fileProblem(d);
    });
    // Click to switch between fitting the panel and full size.
    img.addEventListener('click', () => {
      box.dataset.zoom = box.dataset.zoom === 'fit' ? 'full' : 'fit';
    });
    box.replaceChildren(img);
  },
};

function fieldList(d) {
  const list = el('dl', { class: 'fields fields-1' });
  for (const [key, label] of FIELDS) {
    const f = d.fields?.[key];
    list.append(el('div', { class: `field${!f || f.confidence < LOW_CONFIDENCE ? ' field-low' : ''}` },
      el('dt', {}, label),
      el('dd', {},
        f ? el('span', { class: 'field-value' }, f.value) : el('span', { class: 'field-missing' }, 'not found'),
        f ? confBadge(f.confidence) : '')));
  }
  return list;
}

// An <img> that fails to load gets no status code, so ask the endpoint again
// without following the redirect and explain what it says.
async function fileProblem(d) {
  try {
    const res = await fetch(fileURL(d), { redirect: 'manual', cache: 'no-store' });
    if (res.type === 'opaqueredirect') {
      return 'The server signed a link, but S3 refused it. Check the server’s AWS credentials can read the documents bucket.';
    }
    const body = await res.text();
    try {
      return `The image could not be loaded: ${JSON.parse(body).error}.`;
    } catch {
      // Not our JSON error: the route doesn't exist, so the server predates this endpoint.
      return 'The image could not be loaded: the server doesn’t have the document file endpoint. Restart it (go run ./cmd/server) to pick up the latest code.';
    }
  } catch {
    return 'The image could not be loaded: the server is unreachable.';
  }
}

function fileURL(d) {
  // The server answers with a redirect to a short-lived signed S3 URL.
  return `/api/batches/${encodeURIComponent(batchID)}/documents/${d.seq}/file`;
}

$('v-close').addEventListener('click', () => viewer.dialog.close());
$('v-prev').addEventListener('click', () => viewer.step(-1));
$('v-next').addEventListener('click', () => viewer.step(1));
viewer.dialog.addEventListener('keydown', (e) => {
  if (e.key === 'ArrowLeft') viewer.step(-1);
  if (e.key === 'ArrowRight') viewer.step(1);
});
// A click on the backdrop lands on the <dialog> element itself.
viewer.dialog.addEventListener('click', (e) => {
  if (e.target === viewer.dialog) viewer.dialog.close();
});

function confBadge(confidence, prefix) {
  const low = confidence < LOW_CONFIDENCE;
  return el('span', { class: `conf${low ? ' conf-low' : ''}`, title: 'Textract confidence' },
    `${prefix ? prefix + ' ' : ''}${Math.round(confidence)}%`);
}

function pillState(status) {
  return { extracted: 'done', failed: 'failed', skipped: 'cancelled', pending: 'uploading' }[status] ?? 'uploading';
}

function statusLabel(status) {
  return { extracted: 'Extracted', failed: 'Failed', skipped: 'Skipped', pending: 'Pending' }[status] ?? status;
}

function basename(path) {
  return path.split('/').pop();
}

// el builds an element. Text goes in as text nodes, never as HTML, so
// values from the documents can't inject markup.
function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  node.append(...children.filter((c) => c !== ''));
  return node;
}
