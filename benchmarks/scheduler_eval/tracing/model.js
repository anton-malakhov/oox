// SPDX-License-Identifier: Apache-2.0
// Pure parser shared by the viewer and node tests. No inferred migrations.
(function (root) {
  'use strict';
  function parse(data) {
    if (data.schema !== 'oox-scheduler-trace-v1' || !Array.isArray(data.events))
      throw new Error('Unsupported trace schema');
    if (data.events.length > 4194304) throw new Error('Trace exceeds 4M events');
    const events = data.events.map((e, sequence) => {
      for (const key of ['ns', 'tid', 'id', 'parent', 'a', 'b'])
        if (!Number.isSafeInteger(e[key]) || e[key] < 0) throw new Error(`Invalid ${key}`);
      if (!Number.isInteger(e.worker) || !['B', 'E', 'i'].includes(e.ph) || typeof e.kind !== 'string')
        throw new Error('Invalid event');
      return {...e, sequence};
    }).sort((a, b) => a.ns - b.ns || a.sequence - b.sequence);
    const lanes = new Map(), spans = [], instants = [], stacks = new Map(), offers = new Map();
    const flows = [], byId = new Map(), workerLane = new Map(), splits = new Map(), ranges = new Map();
    let unmatched = 0;
    for (const e of events) {
      if (!lanes.has(e.tid)) lanes.set(e.tid, {tid: e.tid, worker: e.worker, depth: 0});
      if (e.worker >= 0) { lanes.get(e.tid).worker = e.worker; workerLane.set(e.worker, e.tid); }
      if (e.kind === 'split') splits.set(e.id, e);
      if (e.kind === 'range_spec') ranges.set(e.id, [e.a, e.b]);
      if (e.kind.startsWith('offer_') || e.kind === 'activate' || e.kind === 'activate_mask' || e.kind === 'defer')
        offers.set(e.id, [...(offers.get(e.id) || []), e]);
      if (e.ph === 'B') {
        const stack = stacks.get(e.tid) || [];
        const s = {...e, initial_range: ranges.get(e.id), start: e.ns, end: null, depth: stack.length};
        stack.push(s); stacks.set(e.tid, stack);
        lanes.get(e.tid).depth = Math.max(lanes.get(e.tid).depth, s.depth);
        spans.push(s);
        byId.set(e.id, s);
      } else if (e.ph === 'E') {
        const stack = stacks.get(e.tid) || [];
        const s = stack.at(-1);
        if (s && s.id === e.id && s.kind === e.kind) { s.end = e.ns; stack.pop(); }
        else unmatched++;
      } else {
        instants.push(e);
      }
    }
    // Match after collecting all sends: clock resolution can give a send and
    // receipt identical timestamps, even when per-buffer export reverses them.
    const consumed = new Set();
    for (const e of events) {
      const activation = e.ph === 'B' && (e.kind === 'resident' || e.kind === 'mask_task');
      if (!activation && !e.kind.startsWith('take_')) continue;
      const source = (offers.get(e.id) || []).filter(s => s.ns <= e.ns && !consumed.has(s)
        && !(s.ns === e.ns && s.tid === e.tid && s.sequence > e.sequence)
        && (activation ? s.kind === (e.kind === 'mask_task' ? 'activate_mask' : 'activate')
          : e.kind === 'take_deferred' ? s.kind === 'defer' : s.kind.startsWith('offer_'))).at(-1);
      if (source) {
        consumed.add(source);
        flows.push({kind: activation ? source.kind : source.kind === 'defer' ? 'defer' : 'queue_delivery', id:e.id,source,target:e});
      }
    }
    // Victim endpoints refer to the worker observed by the steal site, not to
    // a guessed previous executor. Range splits are observed on the thief.
    for (const e of instants) {
      if (e.kind === 'steal_queue' || e.kind === 'steal_range') {
        const tid = workerLane.get(e.a), split = splits.get(e.id);
        if (tid !== undefined && tid !== e.tid)
          flows.push({kind: e.kind, id: e.id, source: {...e, tid, worker: e.a,
            ...(e.kind === 'steal_range' && split ? {range_id: split.parent} : {})}, target: e, split});
      } else if (e.kind === 'split') {
        const parent = byId.get(e.parent), child = byId.get(e.id);
        if (parent && child)
          flows.push({kind: 'split', id: e.id, source: {...e, tid: parent.tid, worker: parent.worker,
            range_id: parent.id}, target: child, split: e});
      }
    }
    const max = events.at(-1)?.ns || 1;
    for (const s of spans) if (s.end === null) { s.end = max; s.incomplete = true; unmatched++; }
    // A specification can tie a receiving thread's begin timestamp.
    for (const s of spans) {
      s.initial_range = ranges.get(s.id);
      let owner = s;
      const seen = new Set();
      while (!['task', 'resident', 'mask_task'].includes(owner.kind) && byId.has(owner.parent)
        && byId.get(owner.parent).kind !== 'wave' && !seen.has(owner.parent)) {
        seen.add(owner.id); owner = byId.get(owner.parent);
      }
      s.execution_id = owner.id;
    }
    return {data, events, spans, flows, instants, unmatched,
      lanes: [...lanes.values()].sort((a, b) => (a.worker < 0 ? 999 : a.worker) - (b.worker < 0 ? 999 : b.worker) || a.tid - b.tid),
      waves: spans.filter(s => s.kind === 'wave').sort((a, b) => a.a - b.a), max};
  }
  function rangeProjection(record, callbacks = false) {
    const chunks = record.spans.filter(s => s.kind === 'chunk' && s.a < s.b);
    const calls = record.spans.filter(s => s.kind === 'callback' && s.a < s.b);
    const fixed = ['RAPID_START', 'RAPID_GROUP'].includes(record.data.mode);
    const blocks = callbacks && calls.length ? calls : chunks.length ? chunks : calls.length ? calls
      : record.spans.filter(s => s.kind === 'range' && s.a < s.b);
    const initialOnly = !chunks.length && !calls.length && !fixed;
    const specifications = new Map(), executions = new Map();
    for (const e of record.events)
      if (e.kind === 'range_spec' || e.kind === 'split') specifications.set(e.id, [e.a, e.b]);
    for (const s of record.spans) {
      executions.set(s.id, s.execution_id);
      if (s.kind === 'range') specifications.set(s.id, [s.a, s.b]);
    }
    // Command IDs name an envelope; resolve the recorded range descendant.
    for (const s of record.spans)
      if (s.kind === 'range' && !specifications.has(s.execution_id)) specifications.set(s.execution_id, [s.a, s.b]);
    const flows = record.flows.flatMap(f => {
      const range = f.split ? [f.split.a, f.split.b] : specifications.get(f.id);
      return range && range[0] < range[1] ? [{...f, range, execution_id: executions.get(f.id) ?? f.id}] : [];
    });
    return {blocks, flows, initialOnly};
  }
  function clipInterval(start, end, left, right) {
    return left >= right || end < left || start >= right ? null : [Math.max(start, left), Math.min(end, right)];
  }
  const api = {parse, rangeProjection, clipInterval};
  if (typeof module !== 'undefined') module.exports = api;
  root.TraceModel = api;
})(typeof globalThis === 'undefined' ? window : globalThis);
