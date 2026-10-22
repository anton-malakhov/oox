// SPDX-License-Identifier: Apache-2.0
'use strict';
// Teaching execution of the explicit-policy path. Range/budget transitions and
// the owner-private buffer follow the local C++ policies. OS timing, mailbox
// proxy lifetime, cancellation and cross-call affinity persistence are excluded.
(() => {
  const kinds = ['auto', 'simple', 'static', 'affinity'];
  const adaptive = s => s.kind === 'auto' || s.kind === 'affinity';
  function state(kind, workers) {
    return {kind, divisor: workers * (kind === 'auto' ? 2 : kind === 'affinity' ? 16 : 1),
      depth: 5, head: 0, slots: workers * 16, delay: true,
      history: kind === 'affinity' ? Array(workers * 16).fill(null) : null, workers};
  }
  function divisible(s) {
    if (s.kind === 'simple') return true;
    if (s.kind === 'static') return s.divisor > 1;
    if (s.kind === 'affinity') return s.divisor > 16;
    if (s.divisor > 1) return true;
    if (s.divisor && s.depth) { s.divisor = 0; --s.depth; return true; }
    return false;
  }
  function hint(s) {
    if (s.kind === 'static') return s.head;
    if (s.kind === 'affinity' && s.divisor) return s.history[s.head] ?? Math.floor(s.head / 16);
    return null;
  }
  function splitState(s, proportional) {
    const child = {...s};
    if (s.kind === 'simple') return child;
    if (proportional) {
      const factor = s.kind === 'affinity' ? 16 : 1;
      child.divisor = Math.floor(s.divisor / factor / 2) * factor;
      s.divisor -= child.divisor;
    } else child.divisor = s.divisor = Math.floor(s.divisor / 2);
    if (s.kind === 'static' || s.kind === 'affinity') {
      child.head = (s.head + s.divisor) % (s.kind === 'static' ? s.workers : s.slots);
      child.delay = proportional;
    }
    return child;
  }
  function splitRange(range, s, proportional) {
    const length = range.b - range.a;
    let right = length - Math.floor(length / 2);
    if (proportional) {
      const count = s.divisor / (s.kind === 'affinity' ? 16 : 1), weight = Math.floor(count / 2);
      right = Math.floor(length / count) * weight + Math.floor(((length % count) * weight + count / 2) / count);
    }
    const child = {a: range.b - right, b: range.b, state: splitState(s, proportional)};
    range.b = child.a;
    return child;
  }
  function prefix(kind, begin, end, workers, target, grain) {
    const seeds = [{a: begin, b: end, state: state(kind, workers)}];
    while (seeds.length < target) {
      let selected = -1, largest = 0;
      for (let i = 0; i < seeds.length; ++i) {
        const seed = seeds[i], size = seed.b - seed.a;
        if (size > grain && size > largest && divisible({...seed.state})) { selected = i; largest = size; }
      }
      if (selected < 0) break;
      const seed = seeds[selected]; divisible(seed.state);
      seeds.push(splitRange(seed, seed.state, kind === 'static' || kind === 'affinity'));
    }
    return seeds;
  }
  function place(seeds, free) {
    const result = Array(free.length).fill(null), used = new Set();
    free.forEach((worker, slot) => {
      const index = seeds.findIndex((seed, i) => !used.has(i) && hint(seed.state) === worker);
      if (index >= 0) { result[slot] = seeds[index]; used.add(index); }
    });
    const rest = seeds.filter((_, i) => !used.has(i));
    return result.map(seed => seed || rest.shift() || null);
  }
  function demand(s, peer) {
    if (s.kind === 'affinity') {
      if (s.delay) { s.delay = false; return false; }
      if (s.divisor > 1) return true;
      if (s.divisor && s.depth) { s.divisor = 0; return true; }
    }
    if (peer) s.depth = Math.min(64, s.depth + 1);
    return peer;
  }
  function fill(buffer, depth, grain) {
    let splits = 0;
    while (buffer.length < 8) {
      const back = buffer[buffer.length - 1];
      if (!back || back.b - back.a <= grain || back.depth >= depth) break;
      const left = {a: back.a, b: back.a + Math.floor((back.b - back.a) / 2), depth: back.depth + 1};
      back.a = left.b; ++back.depth; buffer.push(left); ++splits;
    }
    return splits;
  }
  function run(mode, pattern, n, workers, grain, scenario, costsFor, options = {}) {
    const kind = mode.replace('native-', '');
    if (!kinds.includes(kind) || !Number.isSafeInteger(n) || n < 0 || n > 1048576 ||
        !Number.isInteger(workers) || workers < 1 || workers > 32 ||
        !Number.isSafeInteger(grain) || grain < 0) throw Error('Invalid native teaching-model input');
    grain = Math.max(1, grain);
    const jobs = (scenario === 'stream' ? [{pattern, at: 0}, {pattern: 'balanced', at: 8},
      {pattern: 'cluster', at: 24}, {pattern: 'single', at: 40}] : [{pattern, at: 0}])
      .map((j, id) => ({...j, id, begin: id * n, end: (id + 1) * n, visited: 0}));
    const total = n * jobs.length, costs = jobs.flatMap(j => costsFor(j.pattern, n));
    const done = Array(total).fill(-1), executions = Array(total).fill(0);
    const active = Array(workers).fill(null), queues = Array.from({length: workers}, () => []);
    const arrived = new Set(), launched = new Set(), pending = [], frames = [], events = {};
    let serial = 0, created = 0, privateSplits = 0, turn = 0, completed = 0;
    const task = (seed, worker, job, ordinary = false) => {
      if (ordinary) ++created;
      return {id: ++serial, cursor: seed.a, end: seed.b, reserved: seed.a, remaining: 0,
        state: seed.state, worker, publisher: worker, job, branch: null, finished: false,
        stage: 'initial', buffer: null, hint: hint(seed.state)};
    };
    const view = t => t && ({id: t.id, cursor: t.cursor, end: t.end, reserved: t.reserved,
      remaining: t.remaining, worker: t.worker, budget: t.state.divisor, depth: t.state.depth,
      privateChunks: t.buffer?.map(r => ({a: r.a, b: r.b})) || []});
    function snapshot(text, type = 'work', move = null) {
      events[type] = (events[type] || 0) + 1;
      if (options.captureFrames === false) return;
      frames.push({text, type, move, created, privateSplits, tick: Math.floor(turn / workers),
        jobs: jobs.map(j => ({...j, status: !arrived.has(j.id) ? 'future' : !launched.has(j.id) ? 'waiting' : j.visited === n ? 'done' : 'running'})),
        done: done.slice(), active: active.map(view), queues: queues.map(q => q.map(view))});
    }
    function release(t) {
      if (t.finished) return;
      t.finished = true;
      for (let node = t.branch; node; node = node.parent) {
        if (--node.pending < 0) throw Error('Negative teaching join count');
        if (node.pending) break;
      }
    }
    function start(t, worker, seed = false) {
      t.worker = worker;
      const s = t.state;
      if (s.kind === 'affinity' && s.divisor && t.hint !== null && t.hint !== worker) s.history[s.head] = worker;
      const check = s.kind === 'auto' ? s.divisor === 0 : s.kind === 'affinity' && s.divisor < 16;
      if (check && !seed) {
        s.divisor = 1;
        if (worker !== t.publisher && t.branch?.pending > 1) {
          s.depth = Math.min(64, Math.max(1, s.depth) + 1); t.branch.stolen = true;
        }
      }
    }
    function publish(t, seed, w, reason) {
      const child = task(seed, w, t.job, true);
      if (adaptive(t.state)) child.branch = t.branch = {pending: 2, stolen: false, parent: t.branch};
      const target = child.hint ?? w;
      queues[target].push(child);
      snapshot(`П${w} — владелец: ${reason}; публикует обычную задачу [${seed.a}, ${seed.b}) в очередь П${target}.`,
        'publish', {from: w, to: target, a: seed.a, b: seed.b});
    }
    function arrivals() {
      for (const job of jobs) if (!arrived.has(job.id) && job.at <= Math.floor(turn / workers)) {
        arrived.add(job.id); pending.push(job);
        if (scenario === 'stream') snapshot(`Пришло J${job.id + 1} [${job.begin}, ${job.end}).`, 'arrival');
      }
      while (pending.length) {
        const free = active.map((t, w) => ({t, w})).filter(({t, w}) => (!t || t.finished) && !queues[w].length).map(({w}) => w);
        if (!free.length) break;
        const job = pending.shift(), count = Math.min(free.length, Math.max(1, Math.ceil(n / grain)));
        const owners = free.slice(0, count), seeds = prefix(kind, job.begin, job.end, workers, count, grain);
        const assigned = place(seeds, owners);
        assigned.forEach((seed, slot) => {
          if (seed) { const t = task(seed, owners[slot], job); start(t, owners[slot], true); active[owners[slot]] = t; }
        });
        launched.add(job.id);
        snapshot(`Rapid + Native ${kind}: одно исходное состояние, ${seeds.length} стартовых частей. ` +
          assigned.map((s, i) => s ? `П${owners[i]} [${s.a}, ${s.b})` +
            (kind==='simple'?'':`, ${kind==='affinity'?'слоты affinity':'бюджет'} ${s.state.divisor}`) : '').filter(Boolean).join('; ') +
          '. Стартовые части ещё не являются обычными задачами очереди.', 'activate');
      }
    }
    snapshot(`Диапазон [0, ${total}) ещё не запущен.`, 'initial');
    arrivals();
    const guardLimit = Math.max(100000, costs.reduce((a, b) => a + b, 0) * workers * 4 + total * 64);
    for (let guard = 0; guard < guardLimit; ++guard) {
      if (completed === total) {
        snapshot('Все итерации завершены. Модель не воспроизводит runtime lifetime и стоимость join.', 'finish');
        return {frames, costs, executions, n: total, workers, jobs, scenario, events, created, privateSplits};
      }
      arrivals();
      const w = turn++ % workers;
      if (active[w]?.finished) active[w] = null;
      let t = active[w];
      if (!t) {
        let victim = w;
        if (queues[w].length) t = queues[w].pop();
        else { victim = queues.findIndex((q, j) => j !== w && q.length); if (victim >= 0) t = queues[victim].shift(); }
        if (!t) continue; // Native never steals directly from another active remainder.
        start(t, w); active[w] = t;
        snapshot(`П${w} ${victim === w ? 'берёт свою' : 'крадёт уже опубликованную'} задачу [${t.cursor}, ${t.end}) из очереди П${victim}.`,
          victim === w ? 'take' : 'steal', victim === w ? null : {from: victim, to: w, a: t.cursor, b: t.end});
        continue;
      }
      const s = t.state;
      if (t.cursor === t.end) { release(t); continue; }
      if (t.stage === 'initial') {
        if (t.end - t.cursor > grain && divisible(s)) {
          const range = {a: t.cursor, b: t.end};
          const child = splitRange(range, s, kind === 'static' || kind === 'affinity');
          t.end = range.b; publish(t, child, w, 'продолжает начальное дробление политики'); continue;
        }
        t.stage = adaptive(s) && t.end - t.cursor > grain && s.depth ? 'buffer' : 'compute';
        if (t.stage === 'buffer') t.buffer = [{a: t.cursor, b: t.end, depth: 0}];
      }
      if (!t.remaining && t.cursor >= t.reserved) {
        if (t.buffer) {
          const splits = fill(t.buffer, s.depth, grain);
          if (splits) {
            privateSplits += splits;
            snapshot(`П${w} делит свой приватный буфер: ${t.buffer.map(r => `[${r.a}, ${r.b})`).join(', ')}. Другие потоки не могут брать эти части напрямую.`, 'private-split');
            continue;
          }
          const idle = active.some((a, j) => j !== w && (!a || a.finished) && !queues[j].length);
          const peerDemand = !!t.branch && (t.branch.stolen || (idle && t.branch.pending === 1));
          if (demand(s, peerDemand)) {
            if (t.buffer.length > 1) {
              const range = t.buffer.shift(), childState = splitState(s, false);
              childState.depth -= range.depth;
              if (childState.depth < 0) throw Error('Negative relative range depth');
              t.end = range.a;
              publish(t, {a: range.a, b: range.b, state: childState}, w, 'отдаёт часть приватного буфера по политике/спросу'); continue;
            }
            const back = t.buffer[0];
            if (back.b - back.a > grain && back.depth < s.depth) continue;
          }
          t.reserved = t.buffer[t.buffer.length - 1].b;
        } else t.reserved = t.end;
        snapshot(`П${w} начинает локальный блок [${t.cursor}, ${t.reserved}). Разделить его работающий callback нельзя.`, 'reserve');
        continue;
      }
      if (!t.remaining) t.remaining = costs[t.cursor];
      if (--t.remaining) {
        snapshot(`П${w} выполняет body(${t.cursor}); до границы локального блока владелец не публикует новые части.`);
      } else {
        const i = t.cursor++;
        if (done[i] !== -1) throw Error(`Duplicate native iteration ${i}`);
        done[i] = w; ++executions[i]; ++completed; ++t.job.visited;
        if (t.buffer && t.cursor === t.reserved) t.buffer.pop();
        if (t.cursor === t.end) release(t);
        snapshot(`П${w} завершил итерацию ${i}.`);
      }
    }
    throw Error(`Native teaching model did not terminate: ${mode}/${pattern}/${n}/${workers}/${grain}`);
  }
  const api = {run, prefix};
  if (typeof module !== 'undefined') module.exports = api;
  else globalThis.SchedulerNativeModel = api;
})();
