const assert = require('node:assert/strict');
const {buildRun} = require('./model.js');
const {prefix, run} = require('./native-model.js');

// Hand-computed examples, not a second implementation of prefix splitting.
assert.deepEqual(prefix('static', 0, 100, 3, 3, 1).map(s => [s.a, s.b, s.state.divisor, s.state.head]),
  [[0, 33, 1, 0], [67, 100, 1, 2], [33, 67, 1, 1]]);
assert.deepEqual(prefix('auto', 0, 100, 3, 3, 1).map(s => [s.a, s.b, s.state.divisor]),
  [[0, 25, 1], [50, 100, 3], [25, 50, 1]]);
const lazy = buildRun('lazy', 'tail', 64, 4, 2);
assert.equal(lazy.frames.find(f => f.created > 0).type, 'split-steal');
const native = buildRun('native-auto', 'tail', 64, 4, 2);
assert.equal(native.frames.find(f => f.created > 0).type, 'publish');
assert(native.frames.some(f => f.type === 'private-split'));
assert(!native.frames.some(f => f.type === 'split-steal'));
assert(native.frames.find(f => f.type === 'publish').active.every(Boolean), 'owner can publish before any worker is idle');
const statical = buildRun('native-static', 'tail', 64, 4, 2);
assert.equal(statical.created, 0);
assert.deepEqual(statical.frames.at(-1).done, Array.from({length: 64}, (_, i) => Math.floor(i / 16)));
assert(buildRun('native-simple', 'balanced', 64, 4, 2).created > 0);

// Dense small inputs and large deterministic generated costs. The oracle is a
// flat serial visit vector; no budget, queue or completion-tree logic is shared.
const seed = 0x4e415449;
let random = seed, cases = 0;
function next() { random = (Math.imul(random, 1664525) + 1013904223) >>> 0; return random; }
for (const policy of ['auto', 'simple', 'static', 'affinity']) {
  for (const workers of [1, 2, 3, 5, 8]) for (const grain of [0, 1, 3, 17]) for (let n = 0; n <= 33; ++n) {
    const context = `seed=${seed} case=${cases++} policy=${policy} workers=${workers} grain=${grain} n=${n}`;
    const result = buildRun('native-' + policy, ['balanced', 'tail', 'cluster', 'single'][next() % 4],
      n, workers, grain, cases % 2 ? 'single' : 'stream', {captureFrames: false});
    assert.deepEqual(result.executions, Array(result.n).fill(1), context);
    assert.equal(result.events['split-steal'] || 0, 0, context);
  }
  for (const n of [65537, 262147]) {
    const costs = Array.from({length: n}, () => 1 + (next() % 7));
    const expected = Array(n).fill(1);
    const result = run('native-' + policy, 'generated', n, 8, 17, 'single', () => costs, {captureFrames: false});
    assert.deepEqual(result.executions, expected, `seed=${seed} case=${cases++} policy=${policy} n=${n}`);
  }
}
console.log(`PASS: Native prefix, producer/consumer distinction, private buffer, ${cases} independent serial-oracle cases (seed=${seed})`);
