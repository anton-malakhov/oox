const fs = require('fs');
const vm = require('vm');
const assert = require('assert/strict');
const html = fs.readFileSync(__dirname + '/index.html', 'utf8');
const script = fs.readFileSync(__dirname + '/app.js', 'utf8');
new vm.Script(script);
const model = require('./model.js');
let runs = 0, frames = 0;
for (const mode of model.definitions.map(d => d.id)) {
  for (const pattern of ['balanced', 'tail', 'cluster', 'single']) {
    for (const n of [4, 5, 9, 17, 32, 64, 127]) {
      for (const grain of [1, 2, 4, 8]) {
        const run = model.buildRun(mode, pattern, n, 4, grain);
        const reference = Array(n).fill(0);
        for (let i = 0; i < n; ++i) reference[i]++;
        assert.deepEqual(Array.from(run.executions), reference, `${mode}/${pattern}/${n}/${grain}`);
        for (const f of run.frames) {
          if (f.type === 'initial' || (mode === 'sharing' && f.type === 'publish')) continue;
          const coverage = Array.from(f.done, w => w >= 0 ? 1 : 0);
          for (const t of [...f.active.filter(Boolean), ...f.queues.flat()]) {
            assert(t.cursor <= t.end && t.reserved <= t.end);
            for (let i = t.cursor; i < t.end; ++i) coverage[i]++;
          }
          assert(coverage.every(x => x === 1), `${mode}/${pattern}/${n}/${grain}: ${f.text}`);
          frames++;
        }
        runs++;
      }
    }
  }
}
// Minimal DOM double: tests UI event wiring, not browser rendering or layout.
let streamRuns=0, concurrentLazy=false;
for(const mode of model.definitions.map(d=>d.id))for(const pattern of ['balanced','tail','cluster','single'])for(const workers of [4,8])for(const n of [16,64,127])for(const grain of [1,2,4]) {
  const run=model.buildRun(mode,pattern,n,workers,grain,'stream');
  assert.deepEqual(Array.from(run.executions),Array(n*4).fill(1));
  for(const f of run.frames) {
    const coverage=Array.from(f.done,w=>w>=0?1:0);
    for(const job of f.jobs) {
      if(job.status==='future'||job.status==='waiting')for(let i=job.begin;i<job.end;i++)coverage[i]++;
      if(f.tick<job.at)assert.equal(job.status,'future');
    }
    for(const t of [...f.active.filter(Boolean),...f.queues.flat()]) {
      assert(t.reserved<=t.end);
      if(t.cursor<t.end)assert.equal(Math.floor(t.cursor/n),Math.floor((t.end-1)/n),'cross-job task');
      for(let i=t.cursor;i<t.end;i++)coverage[i]++;
    }
    assert(coverage.every(x=>x===1),`${mode}/${pattern}/${workers}/${n}/${grain}: ${f.text}`);
    const running=f.jobs.filter(j=>j.status==='running').length;
    if(mode==='mask')assert(running<=1);
    if(mode==='lazy'&&running>1)concurrentLazy=true;
  }
  assert.equal(run.frames.filter(f=>f.type==='arrival').length,4);
  streamRuns++;
}
assert(concurrentLazy,'Stream must actually overlap jobs, not concatenate independent runs');
class Node {
  constructor(tag = '') { this.tag = tag; this.children = []; this.style = {setProperty(k,v){this[k]=v;}}; this.attrs = {}; this.events = {}; this.value = ''; }
  setAttribute(k, v) { this.attrs[k] = v; }
  append(...nodes) { for (const node of nodes) { this.children.push(node); if (typeof node === 'object') node.parent = this; } }
  replaceChildren(...nodes) { this.children = []; this.append(...nodes); }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter(x => x !== this); }
  addEventListener(name, fn) { this.events[name] = fn; }
  getBoundingClientRect() { return {width: this.tag === 'section' ? 280 : 1024}; }
}
const ids = Object.fromEntries([...html.matchAll(/id="([^"]+)"/g)].map(m => [m[1], new Node()]));
ids['sp-pattern'].value = 'tail'; ids['sp-speed'].value = '4';
ids['sp-workers'].value='4';ids['sp-scenario'].value='single';
const root = ids['scheduler-step-player'];
root.querySelector = query => ids[query.slice(1)];
root.querySelectorAll = () => ids['sp-choices'].children.flatMap(label => label.children.filter(n => n.tag === 'input'));
const listeners = {}, timers = new Map(); let timerId = 0;
const context = {
  URLSearchParams,
  SchedulerModel: model,
  localStorage: {getItem: () => null, setItem: (_, value) => {context.saved = JSON.parse(value);}},
  document: {getElementById: id => ids[id], createElement: tag => new Node(tag), createElementNS: (_, tag) => new Node(tag), createTextNode: value => ({value}), addEventListener: (name, fn) => listeners[name] = fn},
  window: {matchMedia: () => ({matches: true}), addEventListener: (name, fn) => listeners[name] = fn},
  ResizeObserver: class {observe() {}},
  setInterval: fn => {timers.set(++timerId, fn); return timerId;}, clearInterval: id => timers.delete(id)
};
vm.createContext(context); vm.runInContext(script, context);
assert.equal(ids['sp-panels'].children.length, 2);
const click = id => ids[id].events.click();
click('sp-next'); assert(ids['sp-position'].textContent.startsWith('1 /'));
click('sp-back'); assert(ids['sp-position'].textContent.startsWith('0 /'));
click('sp-event'); assert(ids['sp-position'].textContent.startsWith('1 /'));
click('sp-play'); assert.equal(timers.size, 1);
assert.equal(context.saved.modelContent.frame, 1);
for (let i = 0; i < 5; ++i) [...timers.values()].forEach(fn => fn());
click('sp-play'); assert.equal(timers.size, 0);
for (const box of root.querySelectorAll()) {box.checked = true; box.events.change();}
assert.equal(ids['sp-panels'].children.length, model.definitions.length);
click('sp-compare-native');
assert.deepEqual(Array.from(context.saved.modelContent.schedulers), ['lazy', 'native-auto']);
assert.equal(ids['sp-panels'].children.length, 2);
ids['sp-pattern'].value = 'single'; ids['sp-pattern'].events.change();
assert(ids['sp-cost'].textContent.includes('32'));
ids['sp-seek'].value = ids['sp-seek'].max; ids['sp-seek'].events.input();
assert(ids['sp-next'].disabled);
click('sp-restart'); assert(ids['sp-position'].textContent.startsWith('0 /'));
ids['sp-workers'].value='8';ids['sp-workers'].events.change();
ids['sp-scenario'].value='stream';ids['sp-scenario'].events.change();
assert.equal(ids['sp-legend'].children.length,8);
assert(ids['sp-description'].textContent.includes('4 задания'));
click('sp-event');assert(ids['sp-position'].textContent.startsWith('1 /'));
for (const box of root.querySelectorAll()) {box.checked = false; box.events.change();}
assert(ids['sp-play'].disabled);
console.log(`PASS: ${runs} single runs, ${frames} single coverage checks, ${streamRuns} multi-job runs; controls, 8 workers, seek, playback and local storage. Visual browser layout not tested.`);
