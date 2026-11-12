const assert=require('node:assert/strict');
const E=require('./lab-engine.js');
let cases=0;
for(const n of [1,2,3,7,8,15,16,17,31,63,64,65,127,257,4096,16384]){
  const tail=Array(n).fill(1),cluster=Array(n).fill(1);
  for(let i=Math.floor(3*n/4);i<n;++i)tail[i]=16;
  for(const [a,b] of [[Math.floor(n/8),Math.floor(n/4)],[Math.floor(5*n/8),Math.floor(3*n/4)]])for(let i=a;i<b;++i)cluster[i]=6;
  for(const [pattern,expected] of [['tail',tail],['cluster',cluster]])assert.deepEqual(E.workload({n,unit:1,pattern},17),expected,`native pattern/${pattern}/${n}/seed17`);
}
// Deliberately simple serial oracle: visits each input once, sums its given cost.
function oracle(jobs){return jobs.map(j=>({id:j.id,visits:j.values.map(()=>1),work:j.values.reduce((a,b)=>a+b,0)}));}
function verify(c,label){
  const r=E.simulate(c);assert(r.ok,label+': '+r.errors);const ref=oracle(c.jobs);
  assert.equal(r.useful,ref.reduce((a,j)=>a+j.work,0),label);
  for(const expected of ref){const j=r.jobs.find(j=>j.id===expected.id);assert.deepEqual(j.visits,expected.visits,label);for(const dep of c.jobs.find(x=>x.id===j.id).after)assert(j.start>=r.jobs.find(x=>x.id===dep).end,label);}
  const work=r.segments.filter(s=>s.kind==='work').reduce((a,s)=>a+s.end-s.start,0);assert.equal(work,r.useful,label);
  assert(Math.abs(c.workers*r.time-r.useful-r.scheduled-r.idle)<1e-6,label);
  assert(r.time+1e-6>=r.lower,label);
  for(let w=0;w<c.workers;w++){const segments=r.segments.filter(s=>s.worker===w).sort((a,b)=>a.start-b.start);for(let i=1;i<segments.length;i++)assert(segments[i].start>=segments[i-1].end,label);}
  cases++;return r;
}
for(const preset of ['static','stealing','sharing','lazy'])for(const workers of [1,2,4,8])for(const n of [1,2,3,7,16])for(const shape of ['balanced','tail','cluster','single','random']){
  const c=E.preset(preset);c.workers=workers;c.seed=17;c.jobs[0].n=n;c.jobs[0].pattern=shape;c.jobs[0].values=E.workload(c.jobs[0],17);verify(c,`${preset}/${workers}/${n}/${shape}/seed17`);
}
for(const policy of ['fixed','doubling','sqrt','heartbeat','guided'])for(let seed=1;seed<=12;seed++){
  const c=E.preset();c.grainPolicy=policy;c.seed=seed;c.jobs=Array.from({length:3},(_,i)=>({id:'J'+i,n:7+i*3,unit:30,pattern:i===0?'single':'random',arrival:i*125,after:i===2?['J0','J1']:[]}));
  c.jobs.forEach((j,i)=>j.values=E.workload(j,seed+i));verify(c,`${policy}/seed${seed}`);
}
for(const n of [2048,8192]){
  const c=E.preset();c.seed=42;c.grain=16;c.jobs[0].n=n;c.jobs[0].pattern='random';c.jobs[0].values=E.workload(c.jobs[0],42);verify(c,`large/${n}/seed42`);
}
const serial=E.preset('static');serial.workers=1;Object.keys(serial.costs).forEach(k=>serial.costs[k]=0);serial.jobs[0].n=5;serial.jobs[0].values=[2,3,5,7,11];assert.equal(verify(serial,'serial exact').time,28);
const staticTwo=E.preset('static');staticTwo.workers=2;Object.keys(staticTwo.costs).forEach(k=>staticTwo.costs[k]=0);staticTwo.jobs[0].n=4;staticTwo.jobs[0].values=[1,2,3,4];assert.equal(verify(staticTwo,'two-worker static exact').time,7);
staticTwo.jobs[0].n=5;staticTwo.jobs[0].values=[1,2,3,4,5];assert.equal(verify(staticTwo,'odd static remainder goes to leading owner').time,9);
const invalid=E.preset();invalid.nodes.find(n=>n.op==='compute').enabled=false;assert(!E.simulate(invalid).ok);
const cycle=E.preset();cycle.nodes.find(n=>n.id==='local').yes='local';assert(!E.simulate(cycle,{maxEvents:500}).ok);
const deps=E.preset();deps.jobs[0].after=['J1'];deps.jobs.push({...deps.jobs[0],id:'J2',after:['J1']});assert(E.validate(deps).length);
const a=E.simulate(E.preset());const b=E.simulate(E.preset());assert.deepEqual(a,b,'reproducible seed');
for(const name of ['stealing','sharing']){const c=E.preset(name);c.queueCapacity=1;c.jobs[0].values=E.workload(c.jobs[0],c.seed);verify(c,'bounded queue/'+name);}
assert(E.validate({...E.preset(),nodes:[null]}).length);
console.log(`PASS: ${cases} deterministic cases vs serial oracle; exact toy timings, accounting, dependencies, concurrency, invalid graph, replay`);
