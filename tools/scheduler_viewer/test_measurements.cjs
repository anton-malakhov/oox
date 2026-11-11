const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {analyze}=require('./analyze_measurements.cjs');
const file=process.argv[2];if(!file)throw Error('Pass measurement.json path');
const d=JSON.parse(fs.readFileSync(file,'utf8')),median=a=>{const s=[...a].sort((a,b)=>a-b),i=Math.floor(s.length/2);return s.length%2?s[i]:(s[i-1]+s[i])/2;};
assert.equal(d.protocol.instrumented,false);
for(const job of d.jobs){const raw=JSON.parse(fs.readFileSync(path.join(path.dirname(file),job.raw),'utf8'));assert.deepEqual(raw,job.data);assert(raw.oracle_ok);}
for(const row of d.paired){const collect=mode=>d.jobs.filter(j=>j.workers===row.workers&&j.mode===mode).sort((a,b)=>a.repetition-b.repetition).map(j=>j.data.cases.find(c=>c.case===row.case).ns);
  const a=collect('RAPID_AUTO'),b=collect(row.reference),ratios=a.map((v,i)=>v/b[i]);
  assert.deepEqual(row.pairs,ratios);assert(Math.abs(row.ratio.median-median(ratios))<1e-12);
}
const r=analyze(d,d,file);assert(r.replication_same_binaries);
assert(r.model_checks.every(x=>Number.isFinite(x.static_error)&&(Number.isFinite(x.event_error)||(x.event_error===null&&x.simulation_error))));
assert(r.body_ns[1000]>r.body_ns[100]);
const invalid=JSON.parse(JSON.stringify(d));invalid.primitives_ns.sc_store_acquire_load.median=0;
assert.throws(()=>analyze(invalid,null,file),/Invalid calibration/);
console.log(`PASS: ${d.jobs.length} raw process records, ${d.paired.length} independently recomputed A/B rows, model checks, invalid probe rejection`);
