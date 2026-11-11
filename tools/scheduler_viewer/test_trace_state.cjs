const assert = require('node:assert/strict');
const {traceSnapshot,traceProjection,traceLayer,searchWindows} = require('./trace.js');
const blocks = [
  {start:10,end:20,tid:0}, {start:22,end:30,tid:1},
  {start:11,end:40,tid:2,incomplete:true},
  {start:1,end:8,tid:0}, {start:40,end:50,tid:0}
];
const projection = {blocks,flows:[{target:{ns:15}},{target:{ns:25}}]};
assert.equal(traceSnapshot({},projection,10,40,0).blocks[0].state,'running');
const mid = traceSnapshot({},projection,10,40,10);
assert.deepEqual(mid.blocks.map(x=>x.state),['done','unknown']);
assert.equal(mid.flows.length,1); assert.equal(mid.latest,15);
assert.equal(traceSnapshot({},projection,10,40,30).blocks.filter(x=>x.state==='done').length,2);
assert(traceSnapshot({},{...projection,initialOnly:true},10,40,30).blocks.every(x=>x.state==='unknown'));
assert.equal(traceSnapshot({},projection,10,40,0).flows.length,0);
assert.equal(traceSnapshot({},projection,10,40,30).flows.length,2,'Keep both observed transfers, not just the last');
assert.equal(traceSnapshot({},projection,10,40,30,'last').flows.length,1);
assert.equal(traceSnapshot({},projection,10,40,30,'recent').flows.length,2);
assert.deepEqual(['activate','queue_delivery','steal_range','steal_queue','split'].map(traceLayer),
  ['activation','queues','steals','steals','splits']);
const cut=traceProjection({spans:[{id:8,tid:1,worker:6}],instants:[{kind:'split',id:9,parent:8,ns:12,tid:0,worker:0,a:53,b:56}]},
  {blocks:[],flows:[{kind:'split',target:{ns:30}}]});
assert.equal(cut.flows.length,1);
assert.equal(cut.flows[0].target.ns,12,'Creation must not wait for child execution');
assert.equal(cut.flows[0].source.worker,6);
assert.equal(cut.flows[0].actor,0);
const noRange={kind:'steal_queue',id:17,source:{tid:0,worker:0,ns:8},target:{tid:1,worker:1,ns:8}};
const unseenRange=traceProjection({spans:[],instants:[],flows:[noRange]}, {blocks:[],flows:[]});
assert.equal(unseenRange.flows.length,1,'Keep an observed theft even when its range is unknown');
assert.equal(unseenRange.flows[0].range,null,'Do not invent iteration coordinates');
const missingLane=traceProjection({spans:[],flows:[],instants:[{kind:'steal_queue',id:19,tid:1,worker:1,ns:8,a:7,b:1}]},{blocks:[],flows:[]});
assert.equal(missingLane.flows[0].source.worker,7,'The observed donor index remains usable without a recorded native lane');
assert.equal(missingLane.flows[0].source.tid,null);
const searches={events:[{ns:1,worker:1,kind:'resident_queue_scan'},
  {ns:2,worker:1,kind:'resident',ph:'B'},{ns:10,worker:1,kind:'resident_range_probe'},
  {ns:11,worker:1,kind:'resident_queue_scan'},{ns:15,worker:1,kind:'resident_range_probe'}]};
assert.deepEqual(searchWindows(searches,0,20,20),[{worker:1,start:11,end:15}], 'Do not span a Rapid command');
assert.equal(searchWindows(searches,0,20,14).length,0,'No future probe');
for(let offset=0;offset<=30;offset++) {
  const state=traceSnapshot({},projection,10,40,offset);
  for(const b of state.blocks){assert(b.start<=10+offset);assert(b.start>=10&&b.start<40);if(b.state==='done')assert(b.end<=10+offset);}
  assert(state.flows.every(f=>f.target.ns<=10+offset));
}
console.log('PASS: exact start/end, future exclusion, wave isolation, unknown/incomplete scopes, rewind, observed transfers');
