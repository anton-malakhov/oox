// SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {parse,rangeProjection,clipInterval} = require('./model.js');
const event = (ns,tid,worker,kind,ph,id,a=0,b=0,parent=0) => ({ns,tid,worker,kind,ph,id,a,b,parent});
const trace = events => ({schema:'oox-scheduler-trace-v1',events,dropped_events:0,oracle_ok:true});
// Explicit reference: two lanes, one queued child, one activation.
const events = [
  event(0,0,0,'wave','B',1,0,8),event(1,0,0,'range','B',2,0,4,1),
  event(2,0,0,'split','i',3,2,4,2),event(3,0,0,'offer_mailbox','i',3,1),
  event(4,1,1,'take_mailbox','i',3,1),event(5,1,1,'task','B',3),
  event(6,1,1,'range','B',4,2,4,3),event(7,1,1,'range','E',4),
  event(8,1,1,'task','E',3),event(9,0,0,'activate','i',5,1),
  event(10,1,1,'resident','B',5),event(11,1,1,'resident','E',5),
  event(12,0,0,'range','E',2),event(13,0,0,'wave','E',1)
];
const result = parse(trace(events));
assert.equal(result.spans.length,5);assert.equal(result.unmatched,0);
assert.deepEqual(result.flows.map(f=>f.kind).sort(),['activate','queue_delivery','split']);
assert.equal(result.flows.find(f=>f.kind==='queue_delivery').source.tid,0);
assert.equal(result.flows.find(f=>f.kind==='queue_delivery').target.tid,1);
// No migration is inferred merely from execution on another lane.
assert.equal(parse(trace([event(0,0,0,'task','B',1),event(1,0,0,'task','E',1),
  event(2,1,1,'task','B',1),event(3,1,1,'task','E',1)])).flows.length,0);
assert.equal(parse(trace([event(0,0,0,'task','B',1)])).unmatched,1);
assert.throws(()=>parse(trace([{...events[0],ns:-1}])));
assert.equal(parse(trace([event(1,0,0,'take_mailbox','i',9,0),
  event(1,1,1,'offer_mailbox','i',9,0)])).flows.length,1);
// Equal clocks retain known per-thread order and acquisition type. A later
// defer cannot become the source of the take which preceded that defer.
const tied=parse(trace([event(10,0,0,'take_mailbox','i',9),event(10,0,0,'defer','i',9),
  event(10,1,1,'take_deferred','i',9),event(9,2,2,'offer_mailbox','i',9)]));
assert.deepEqual(tied.flows.map(f=>[f.source.kind,f.target.kind]),
  [['offer_mailbox','take_mailbox'],['defer','take_deferred']]);
assert.equal(parse(trace([event(1,0,0,'take_mailbox','i',9),
  event(1,0,0,'offer_mailbox','i',9)])).flows.length,0);
// The initial owner envelope overlaps its donated child. Only actually run
// chunks may color the iteration-space plot as executed work.
const detailed = events.map(e=>({...e,ns:e.ns*10}));
detailed.push(event(40,0,0,'chunk','B',11,0,2,2),event(110,0,0,'chunk','E',11),
  event(61,1,1,'chunk','B',12,2,4,4),event(69,1,1,'chunk','E',12));
const projected=rangeProjection(parse(trace(detailed)));
assert.equal(projected.initialOnly,false);
assert.deepEqual(projected.blocks.map(s=>[s.tid,s.a,s.b]),[[0,0,2],[1,2,4]]);
assert.deepEqual(projected.flows.find(f=>f.kind==='queue_delivery').range,[2,4]);
assert.equal(rangeProjection(result).initialOnly,true);
assert.equal(rangeProjection({...result,data:{...result.data,mode:'RAPID_START'}}).initialOnly,false);
// The transferred task, donor range, and executing chunk have distinct IDs.
const stolen=parse(trace([event(0,0,0,'wave','B',1,0,4),event(1,0,0,'range','B',2,0,4,1),
  event(5,1,1,'split','i',3,2,4,2),event(5,1,1,'steal_range','i',3,0,1),
  event(5,1,1,'task','B',3),event(5,1,1,'range','B',4,2,4,3),
  event(5,1,1,'chunk','B',5,2,4,4),event(15,1,1,'chunk','E',5),
  event(16,1,1,'range','E',4),event(17,1,1,'task','E',3),
  event(19,0,0,'range','E',2),event(20,0,0,'wave','E',1)]));
const theft=rangeProjection(stolen).flows.find(f=>f.kind==='steal_range');
assert.deepEqual([theft.source.id,theft.source.range_id,theft.source.tid,theft.source.worker],[3,2,0,0]);
assert.deepEqual(theft.range,[2,4]);
assert.equal(theft.execution_id,3);
assert.equal(stolen.spans.find(s=>s.id===5).execution_id,3);
const wrapped=parse(trace([event(0,0,0,'activate','i',10),event(1,1,1,'resident','B',10),
  event(2,1,1,'phase','B',11,0,0,10),event(3,1,1,'range','B',12,4,8,11),
  event(4,1,1,'range','E',12),event(5,1,1,'phase','E',11),event(6,1,1,'resident','E',10)]));
assert.deepEqual(rangeProjection(wrapped).flows[0].range,[4,8]);
assert.equal(wrapped.spans.find(s=>s.id===12).execution_id,10);
assert.deepEqual(clipInterval(5,35,10,20),[10,20]);
assert.deepEqual(clipInterval(10,10,10,20),[10,10]);
assert.equal(clipInterval(20,20,10,20),null);
assert.equal(clipInterval(0,9,10,20),null);
assert.equal(clipInterval(5,35,20,10),null);
// Exercise the actual viewer handlers without a browser: canvas geometry and
// click ordering must agree with the displayed, clipped drawing order.
function viewer(data) {
  const noop=()=>{}, context=new Proxy({measureText:()=>({width:1e6}),
    createLinearGradient:()=>({addColorStop:noop})},{get:(o,k)=>o[k]??noop});
  class Element {
    constructor(){this.value='0';this.checked=true;this.clientWidth=1000;this.textContent='';this.style={};}
    append(...children){for(const child of children)if(child&&typeof child==='object')child.parentElement=this;}
    replaceChildren(...children){this.children=children;this.append(...children);}
    addEventListener(){}
    setAttribute(){}
    getContext(){return context;}
    getBoundingClientRect(){return {left:0,top:0,width:this.clientWidth};}
  }
  const elements=new Map();
  const get=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
  get('splits').checked=false;get('callbacks').checked=false;
  get('trace-data').textContent=JSON.stringify([{mode:'RAPID_AUTO',tasks:4,workers:2,...data}]);
  const sandbox={TraceModel:require('./model.js'),document:{getElementById:get,
    createElement:()=>new Element(),createTextNode:value=>value,body:new Element()},
    Option:function(text,value){this.value=value;},devicePixelRatio:1,
    getComputedStyle:()=>({color:'#000',borderTopColor:'#ccc'}),window:{addEventListener:noop}};
  const source=require('node:fs').readFileSync(require.resolve('./viewer.js'),'utf8');
  require('node:vm').runInNewContext(source.replace(/\}\)\(\);\s*$/,
    'globalThis.viewerTest={records,pick,draw};})();'),sandbox);
  return {...sandbox.viewerTest,get};
}
const drawn=viewer(trace(stolen.events)),drawnRecord=drawn.records[0];
const marker=drawnRecord.flowHits.find(f=>f.value.kind==='steal_range');
assert.ok(drawnRecord.hits.some(h=>marker.x1>=h.x&&marker.x1<=h.x+h.w));
drawn.pick(drawnRecord,{clientX:marker.x1,clientY:(marker.y1+marker.y2)/2});
assert.equal(JSON.parse(drawn.get('detail').textContent).kind,'steal_range');
const clipped=viewer(trace([event(5,1,1,'chunk','B',2,0,4),event(10,0,0,'wave','B',1,0,4),
  event(20,0,0,'wave','E',1),event(35,1,1,'chunk','E',2)]));
assert.equal(clipped.records[0].hits.length,1);
assert.equal(clipped.records[0].hits[0].x,90);
assert.equal(clipped.records[0].hits[0].w,894);
// Generated independent interval oracle; lanes and buffer ordering vary.
for(let count=1;count<=256;count++){
  const input=[], expected=[];
  for(let i=0;i<count;i++){
    const start=i*4, tid=i%7,id=i+1;
    input.push(event(start,tid,tid,'task','B',id),event(start+2,tid,tid,'task','E',id));
    expected.push([id,tid,start,start+2]);
  }
  input.sort((a,b)=>a.tid-b.tid||a.ns-b.ns);
  const actual=parse(trace(input));
  assert.deepEqual(actual.spans.map(s=>[s.id,s.tid,s.start,s.end]),expected,`case=${count}`);
  assert.equal(actual.flows.length,0);
}
for(const path of process.argv.slice(2)){
  const data=JSON.parse(require('node:fs').readFileSync(path,'utf8'));
  const parsed=parse(data);
  assert.equal(data.oracle_ok,true,path);
  if(!data.dropped_events)assert.equal(parsed.unmatched,0,path);
  console.log(path,parsed.spans.length,'spans',parsed.flows.length,'flows');
}
console.log('trace model tests PASS');
