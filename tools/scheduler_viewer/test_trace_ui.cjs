const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const parser=require('../../benchmarks/scheduler_eval/tracing/model.js');
const event=(ns,tid,worker,kind,ph,id,a=0,b=0,parent=0)=>({ns,tid,worker,kind,ph,id,a,b,parent});
const events=[event(0,0,0,'wave','B',100,0,4),event(1,0,0,'range','B',1,0,4,100),
  event(2,0,0,'chunk','B',2,0,2,1),event(3,0,0,'callback','B',3,0,1,2),event(4,0,0,'callback','E',3),
  event(5,0,0,'callback','B',4,1,2,2),event(6,0,0,'callback','E',4),event(7,0,0,'chunk','E',2),
  event(8,0,0,'split','i',10,2,4,1),event(9,0,0,'offer_mailbox','i',10,1),
  event(10,1,1,'take_mailbox','i',10,1),event(10,1,1,'steal_queue','i',10,0,1),
  event(11,1,1,'task','B',10),event(12,1,1,'range','B',11,2,4,10),event(13,1,1,'chunk','B',12,2,4,11),
  event(14,1,1,'callback','B',13,2,3,12),event(15,1,1,'callback','E',13),
  event(16,1,1,'callback','B',14,3,4,12),event(17,1,1,'callback','E',14),
  event(18,1,1,'chunk','E',12),event(19,1,1,'range','E',11),event(20,1,1,'task','E',10),
  event(21,0,0,'range','E',1),event(22,0,0,'wave','E',100)];
class Element{
  constructor(tag='div'){this.tag=tag;this.children=[];this.attrs={};this.events={};this.checked=true;this.value='0';this.style={};this.clientWidth=600;}
  append(...nodes){for(const n of nodes){this.children.push(n);if(n&&typeof n==='object')n.parentElement=this;}}
  prepend(...nodes){this.children.unshift(...nodes);}
  replaceChildren(...nodes){this.children=[];this.append(...nodes);}
  setAttribute(k,v){this.attrs[k]=v;}
  addEventListener(k,v){this.events[k]=v;}
  getBoundingClientRect(){return {width:600,left:0,top:0};}
  getContext(){return new Proxy({}, {get:()=>()=>{},set:()=>true});}
}
const html=fs.readFileSync(__dirname+'/trace.html','utf8');
const ids=Object.fromEntries([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element()]));
for(const id of ['activation','queues','steals','splits'])ids[id].parentElement=new Element('label');
ids.history.value='all';ids.wave.value='0';ids.callbacks.checked=true;
const record=mode=>({schema:'oox-scheduler-trace-v1',mode,tasks:4,workers:2,grain:1,work:1,workload:'tail',iterations:1,events,oracle_ok:true,dropped_events:0});
ids['trace-data'].textContent=JSON.stringify([record('RAPID_AUTO'),record('RAPID_NATIVE_AUTO')]);
const ctx={TraceModel:parser,URLSearchParams,location:{search:''},devicePixelRatio:1,
  document:{getElementById:id=>{assert(ids[id],id);return ids[id];},createElement:tag=>new Element(tag),createTextNode:text=>({text}),addEventListener(){},body:new Element(),documentElement:new Element()},
  window:{addEventListener(){}},getComputedStyle:()=>({color:'#123',borderTopColor:'#aaa',getPropertyValue:()=> '#135'}),
  Option:class extends Element{constructor(text,value){super('option');this.textContent=text;this.value=value;}},
  cancelAnimationFrame(){},requestAnimationFrame(){return 1;}};
vm.createContext(ctx);
const source=fs.readFileSync(__dirname+'/trace.js','utf8').replace(/\}\)\(\);\s*$/,'globalThis.ui={records};})();');
vm.runInContext(source,ctx);
assert(ids.activation.disabled,'No activation must be reported as missing, not fabricated');
assert.equal(ids['splits-count'].textContent,'0/2');
ids['next-transfer'].onclick();assert.equal(ids.time.value,8,'Split is visible at creation');
ids['show-all'].onclick();
const r=ctx.ui.records[0];
assert.equal(r.flowHits.length,3);
assert(r.activity.textContent.includes('П1'));
assert.equal(r.journal.children.length,3);
ids.history.value='last';ids.history.onchange();assert.equal(r.flowHits.length,2);
ids.history.value='all';ids.history.onchange();
ids.splits.checked=false;ids.splits.onchange();assert.equal(r.flowHits.length,2);
ids.queues.checked=false;ids.queues.onchange();assert.equal(r.flowHits.length,1);
ids.steals.checked=false;ids.steals.onchange();assert.equal(r.flowHits.length,0);
ids.callbacks.checked=false;ids.callbacks.onchange();assert.equal(r.hits.length,2);
ids.callbacks.checked=true;ids.callbacks.onchange();assert.equal(r.hits.length,4);
ids['compare-native'].onclick();assert(ctx.ui.records.every(r=>r.visible));
console.log('PASS: history, birth-time splits, observable filter effects, callback/chunk switch, absent layers, Native pair');
