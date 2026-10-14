const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const engine=require('./lab-engine.js');
const html=fs.readFileSync(__dirname+'/lab.html','utf8');
class Element {
  constructor(tag='div'){this.tag=tag;this.children=[];this.style={};this.value='';this.attrs={};}
  append(...children){for(const child of children){this.children.push(child);if(child&&typeof child==='object')child.parentElement=this;}}
  replaceChildren(...children){this.children=[];this.append(...children);}
  add(child){this.append(child);}
  setAttribute(key,value){this.attrs[key]=value;}
  getBoundingClientRect(){return {width:800};}
  getContext(){return new Proxy({}, {get:(target,key)=>target[key]||(()=>{}),set:(target,key,value)=>(target[key]=value,true)});}
}
const ids=Object.fromEntries([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],new Element()]));
ids['lab-canvas-base'].parentElement=new Element();ids['lab-canvas-new'].parentElement=new Element();
let saved;
const ctx={
  SchedulerLab:engine,SchedulerFlowchart:require('./flowchart.js'),SchedulerEvidence:{curated:[],formulas:[],tables:[],documents:[]},
  document:{getElementById:id=>ids[id],createElement:tag=>new Element(tag),createElementNS:(_,tag)=>new Element(tag),addEventListener(){}},
  window:{addEventListener(){}},devicePixelRatio:1,getComputedStyle:()=>({color:'#000'}),
  Option:class extends Element{constructor(text,value){super('option');this.textContent=text;this.value=value;}},
  localStorage:{getItem:()=>null,setItem:(_,value)=>saved=JSON.parse(value)},
  requestAnimationFrame:()=>1,cancelAnimationFrame(){},setTimeout
};
vm.createContext(ctx);
if(process.argv[2])ctx.SchedulerMeasurements=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
else vm.runInContext(fs.readFileSync(__dirname+'/local-measurements.js','utf8'),ctx);
vm.runInContext(fs.readFileSync(__dirname+'/lab.js','utf8'),ctx);
assert(!ids['lab-results'].hidden);
ids['lab-add-job'].onclick();
const row=ids['lab-jobs'].children[1];
const deps=row.children[5].children[0];deps.value='J1';deps.oninput();
ids['lab-run'].onclick();
assert.deepEqual(saved.config.jobs[1].after,['J1']);
let run=engine.simulate(saved.config);assert(run.jobs[1].start>=run.jobs[0].end);
const arrival=row.children[1].children[0];arrival.value='50000';arrival.oninput();ids['lab-run'].onclick();
assert.equal(saved.config.jobs[1].arrival,50000);run=engine.simulate(saved.config);assert(run.jobs[1].start>=50000);
const reserve=ids['lab-costs'].children[6].children.find(n=>n.tag==='input');reserve.value='77';reserve.oninput();ids['lab-run'].onclick();assert.equal(saved.config.costs.reserve,77);
ids['lab-baseline'].onclick();assert(ids['lab-results'].hidden,'Old A/B result must be hidden after replacing baseline');
const body=ids['lab-nodes'].children.find(row=>row.children[1].children[0].textContent==='body').children[0].children[0];
body.checked=false;body.onchange();ids['lab-run'].onclick();assert(ids['lab-error'].textContent.includes('вычислительного'));assert(ids['lab-results'].hidden);
body.checked=true;body.onchange();
if(ctx.SchedulerMeasurements){ids['lab-profile'].onclick();ids['lab-run'].onclick();assert.equal(saved.config.costs.take,ctx.SchedulerMeasurements.proxy_costs.take);assert.equal(saved.config.jobs[0].unit,ctx.SchedulerMeasurements.body_ns[1000]);}
console.log('PASS: live dependency/arrival/cost editing, persisted config, baseline invalidation, mandatory compute');
