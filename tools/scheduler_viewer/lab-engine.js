// SPDX-License-Identifier: Apache-2.0
'use strict';
(function(root) {
  const operations=['take','split','steal','tail','compute','wait','stop'];
  const costKeys=['publish','rapid','take','split','steal','tail','reserve','migration','wait','join'];
  const defaults={publish:120,rapid:180,take:20,split:100,steal:120,tail:150,reserve:30,migration:80,wait:250,join:100};
  function preset(name='lazy') {
    return {schema:'scheduler-lab-v1',name,workers:8,seed:1,grain:2,queueCapacity:256,grainPolicy:name==='lazy'?'doubling':'fixed',
      dispatch:name==='static'?'mask':name==='lazy'?'rapid':name==='sharing'?'mailbox':'tree',
      entry:'local',costs:{...defaults},costScale:1,
      jobs:[{id:'J1',arrival:0,n:64,pattern:'tail',unit:200,after:[]}],
      nodes:[
        {id:'local',op:'take',yes:'split',no:'queue',enabled:true},
        {id:'split',op:'split',yes:'local',no:'body',enabled:name==='stealing'||name==='sharing'},
        {id:'queue',op:'steal',yes:'split',no:'range',enabled:name!=='static'},
        {id:'range',op:'tail',yes:'body',no:'sleep',enabled:name==='lazy'},
        {id:'body',op:'compute',yes:'local',no:'sleep',enabled:true},
        {id:'sleep',op:'wait',yes:'local',no:'local',enabled:true}
      ]};
  }
  function validate(c) {
    const errors=[];
    if(!c||c.schema!=='scheduler-lab-v1')return ['Неизвестный формат конфигурации'];
    if(!Number.isInteger(c.workers)||c.workers<1||c.workers>32)errors.push('workers: 1..32');
    if(!Number.isInteger(c.seed)||c.seed<0||c.seed>0xffffffff)errors.push('seed: uint32');
    if(!Number.isInteger(c.grain)||c.grain<1||c.grain>65536)errors.push('grain: 1..65536');
    if(c.queueCapacity!==undefined&&(!Number.isInteger(c.queueCapacity)||c.queueCapacity<1||c.queueCapacity>16384))errors.push('queueCapacity: 1..16384');
    if(!['fixed','doubling','sqrt','heartbeat','guided'].includes(c.grainPolicy))errors.push('Неизвестная политика порций');
    if(!['mask','rapid','mailbox','tree'].includes(c.dispatch))errors.push('Неизвестный способ старта');
    if(!Number.isFinite(c.costScale)||c.costScale<0||c.costScale>100)errors.push('costScale: 0..100');
    for(const key of costKeys)if(!Number.isFinite(c.costs?.[key])||c.costs[key]<0||c.costs[key]>1e8)errors.push('Некорректная стоимость '+key);
    if(!Array.isArray(c.nodes)||!c.nodes.length||c.nodes.length>32)return [...errors,'Нужно 1..32 узла'];
    if(c.nodes.some(n=>!n||typeof n!=='object'))return [...errors,'Узлы должны быть объектами'];
    const ids=new Set(c.nodes.map(n=>n.id));
    if(ids.size!==c.nodes.length||!ids.has(c.entry))errors.push('ID должны быть уникальны; entry должен существовать');
    for(const n of c.nodes)if(!/^[a-zA-Z0-9_-]{1,32}$/.test(n.id)||!operations.includes(n.op)||!ids.has(n.yes)||!ids.has(n.no)||typeof n.enabled!=='boolean')errors.push('Неверный узел или переход: '+n.id);
    if(!c.nodes.some(n=>n.enabled&&n.op==='compute'))errors.push('Нет включённого вычислительного блока');
    if(!Array.isArray(c.jobs)||!c.jobs.length||c.jobs.length>16)return [...errors,'Нужно 1..16 заданий'];
    if(c.jobs.some(j=>!j||typeof j!=='object'))return [...errors,'Задания должны быть объектами'];
    const jobIds=new Set(c.jobs.map(j=>j.id));
    if(jobIds.size!==c.jobs.length)errors.push('ID заданий повторяются');
    let total=0;
    for(const j of c.jobs){
      if(typeof j.id!=='string'||j.id.length>32||!Number.isInteger(j.n)||j.n<1||!Number.isFinite(j.unit)||j.unit<=0||j.unit>1e8||!Number.isFinite(j.arrival)||j.arrival<0||j.arrival>1e9||!['balanced','tail','cluster','single','random'].includes(j.pattern))errors.push('Неверная нагрузка '+j.id);
      if(!Array.isArray(j.after)||j.after.some(id=>!jobIds.has(id)||id===j.id))errors.push('Неверные зависимости '+j.id);
      if(j.values&&(!Array.isArray(j.values)||j.values.length!==j.n||j.values.some(v=>!Number.isFinite(v)||v<=0||v>1e10)))errors.push('Неверные индивидуальные стоимости '+j.id);
      total+=j.n;
    }
    if(total>16384)errors.push('Слишком много итераций: максимум 16384');
    if(!errors.length){
      const visited=new Set(),visiting=new Set(),byId=new Map(c.jobs.map(j=>[j.id,j]));
      function visit(id){if(visiting.has(id)){errors.push('Цикл зависимостей заданий');return;}if(visited.has(id))return;visiting.add(id);for(const p of byId.get(id).after)visit(p);visiting.delete(id);visited.add(id);}
      for(const j of c.jobs)visit(j.id);
      for(const start of c.nodes){const seen=new Set();let n=start;while(n&&!n.enabled){if(seen.has(n.id)){errors.push('Цикл выключенных блоков');break;}seen.add(n.id);n=c.nodes.find(x=>x.id===n.no);}}
    }
    return errors;
  }
  class Heap {
    constructor(){this.a=[];this.serial=0;}
    add(at,fn){const e={at,fn,order:this.serial++};let i=this.a.length;this.a.push(e);while(i){const p=(i-1)>>1;if(!this.less(e,this.a[p]))break;this.a[i]=this.a[p];i=p;}this.a[i]=e;}
    less(a,b){return a.at<b.at||(a.at===b.at&&a.order<b.order);}
    pop(){const first=this.a[0],last=this.a.pop();if(this.a.length){let i=0;while(i*2+1<this.a.length){let ch=i*2+1;if(ch+1<this.a.length&&this.less(this.a[ch+1],this.a[ch]))ch++;if(!this.less(this.a[ch],last))break;this.a[i]=this.a[ch];i=ch;}this.a[i]=last;}return first;}
  }
  function random(seed){let x=seed>>>0;return ()=>{x=(Math.imul(x,1664525)+1013904223)>>>0;return x/4294967296;};}
  function workload(j,seed){const rng=random(seed);return j.values?.slice()||Array.from({length:j.n},(_,i)=>j.unit*(j.pattern==='tail'&&i>=Math.floor(j.n*3/4)?16:j.pattern==='single'&&i===j.n-1?32:j.pattern==='cluster'&&((i>=Math.floor(j.n/8)&&i<Math.floor(j.n/4))||(i>=Math.floor(j.n*5/8)&&i<Math.floor(j.n*3/4)))?6:j.pattern==='random'?1+Math.floor(rng()*16):1));}
  function simulate(config,{maxEvents=60000,record=true}={}) {
    const errors=validate(config);if(errors.length)return {ok:false,errors};
    const c=JSON.parse(JSON.stringify(config)),P=c.workers,nodes=new Map(c.nodes.map(n=>[n.id,n])),heap=new Heap();c.queueCapacity??=256;
    const jobs=c.jobs.map((j,k)=>({...j,values:workload(j,c.seed+k),visits:Array(j.n).fill(0),arrived:false,launched:false,completed:null,remaining:j.n}));
    const workers=Array.from({length:P},(_,id)=>({id,task:null,rng:random(c.seed+id*997),queue:[],state:'idle'}));
    const queueFree=Array(P).fill(0),rangeFree=Array(P).fill(0),segments=[],events=[];
    const count={split:0,steal:0,tail:0,failed:0,tasks:0,operations:0};
    let now=0,finish=null,eventCount=0;
    const cost=key=>c.costs[key]*c.costScale;
    function task(job,a,b,ready,ordinary){if(ordinary)count.tasks++;return {job,a,b,head:a,ready,initial:b-a,quantum:c.grain,rate:null};}
    function log(kind,w,detail){if(record)events.push({at:now,kind,worker:w,detail});}
    function delay(w,ns,kind,done,detail={}){const start=now,end=now+ns;if(record||kind==='work')segments.push({worker:w.id,start,end,kind,...detail});w.state=kind;heap.add(end,()=>{w.state='idle';done();});}
    function go(w,id){heap.add(now,()=>enter(w,id));}
    function validTask(w){if(w.task&&w.task.a>=w.task.b)w.task=null;return w.task;}
    function launchReady(){
      for(const j of jobs){
        if(j.launched||!j.arrived||!j.after.every(id=>jobs.find(q=>q.id===id).completed!==null))continue;
        const free=workers.filter(w=>!validTask(w)&&w.queue.length===0);
        if((c.dispatch==='mask'&&free.length!==P)||(c.dispatch==='rapid'&&!free.length))continue;
        if(c.dispatch==='mailbox'&&workers.some(w=>w.queue.length>=c.queueCapacity))continue;
        if(c.dispatch==='tree'&&workers[jobs.indexOf(j)%P].queue.length>=c.queueCapacity)continue;
        j.launched=true;j.start=now;const rapid=c.dispatch==='mask'||c.dispatch==='rapid';
        const targets=rapid?free:workers,ready=now+(rapid?cost('rapid'):cost('publish')*(c.dispatch==='mailbox'?P:1));
        if(c.dispatch==='tree')workers[jobs.indexOf(j)%P].queue.push(task(j,0,j.n,ready,true));
        else targets.forEach((w,k)=>{const quotient=Math.floor(j.n/targets.length),remainder=j.n%targets.length,a=k*quotient+Math.min(k,remainder),b=(k+1)*quotient+Math.min(k+1,remainder);if(a===b)return;const t=task(j,a,b,ready,!rapid);if(rapid)w.task=t;else w.queue.push(t);});
        log('launch',-1,{job:j.id,ready,participants:targets.map(w=>w.id)});
      }
    }
    function leased(w,resource,index,ns,done){const start=Math.max(now,resource[index]);resource[index]=start+ns;delay(w,start-now+ns,'scheduler',done);}
    function chunk(t){
      const remaining=t.b-t.a;
      if(c.dispatch==='mask')return remaining;
      if(c.grainPolicy==='fixed')return Math.min(remaining,c.grain);
      if(c.grainPolicy==='guided')return Math.min(remaining,Math.max(c.grain,Math.ceil(remaining/P)));
      if(c.grainPolicy==='doubling'){const n=Math.min(remaining,t.quantum);t.quantum=Math.min(Math.max(c.grain,Math.ceil(t.initial/8)),t.quantum*2);return n;}
      if(!t.rate)return Math.min(remaining,c.grain);
      const target=c.grainPolicy==='heartbeat'?20*cost('reserve'):Math.sqrt(P*cost('reserve')*t.rate*t.initial/(1+workers.filter(w=>!validTask(w)).length/P));
      return Math.min(remaining,Math.max(c.grain,Math.min(Math.ceil(remaining/4),Math.round(target/t.rate))));
    }
    function enter(w,id){
      count.operations++;launchReady();
      const node=nodes.get(id),next=ok=>go(w,ok?node.yes:node.no);
      if(!node.enabled){next(false);return;}
      const own=validTask(w);
      if(node.op==='stop'){w.state='stopped';return;}
      if(node.op==='wait'){delay(w,Math.max(1,cost('wait')),'idle',()=>next(true));return;}
      if(node.op==='take'){
        leased(w,queueFree,w.id,cost('take'),()=>{if(!validTask(w)){const i=w.queue.findLastIndex(t=>t.ready<=now);if(i>=0)w.task=w.queue.splice(i,1)[0];}next(!!validTask(w));});return;
      }
      if(node.op==='steal'||node.op==='tail'){
        if(own){next(true);return;}
        const victim=P===1?w.id:(w.id+1+Math.floor(w.rng()*(P-1)))%P;
        leased(w,node.op==='steal'?queueFree:rangeFree,victim,cost(node.op),()=>{
          if(validTask(w)){next(true);return;}
          const source=workers[victim];let acquired=null;
          if(victim!==w.id&&node.op==='steal'){const i=source.queue.findIndex(t=>t.ready<=now);if(i>=0)acquired=source.queue.splice(i,1)[0];}
          if(victim!==w.id&&node.op==='tail'){const t=validTask(source);if(t&&t.ready<=now&&t.b-t.head>c.grain){const mid=t.head+Math.floor((t.b-t.head)/2);acquired=task(t.job,mid,t.b,now,true);t.b=mid;}}
          if(acquired){w.task=acquired;count[node.op]++;log(node.op,w.id,{victim,job:acquired.job.id,a:acquired.a,b:acquired.b});delay(w,cost('migration'),'scheduler',()=>next(true));}
          else {count.failed++;next(false);}
        });return;
      }
      if(!own){next(false);return;}
      if(own.ready>now){delay(w,own.ready-now,'idle',()=>go(w,id));return;}
      if(node.op==='split'){
        if(own.b-own.a<=c.grain||w.queue.length>=c.queueCapacity){next(false);return;}
        leased(w,queueFree,w.id,cost('split'),()=>{const t=validTask(w);if(!t||t.b-t.a<=c.grain||w.queue.length>=c.queueCapacity){next(false);return;}const mid=t.a+Math.floor((t.b-t.a)/2);w.queue.push(task(t.job,mid,t.b,now,true));t.b=mid;count.split++;log('split',w.id,{job:t.job.id,at:mid});next(true);});return;
      }
      if(node.op==='compute'){
        const t=own,a=t.a,b=a+chunk(t);t.head=b;
        delay(w,cost('reserve'),'scheduler',()=>{
          const duration=t.job.values.slice(a,b).reduce((sum,v)=>sum+v,0);
          delay(w,duration,'work',()=>{
            for(let i=a;i<b;i++){t.job.visits[i]++;if(t.job.visits[i]!==1)throw Error('Duplicate iteration '+t.job.id+'/'+i);}
            t.a=b;t.rate=duration/(b-a);t.job.remaining-=b-a;log('compute',w.id,{job:t.job.id,a,b});
            if(t.job.remaining===0)heap.add(now+cost('join'),()=>{t.job.completed=now;log('join',-1,{job:t.job.id});launchReady();if(jobs.every(j=>j.completed!==null))finish=now;});
            next(true);
          },{job:t.job.id,a,b});
        });return;
      }
    }
    for(const j of jobs)heap.add(j.arrival,()=>{j.arrived=true;log('arrival',-1,{job:j.id});launchReady();});
    for(const w of workers)go(w,c.entry);
    try {while(heap.a.length&&finish===null&&eventCount++<maxEvents){const e=heap.pop();now=e.at;e.fn();}}
    catch(error){return {ok:false,errors:[error.message],eventCount};}
    if(finish===null)return {ok:false,errors:[eventCount>=maxEvents?'Предел событий: схема не завершила работу':'Работа осталась без исполнителей'],eventCount};
    const useful=jobs.reduce((sum,j)=>sum+j.values.reduce((a,b)=>a+b,0),0);
    const scheduled=segments.filter(s=>s.kind==='scheduler').reduce((sum,s)=>sum+Math.max(0,Math.min(finish,s.end)-Math.min(finish,s.start)),0);
    // With record=false overhead accounting is omitted, not inferred.
    const idle=record?Math.max(0,P*finish-useful-scheduled):null;
    const spans=new Map();
    function earliest(j){if(spans.has(j.id))return spans.get(j.id);const release=Math.max(j.arrival,...j.after.map(id=>earliest(jobs.find(p=>p.id===id))));const end=release+Math.max(...j.values);spans.set(j.id,end);return end;}
    const lower=Math.max(useful/P,...jobs.map(earliest));
    if(jobs.some(j=>j.visits.some(v=>v!==1)))return {ok:false,errors:['Нарушено однократное выполнение'],eventCount};
    if(record&&P*finish-useful-scheduled < -1e-5)return {ok:false,errors:['Нарушен баланс processor-time'],eventCount};
    return {ok:true,time:finish,useful,scheduled:record?scheduled:null,idle,lower,count,segments:record?segments:[],events,
      jobs:jobs.map(j=>({id:j.id,start:j.start,end:j.completed,arrival:j.arrival,visits:j.visits,values:j.values})),eventCount};
  }
  const api={operations,costKeys,defaults,preset,validate,workload,simulate};
  if(typeof module!=='undefined')module.exports=api;else root.SchedulerLab=api;
})(globalThis);
