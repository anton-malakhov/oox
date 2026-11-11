// SPDX-License-Identifier: Apache-2.0
'use strict';
// State projection never invents queue contents or reservations from a range envelope.
function traceLayer(kind) {
  return kind.startsWith('activate') ? 'activation' : kind.startsWith('steal') ? 'steals'
    : kind === 'split' ? 'splits' : 'queues';
}
function traceProjection(record, projection) {
  const spans = new Map(record.spans.map(s => [s.id, s]));
  const key=f=>[f.kind,f.id,f.source.tid,f.source.ns,f.target.tid,f.target.ns].join('/');
  const mapped=new Set(projection.flows.filter(f=>f.kind!=='split').map(key));
  // Missing range metadata must not erase an observed theft from the journal.
  // It only prevents drawing that event at a guessed iteration coordinate.
  const unmapped=(record.flows||[]).filter(f=>f.kind!=='split'&&!mapped.has(key(f)))
    .map(f=>({...f,range:null,execution_id:f.id}));
  const theftKeys=new Set((record.flows||[]).filter(f=>f.kind.startsWith('steal'))
    .map(f=>[f.kind,f.id,f.target.tid,f.target.ns].join('/')));
  const orphanThefts=record.instants.filter(e=>['steal_range','steal_queue'].includes(e.kind)&&
    !theftKeys.has([e.kind,e.id,e.tid,e.ns].join('/'))).map(e=>({kind:e.kind,id:e.id,
      source:{...e,tid:null,worker:e.a},target:e,range:null,execution_id:e.id}));
  // A split is observed at creation, not at the child's eventual execution.
  // Draw a cut on the known donor lane; the creator can be the thief itself.
  const cuts = record.instants.filter(e => e.kind === 'split' && e.a < e.b).map(e => {
    const donor = spans.get(e.parent), point = {...e, tid: donor?.tid ?? e.tid, worker: donor?.worker ?? e.worker};
    return {kind: 'split', id: e.id, source: point, target: point, split: e,
      actor: e.worker, range: [e.a, e.b], execution_id: e.id};
  });
  return {...projection, flows: [...projection.flows.filter(f => f.kind !== 'split'), ...unmapped, ...orphanThefts, ...cuts]
    .sort((a, b) => a.target.ns - b.target.ns)};
}
function traceSnapshot(record, projection, origin, end, offset, history = 'all') {
  const now=origin+offset;
  const blocks=projection.blocks.filter(s=>s.start>=origin&&s.start<end&&s.start<=now)
    .map(s=>({...s,state:projection.initialOnly||s.incomplete?'unknown':s.end<=now?'done':'running'}));
  const observed=projection.flows.filter(f=>f.target.ns>=origin&&f.target.ns<end&&f.target.ns<=now);
  const latest=observed.reduce((last,f)=>Math.max(last,f.target.ns),-1);
  const flows = history === 'last' ? observed.filter(f=>f.target.ns===latest)
    : history === 'recent' ? observed.filter(f=>f.target.ns>=now-5000) : observed;
  return {blocks,flows,observed,latest,now};
}
function searchWindows(record, origin, end, now) {
  const starts=new Map(),rows=[];
  for(const e of record.events) {
    if(e.ns<origin||e.ns>=end||e.ns>now)continue;
    if(e.ph==='B'&&['task','resident'].includes(e.kind))starts.delete(e.worker);
    if(e.kind==='resident_queue_scan')starts.set(e.worker,e.ns);
    if(e.kind==='resident_range_probe'&&starts.has(e.worker)) {
      rows.push({worker:e.worker,start:starts.get(e.worker),end:e.ns});starts.delete(e.worker);
    }
  }
  return rows;
}
if(typeof module!=='undefined')module.exports={traceSnapshot,traceProjection,traceLayer,searchWindows};
(() => {
  if(typeof document==='undefined')return;
  if(new URLSearchParams(location.search).has('mode'))document.body.classList.add('paired');
  const $ = id => document.getElementById(id);
  $('compare-native').hidden=new URLSearchParams(location.search).has('mode');
  let records = [], selected = null;
  let cursor = 0, animation = null, stamp = null, duration = 1;
  const enabled = id => $(id).checked;
  const color = id => id<8?getComputedStyle(document.documentElement).getPropertyValue('--viz-series-'+(id+1)).trim():`hsl(${(id*137.508)%360} 62% 52%)`;
  const threadColor=(r,tid)=>color(r.lanes.find(l=>l.tid===tid)?.worker>=0?r.lanes.find(l=>l.tid===tid).worker:tid+64);
  function bounds(r) {
    const w = $('wave').value === 'all' ? null : r.waves.find(x => x.a === Number($('wave').value));
    return w ? [w.start, w.end] : [0, $('wave').value === 'all' ? r.max : 0];
  }
  function stop() { if(animation!==null)cancelAnimationFrame(animation);animation=null;stamp=null;$('play').textContent='▶ Запустить'; }
  function fit() { stop();duration=Math.max(1,...records.filter(r=>r.visible).map(r=>{const [a,b]=bounds(r);return b-a;}));cursor=0;draw(); }
  function seek(value) { stop();cursor=Math.max(0,Math.min(duration,value));draw(); }
  function step(direction) {
    const times=records.filter(r=>r.visible).flatMap(r=>{const [a,b]=bounds(r);return r.events.filter(e=>e.ns>=a&&e.ns<=b).map(e=>e.ns-a);});
    const candidates=times.filter(t=>direction>0?t>cursor:t<cursor);
    seek(candidates.length?candidates.reduce((a,b)=>direction>0?Math.min(a,b):Math.max(a,b)):direction>0?duration:0);
  }
  function nextTransfer() {
    const times=records.filter(r=>r.visible).flatMap(r=>{const [a,b]=bounds(r);
      return r.projections[0].flows.filter(f=>enabled(traceLayer(f.kind))&&f.target.ns>=a&&f.target.ns<b&&f.target.ns-a>cursor).map(f=>f.target.ns-a);});
    seek(times.length?times.reduce((a,b)=>Math.min(a,b)):duration);
  }
  function advance(now) {
    if(stamp!==null)cursor=Math.min(duration,cursor+(now-stamp)*Number($('rate').value));
    stamp=now;draw();if(cursor>=duration){stop();return;}animation=requestAnimationFrame(advance);
  }
  function accept(data, label) {
    const r = TraceModel.parse(data), mode=new URLSearchParams(location.search).get('mode');
    r.projections=[false,true].map(callbacks=>traceProjection(r,TraceModel.rangeProjection(r,callbacks)));
    r.label = label; r.visible=!mode||data.mode===mode; records.push(r);
  }
  function install() {
    const old = $('wave').value;
    const waves = [...new Set(records.flatMap(r => r.waves.map(w => w.a)))].sort((a,b)=>a-b);
    $('wave').replaceChildren(...waves.map(n=>new Option(String(n),String(n))));
    $('wave').value = waves.includes(Number(old)) && old !== 'all' ? old : waves.length ? String(waves[0]) : 'all';
    const settings = records.map(r => [r.data.workers,r.data.tasks,r.data.work,r.data.grain,r.data.workload,r.data.iterations].join('/'));
    const messages = [];
    if (new Set(settings).size > 1) messages.push('⚠ Разные параметры нагрузки: это не парное сравнение.');
    for (const r of records) {
      if (r.data.dropped_events || r.unmatched) messages.push(`⚠ ${r.data.mode}: потеряно ${r.data.dropped_events || 0} событий, незавершённых границ ${r.unmatched}. Стрелки могут отсутствовать.`);
      if (!r.data.oracle_ok) messages.push(`⚠ ${r.data.mode}: проверка посещения итераций НЕ ПРОЙДЕНА.`);
      if(r.data.scope_drain_timed_out||r.data.pending_scopes||r.data.truncated_scopes)
        messages.push(`⚠ ${r.data.mode}: запись завершена с незакрытыми интервалами задач.`);
      if(r.data.closing_rejected_scopes)
        messages.push(`⚠ ${r.data.mode}: при остановке отклонено ${r.data.closing_rejected_scopes} новых интервалов.`);
    }
    $('status').textContent = messages.join('\n');
    const panels = $('panels'); panels.replaceChildren();
    $('modes').replaceChildren();
    for (const r of records) {
      const panel = document.createElement('section'); panel.className='panel';
      const heading = document.createElement('div'); heading.className='heading';
      const title = document.createElement('h2'); title.textContent=r.data.mode;
      const stats = document.createElement('span'); stats.className='stats'; r.stats=stats;
      const canvas = document.createElement('canvas'); canvas.setAttribute('aria-label',`Состояние потоков ${r.data.mode}`); r.canvas=canvas;
      const event=document.createElement('p');r.event=event;
      const activity=document.createElement('p');r.activity=activity;activity.className='worker-summary';
      const journal=document.createElement('details'),summary=document.createElement('summary');
      summary.textContent='События до курсора: кто, у кого и что забрал';
      const list=document.createElement('div');list.className='event-journal';r.journal=list;
      journal.append(summary,list);
      const search=document.createElement('details'),searchTitle=document.createElement('summary');
      searchTitle.textContent='Диагностика: обход очередей перед проверкой хвоста';
      const searchBody=document.createElement('div');r.search=searchBody;search.append(searchTitle,searchBody);
      const legend=document.createElement('div');legend.className='thread-legend';
      for(const lane of r.lanes){const entry=document.createElement('span'),swatch=document.createElement('i');swatch.style.background=threadColor(r,lane.tid);entry.title=`Внутренняя дорожка записи T${lane.tid}`;entry.append(swatch,document.createTextNode(lane.worker>=0?`П${lane.worker}`:`T${lane.tid}`));legend.append(entry);}
      heading.append(title,stats); panel.append(heading,legend,canvas,event,activity,journal,search); panels.append(panel);
      const choice=document.createElement('label'),check=document.createElement('input');
      check.type='checkbox';check.checked=r.visible;panel.hidden=!r.visible;
      if(new URLSearchParams(location.search).has('mode'))check.disabled=true;
      check.onchange=()=>{r.visible=check.checked;panel.hidden=!r.visible;fit();};
      choice.append(check,document.createTextNode(r.data.mode));$('modes').append(choice);r.checkbox=check;r.panel=panel;
      canvas.addEventListener('click',e=>pick(r,e));
    }
    fit();
    $('compare-native').disabled=!records.some(r=>r.data.mode==='RAPID_NATIVE_AUTO');
  }
  function detail(value, record) {
    selected={id:value.execution_id ?? value.id,record};
    $('detail').textContent=JSON.stringify(value,null,2);draw();
  }
  function pick(r,e) {
    const box=r.canvas.getBoundingClientRect(), x=e.clientX-box.left,y=e.clientY-box.top;
    if(x<r.plotLeft||x>r.plotRight||y<35||y>355)return;
    for(const f of [...r.flowHits].reverse()) {
      const dx=f.x2-f.x1,dy=f.y2-f.y1, t=Math.max(0,Math.min(1,((x-f.x1)*dx+(y-f.y1)*dy)/(dx*dx+dy*dy||1)));
      if(Math.hypot(x-f.x1-t*dx,y-f.y1-t*dy)<7){detail({mode:r.data.mode,id:f.value.id,execution_id:f.value.execution_id,kind:f.value.kind,source:f.value.source,target:f.value.target,split:f.value.split},r);return;}
    }
    const hit=r.hits.filter(h=>x>=h.x&&x<=h.x+h.w&&y>=h.y&&y<=h.y+h.h)
      .sort((a,b)=>Math.abs(y-a.cy)-Math.abs(y-b.cy))[0];
    if(hit){const s=hit.value;detail({mode:r.data.mode,id:s.id,execution_id:s.execution_id,kind:s.kind,parent:s.parent,thread:s.tid,worker:s.worker,range:[s.a,s.b],state:s.state,start_us:(s.start-bounds(r)[0])/1000,observed_us:(Math.min(bounds(r)[0]+cursor,s.end)-s.start)/1000,duration_us:s.state==='done'?(s.end-s.start)/1000:null,incomplete:!!s.incomplete},r);}
  }
  function draw() {
    $('time').max=duration;$('time').value=cursor;$('clock').textContent=(cursor/1000).toFixed(3)+' / '+(duration/1000).toFixed(3)+' мкс';
    const fg=getComputedStyle(document.body).color;
    const grid=records.length?getComputedStyle(records[0].canvas.parentElement).borderTopColor:fg;
    for(const r of records){
      if(!r.canvas||!r.visible)continue;
      drawState(r,fg,grid);
    }
    for(const layer of ['activation','queues','steals','splits']) {
      let total=0,seen=0;
      for(const r of records.filter(r=>r.visible)){const [a,b]=bounds(r);for(const f of r.projections[0].flows)
        if(traceLayer(f.kind)===layer&&f.target.ns>=a&&f.target.ns<b){++total;if(f.target.ns<=a+cursor)++seen;}}
      $(layer).disabled=total===0;
      $(layer+'-count').textContent=`${seen}/${total}`;
      $(layer).parentElement.title=total?'Событий до курсора / за весь выбранный вызов':'В этой записи нет наблюдаемых событий этого типа';
    }
  }
  function drawState(r,fg,grid) {
      const [origin,end]=bounds(r),projection=r.projections[enabled('callbacks')?1:0];
      const state=traceSnapshot(r,{...projection,flows:projection.flows.filter(f=>enabled(traceLayer(f.kind)))},origin,end,cursor,$('history').value);
      const w=r.canvas.clientWidth||800,x0=54,right=w-10,top=35,bottom=355,h=405;
      const lanes=r.lanes,col=(right-x0)/Math.max(1,lanes.length),extent=Math.max(1,r.data.tasks);
      const index=new Map(lanes.map((lane,i)=>[lane.tid,i]));
      const x=tid=>x0+(index.get(tid)+.5)*col,y=i=>top+i/extent*(bottom-top);
      const dpr=devicePixelRatio||1;r.canvas.width=w*dpr;r.canvas.height=h*dpr;r.canvas.style.height=h+'px';
      const c=r.canvas.getContext('2d');c.scale(dpr,dpr);c.font='12px system-ui';
      r.plotLeft=x0;r.plotRight=right;r.hits=[];r.flowHits=[];
      for(let i=0;i<=4;i++){const value=Math.round(extent*i/4),yy=y(value);c.strokeStyle=grid;c.beginPath();c.moveTo(x0,yy);c.lineTo(right,yy);c.stroke();c.fillStyle=fg;c.textAlign='right';c.fillText(String(value),x0-8,yy+4);}
      for(const lane of lanes){const xx=x(lane.tid);c.fillStyle=fg;c.textAlign='center';c.fillText(lane.worker>=0?'П'+lane.worker:'T'+lane.tid,xx,20);c.strokeStyle=grid;c.beginPath();c.moveTo(xx-col/2,top);c.lineTo(xx-col/2,bottom);c.stroke();}
      c.save();c.beginPath();c.rect(x0,top,right-x0,bottom-top);c.clip();
      const ordered=[...state.blocks].sort((a,b)=>(a.state==='running')-(b.state==='running'));
      for(const s of ordered){
        if(!index.has(s.tid))continue;
        const xx=x(s.tid)-col/2+3,yy=y(s.a),ww=Math.max(1,col-6),hh=Math.max(1,y(s.b)-yy);
        c.globalAlpha=s.state==='done'?.22:s.state==='unknown'?.1:.9;c.fillStyle=threadColor(r,s.tid);c.fillRect(xx,yy,ww,hh);c.globalAlpha=1;
        if(s.state==='unknown'){c.strokeStyle=fg;c.setLineDash([2,3]);c.strokeRect(xx,yy,ww,hh);c.setLineDash([]);}
        r.hits.push({x:xx,y:yy,w:ww,h:Math.max(3,hh),cy:yy+hh/2,value:s});
      }
      const flows=state.flows;
      for(const f of flows){
        if(!f.range)continue;
        if(!index.has(f.source.tid)||!index.has(f.target.tid))continue;
        const layer=traceLayer(f.kind),palette={activation:0,queues:1,steals:2,splits:3};
        let x1=x(f.source.tid),x2=x(f.target.tid),yy=y(f.kind==='split'?f.range[0]:(f.range[0]+f.range[1])/2);
        c.strokeStyle=color(palette[layer]);c.fillStyle=c.strokeStyle;c.lineWidth=2;
        if(f.kind==='split') {
          x1-=col/2-4;x2+=col/2-4;c.setLineDash([3,3]);
          c.beginPath();c.moveTo(x1,yy);c.lineTo(x2,yy);c.stroke();c.setLineDash([]);
        } else if(x1===x2) {
          c.beginPath();c.arc(x1,yy,4,0,Math.PI*2);c.stroke();
        } else {
          const dir=x2>x1?1:-1;c.beginPath();c.moveTo(x1,yy);c.lineTo(x2,yy);c.stroke();
          c.beginPath();c.moveTo(x2-dir*5,yy-4);c.lineTo(x2,yy);c.lineTo(x2-dir*5,yy+4);c.stroke();
        }
        r.flowHits.push({x1,x2,y1:yy,y2:yy,value:f});
      }
      c.restore();c.globalAlpha=1;c.lineWidth=1;
      for(const lane of lanes){const running=state.blocks.some(s=>s.tid===lane.tid&&s.state==='running');c.fillStyle=fg;c.textAlign='center';c.fillText(running?'▶':'·',x(lane.tid),378);}
      c.fillStyle=fg;c.textAlign='left';c.fillText('Колонки — потоки',x0,398);
      const latest=state.observed.at(-1);r.event.textContent=latest?`На схеме ${r.flowHits.length} из ${state.observed.length} включённых событий до курсора. Последнее: ${eventText(latest,origin)}. Стрелки показывают историю, не продолжающуюся передачу.`:'До курсора нет включённых передач. Нажми «Следующая передача / деление» или «Показать весь вызов».';
      if(state.observed.some(f=>!f.range))r.event.textContent+=' События без записанного диапазона доступны только в журнале.';
      const calls=r.spans.filter(s=>s.kind==='callback'&&s.start>=origin&&s.start<end&&s.start<=state.now);
      const counts=new Map();for(const s of calls)counts.set(s.worker>=0?'П'+s.worker:'T'+s.tid,(counts.get(s.worker>=0?'П'+s.worker:'T'+s.tid)||0)+1);
      const thieves=[...new Set(projection.flows.filter(f=>f.kind.startsWith('steal')&&f.target.ns>=origin&&f.target.ns<end&&f.target.ns<=state.now).map(f=>laneName(f.target)))];
      r.activity.textContent=`Наблюдаемые успешные кражи до курсора: ${thieves.join(', ')||'нет записанных событий'}. `+(calls.length?'Начали итерации: '+[...counts].map(([w,n])=>w+'='+n).join(', ')+'.':'Отдельные callbacks не записаны или ещё не начались.');
      r.journal.replaceChildren();
      for(const f of state.observed.slice(-30)) {
        const button=document.createElement('button');button.type='button';button.className='journal-event';button.textContent=eventText(f,origin);
        button.onclick=()=>{seek(f.target.ns-origin);detail({kind:f.kind,range:f.range,source_worker:f.source.worker,target_worker:f.target.worker,actor:f.actor,at_us:(f.target.ns-origin)/1000},r);};r.journal.append(button);
      }
      if(state.observed.length>30){const more=document.createElement('p');more.textContent='Показаны последние 30 событий. Перемотай назад для более ранних.';r.journal.prepend(more);}
      const windows=searchWindows(r,origin,end,state.now),first=new Map();
      for(const row of windows)if(!first.has(row.worker))first.set(row.worker,row);
      r.search.replaceChildren();
      const searchNote=document.createElement('p');searchNote.textContent=windows.length?
        'Первый полностью наблюдаемый обход каждого worker: начало очередей → проверка хвоста. Интервал включает tracing и задержки ОС, не доказывает стоимость одной операции.':
        'До курсора нет завершённых интервалов этих меток. Старые записи и другие пути поиска могут их не содержать.';
      r.search.append(searchNote);
      if(windows.length){const table=document.createElement('table'),head=document.createElement('tr');
        for(const name of ['Worker','Начало, мкс','Хвост, мкс','Интервал, мкс']){const cell=document.createElement('th');cell.textContent=name;head.append(cell);}table.append(head);
        for(const row of [...first.values()].sort((a,b)=>a.worker-b.worker)){const tr=document.createElement('tr');
          for(const value of ['П'+row.worker,((row.start-origin)/1000).toFixed(3),((row.end-origin)/1000).toFixed(3),((row.end-row.start)/1000).toFixed(3)]){const cell=document.createElement('td');cell.textContent=value;tr.append(cell);}table.append(tr);}
        r.search.append(table);
      }
      r.stats.textContent=`${r.data.workers} потоков · ${r.data.tasks} итераций · ${r.data.workload} · ${Math.min(cursor,end-origin)/1000} / ${((end-origin)/1000).toFixed(2)} мкс${cursor>=end-origin?' · вызов завершён':''}`;
  }
  function laneName(event){return event.worker>=0?'П'+event.worker:'T'+event.tid;}
  function eventText(f,origin){const when=((f.target.ns-origin)/1000).toFixed(3)+' мкс',range=f.range?`[${f.range[0]}, ${f.range[1]})`:'диапазон не записан';
    if(f.kind==='split')return `${when}: создана часть ${range}, создатель ${f.actor>=0?'П'+f.actor:laneName(f.source)}`;
    const action=f.kind==='steal_range'?'кража хвоста':f.kind==='steal_queue'?'кража задачи':f.kind.startsWith('activate')?'старт группы':'доставка задачи';
    return `${when}: ${action}, ${laneName(f.source)} → ${laneName(f.target)}, ${range}`;
  }
  $('files').addEventListener('change',async e=>{try{for(const f of e.target.files){if(f.size>200*1024*1024)throw Error('File exceeds 200 MB');accept(JSON.parse(await f.text()),f.name);}install();}catch(error){$('status').textContent=error.message;}});
  $('clear').onclick=()=>{stop();cursor=0;duration=1;records=[];selected=null;$('panels').replaceChildren();$('modes').replaceChildren();$('status').textContent='';};
  $('wave').onchange=fit;
  for(const id of ['activation','queues','steals','splits','callbacks'])$(id).onchange=draw;
  $('history').onchange=draw;
  $('next-transfer').onclick=nextTransfer;
  $('compare-native').onclick=()=>{for(const r of records){r.visible=['RAPID_AUTO','RAPID_NATIVE_AUTO'].includes(r.data.mode);r.checkbox.checked=r.visible;r.panel.hidden=!r.visible;}fit();};
  $('restart').onclick=()=>seek(0);$('previous').onclick=()=>step(-1);$('next').onclick=()=>step(1);
  $('show-all').onclick=()=>seek(duration);$('time').oninput=()=>seek(Number($('time').value));
  $('play').onclick=()=>{if(animation!==null){stop();return;}if(cursor>=duration)cursor=0;stamp=null;$('play').textContent='Ⅱ Пауза';animation=requestAnimationFrame(advance);};
  document.addEventListener('visibilitychange',()=>{if(document.hidden)stop();});
  window.addEventListener('resize',draw);
  try {for(const data of JSON.parse($('trace-data').textContent))accept(data,data.mode);if(records.length)install();}catch(error){$('status').textContent=error.message;}
})();
