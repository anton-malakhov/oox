// SPDX-License-Identifier: Apache-2.0
'use strict';
(() => {
  const $ = id => document.getElementById(id);
  let records = [], left = 0, width = 1, selected = null;
  const enabled = id => $(id).checked;
  const color = id => `hsl(${(id * 137.508) % 360} 62% 52%)`;
  function bounds(r) {
    const w = $('wave').value === 'all' ? null : r.waves.find(x => x.a === Number($('wave').value));
    return w ? [w.start, w.end] : [0, $('wave').value === 'all' ? r.max : 0];
  }
  function fit() { left = 0; width = Math.max(1, ...records.filter(r=>r.visible).map(r => { const [a,b] = bounds(r); return b-a; })); draw(); }
  function accept(data, label) {
    const r = TraceModel.parse(data); r.label = label; r.visible=true; records.push(r);
  }
  function install() {
    const old = $('wave').value;
    const waves = [...new Set(records.flatMap(r => r.waves.map(w => w.a)))].sort((a,b)=>a-b);
    $('wave').replaceChildren(...[...waves.map(n=>new Option(String(n),String(n))),new Option('Все','all')]);
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
      const canvas = document.createElement('canvas'); canvas.setAttribute('aria-label',`Временная шкала ${r.data.mode}`); r.canvas=canvas;
      const legend=document.createElement('div');legend.className='thread-legend';
      for(const lane of r.lanes){const entry=document.createElement('span'),swatch=document.createElement('i');swatch.style.background=color(lane.tid);entry.append(swatch,document.createTextNode(`T${lane.tid}${lane.worker>=0?` · W${lane.worker}`:''}`));legend.append(entry);}
      heading.append(title,stats); panel.append(heading,legend,canvas); panels.append(panel);
      const choice=document.createElement('label'),check=document.createElement('input');
      check.type='checkbox';check.checked=r.visible;panel.hidden=!r.visible;
      check.onchange=()=>{r.visible=check.checked;panel.hidden=!r.visible;fit();};
      choice.append(check,document.createTextNode(r.data.mode));$('modes').append(choice);
      let drag = null;
      canvas.addEventListener('wheel',e=>{ e.preventDefault(); const box=canvas.getBoundingClientRect(); const f=Math.max(0,Math.min(1,(e.clientX-box.left-90)/(box.width-106))); zoom(Math.exp(e.deltaY*.002),f); },{passive:false});
      canvas.addEventListener('pointerdown',e=>{drag={x:e.clientX,left};canvas.setPointerCapture(e.pointerId);});
      canvas.addEventListener('pointermove',e=>{if(drag){left=drag.left-(e.clientX-drag.x)/(canvas.clientWidth-106)*width;draw();}});
      canvas.addEventListener('pointerup',e=>{if(drag&&Math.abs(e.clientX-drag.x)<4)pick(r,e);drag=null;});
      canvas.addEventListener('pointercancel',()=>drag=null);
    }
    fit();
  }
  function zoom(factor, anchor=.5) { const next=Math.max(20,width*factor);left+=(width-next)*anchor;width=next;draw(); }
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
    if(hit){const s=hit.value;detail({mode:r.data.mode,id:s.id,execution_id:s.execution_id,kind:s.kind,parent:s.parent,thread:s.tid,worker:s.worker,range:[s.a,s.b],start_us:(s.start-bounds(r)[0])/1000,duration_us:(s.end-s.start)/1000,incomplete:!!s.incomplete},r);}
  }
  function draw() {
    const fg=getComputedStyle(document.body).color;
    const grid=records.length?getComputedStyle(records[0].canvas.parentElement).borderTopColor:fg;
    for(const r of records){
      if(!r.canvas||!r.visible)continue;
      const chosen=selected?.record===r?selected.id:null;
      const [origin,end]=bounds(r),lo=origin+left,hi=lo+width;
      const viewLo=Math.max(origin,lo),viewHi=Math.min(end,hi);
      const projection=TraceModel.rangeProjection(r,enabled('callbacks'));
      const w=r.canvas.clientWidth||1000,x0=90,plot=w-x0-16,top=35,bottom=355,h=405;
      const extent=Math.max(1,r.data.tasks||1);
      const dpr=devicePixelRatio||1;r.canvas.width=w*dpr;r.canvas.height=h*dpr;r.canvas.style.height=h+'px';
      const c=r.canvas.getContext('2d');c.scale(dpr,dpr);c.font='12px system-ui';c.lineWidth=1;
      const x=ns=>x0+(ns-lo)/width*plot,y=index=>top+index/extent*(bottom-top);
      r.plotLeft=x(viewLo);r.plotRight=x(viewHi);
      r.hits=[];r.flowHits=[];
      const ticks=w<700?4:8;
      for(let i=0;i<=ticks;i++){
        const xx=x0+plot*i/ticks;c.strokeStyle=grid;c.beginPath();c.moveTo(xx,top);c.lineTo(xx,bottom);c.stroke();
        c.fillStyle=fg;c.textAlign=i===ticks?'right':i===0?'left':'center';
        c.fillText(((left+width*i/ticks)/1000).toFixed(width<10000?2:1),xx,20);
      }
      for(const value of [...new Set([0,1,2,3,4].map(i=>Math.round(extent*i/4)))]){
        const yy=y(value);c.strokeStyle=grid;c.beginPath();c.moveTo(x0,yy);c.lineTo(w-16,yy);c.stroke();
        c.fillStyle=fg;c.textAlign='right';c.fillText(String(value),x0-10,yy+4);
      }
      c.fillStyle=fg;c.textAlign='left';c.fillText('Время, мкс',x0,bottom+30);
      c.save();c.translate(14,(top+bottom)/2);c.rotate(-Math.PI/2);c.textAlign='center';c.fillText('Диапазон итераций [a, b)',0,0);c.restore();
      c.save();c.beginPath();c.rect(r.plotLeft,top,Math.max(0,r.plotRight-r.plotLeft),bottom-top);c.clip();
      for(const s of projection.blocks){
        const interval=TraceModel.clipInterval(s.start,s.end,viewLo,viewHi);if(!interval)continue;
        const xx=x(interval[0]),xx2=x(interval[1]);
        const yy=y(s.a),hh=Math.max(1,y(s.b)-yy),ww=Math.max(1,xx2-xx);
        c.globalAlpha=projection.initialOnly?.15:chosen&&s.execution_id!==chosen?.4:.85;
        c.fillStyle=color(s.tid);c.fillRect(xx,yy,ww,hh);c.globalAlpha=1;
        if(s.incomplete||projection.initialOnly){c.strokeStyle=color(s.tid);c.setLineDash([2,3]);c.strokeRect(xx,yy,ww,hh);c.setLineDash([]);}
        const label=`[${s.a}, ${s.b})`;
        if(hh>=17&&ww>c.measureText(label).width+8){
          c.save();c.beginPath();c.rect(xx,yy,ww,hh);c.clip();c.fillStyle='#101820';c.fillText(label,xx+4,yy+Math.min(hh-4,16));c.restore();
        }
        r.hits.push({x:xx,y:yy,w:Math.max(4,ww),h:Math.max(3,hh),cy:yy+hh/2,value:s});
      }
      for(const f of projection.flows){
        if(f.kind.startsWith('activate')?!enabled('activation'):f.kind.startsWith('steal')?!enabled('steals'):f.kind==='split'?!enabled('splits'):!enabled('queues'))continue;
        const t1=f.source.ns,t2=f.target.ns,interval=TraceModel.clipInterval(Math.min(t1,t2),Math.max(t1,t2),viewLo,viewHi);
        if(!interval||t2<origin||t2>=end)continue;
        const x1=x(t1),x2=x(t2),mid=y((f.range[0]+f.range[1])/2);
        const targetColor=color(f.target.tid),sourceColor=color(f.source.tid);
        c.globalAlpha=chosen&&f.execution_id!==chosen?.2:.9;c.lineWidth=chosen===f.execution_id?2:1;
        c.setLineDash(f.kind.startsWith('steal')?[5,3]:f.kind==='split'?[2,3]:[]);
        if(f.kind.startsWith('steal')&&t1===t2){
          // Theft is an instantaneous observation over the transferred range.
          // Its vertical extent is iteration space, not an invented duration.
          const ya=y(f.range[0]),yb=y(f.range[1]);
          c.strokeStyle=sourceColor;c.beginPath();c.moveTo(x2,ya);c.lineTo(x2,yb);c.stroke();
          c.setLineDash([]);c.fillStyle=targetColor;c.beginPath();c.moveTo(x2+6,mid);c.lineTo(x2-3,mid-5);c.lineTo(x2-3,mid+5);c.fill();
          r.flowHits.push({x1:x2,x2,y1:ya,y2:yb,value:f});
        }else{
          const gradient=c.createLinearGradient(x1,mid,x2===x1?x2+1:x2,mid);gradient.addColorStop(0,sourceColor);gradient.addColorStop(1,targetColor);
          c.strokeStyle=gradient;c.beginPath();c.moveTo(x1,mid);c.lineTo(x2,mid);c.stroke();c.setLineDash([]);
          c.fillStyle=targetColor;c.beginPath();c.moveTo(x2,mid);c.lineTo(x2-6,mid-4);c.lineTo(x2-6,mid+4);c.fill();
          r.flowHits.push({x1:x(interval[0]),x2:x(interval[1]),y1:mid,y2:mid,value:f});
        }
      }
      c.restore();c.globalAlpha=1;c.lineWidth=1;c.setLineDash([]);
      const scope=projection.initialOnly?' · исходные назначения (старая запись)':enabled('callbacks')&&projection.blocks.some(s=>s.kind==='callback')?' · отдельные итерации':' · выполняемые блоки';
      r.stats.textContent=`${r.data.workers} потоков · ${r.data.tasks} итераций · ${r.data.workload} · запуск ${((end-origin)/1000).toFixed(2)} мкс${scope}`;
    }
  }
  $('files').addEventListener('change',async e=>{try{for(const f of e.target.files){if(f.size>200*1024*1024)throw Error('File exceeds 200 MB');accept(JSON.parse(await f.text()),f.name);}install();}catch(error){$('status').textContent=error.message;}});
  $('clear').onclick=()=>{records=[];selected=null;$('panels').replaceChildren();$('modes').replaceChildren();$('status').textContent='';};
  $('fit').onclick=fit;$('in').onclick=()=>zoom(.5);$('out').onclick=()=>zoom(2);$('wave').onchange=fit;
  for(const id of ['activation','queues','steals','splits','callbacks'])$(id).onchange=draw;
  window.addEventListener('resize',draw);
  try {for(const data of JSON.parse($('trace-data').textContent))accept(data,data.mode);if(records.length)install();}catch(error){$('status').textContent=error.message;}
})();
