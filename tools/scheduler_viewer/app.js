(() => {
  'use strict';
  const { definitions, buildRun } = globalThis.SchedulerModel;
  const root=document.getElementById('scheduler-step-player'), $=id=>root.querySelector('#'+id);
  const colors=[1,2,3,4,5,6,7,8].map(n=>'var(--viz-series-'+n+')'), NS='http://www.w3.org/2000/svg';
  const patternNames={balanced:'равномерное',tail:'тяжёлый хвост',cluster:'кластеры',single:'одна тяжёлая итерация'};
  let chosen=new Set(['mask','lazy']), runs={}, panelViews=[], index=0, timer=null, speed=4;
  const reduced=window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  function svgNode(tag,attrs={},text) {const node=document.createElementNS(NS,tag);Object.entries(attrs).forEach(([k,v])=>node.setAttribute(k,v));if(text!==undefined)node.textContent=text;return node;}
  function text(svg,x,y,value,anchor='start') {svg.append(svgNode('text',{x,y,'text-anchor':anchor},value));}
  function selectedDefinitions(){return definitions.filter(d=>chosen.has(d.id));}
  function lastIndex(){return Math.max(0,...selectedDefinitions().map(d=>runs[d.id].frames.length-1));}
  function stop(){if(timer!==null)clearInterval(timer);timer=null;$('sp-play').textContent='▶ Запустить';}
  function persist() {
    try {
      localStorage.setItem('scheduler-viewer-v1', JSON.stringify({
        modelContent: {pattern: $('sp-pattern').value, workers:Number($('sp-workers').value), scenario:$('sp-scenario').value, schedulers: [...chosen], frame: index},
        privateContent: {speed}
      }));
    } catch (_) { /* Storage is optional. */ }
  }
  function getFrame(id){return runs[id].frames[Math.min(index,runs[id].frames.length-1)];}
  function makePanels() {
    $('sp-panels').replaceChildren();panelViews=[];
    selectedDefinitions().forEach(d=>{
      const section=document.createElement('section');section.className='sp-panel';
      const title=document.createElement('h3');title.textContent=d.name;
      const note=document.createElement('div');note.className='text-small sp-note';note.textContent=d.note;
      const svg=svgNode('svg',{class:'sp-picture',role:'img',height:390,'aria-label':d.name});
      const event=document.createElement('div');event.className='sp-event';
      const count=document.createElement('div');count.className='text-small tabular-nums';
      const jobs=document.createElement('div');jobs.className='text-small sp-jobs';
      section.append(title,note,jobs,svg,event,count);$('sp-panels').append(section);
      panelViews.push({id:d.id,section,svg,event,count,jobs,marks:new Map(),width:0,previous:-1});
    });
    if(!chosen.size)$('sp-panels').textContent='Выбери хотя бы один планировщик.';
    draw();
  }
  function drawPanel(view) {
    const run=runs[view.id],f=getFrame(view.id), width=Math.max(220,view.section.getBoundingClientRect().width), left=32, right=8, col=(width-left-right)/run.workers, top=38, plotHeight=run.jobs.length>1?640:320, scale=plotHeight/run.n, height=plotHeight+70;
    const resize=Math.abs(view.width-width)>1;
    if(resize) {
      view.width=width;view.svg.replaceChildren();view.marks.clear();view.svg.setAttribute('viewBox','0 0 '+width+' '+height);view.svg.setAttribute('height',height);
      const ticks=run.jobs.length>1?[...run.jobs.map(j=>j.begin),run.n]:[0,16,32,48,64];
      ticks.forEach(i=>text(view.svg,left-6,top+i*scale+4,''+i,'end'));
      text(view.svg,4,18,'i');
      for(let w=0;w<run.workers;w++){text(view.svg,left+(w+.5)*col,20,'П'+w,'middle');view.svg.append(svgNode('line',{x1:left+w*col,y1:top,x2:left+w*col,y2:top+plotHeight,stroke:'var(--border)'}));}
      for(const job of run.jobs)if(job.id){const y=top+job.begin*scale;view.svg.append(svgNode('line',{x1:left,y1:y,x2:width-right,y2:y,stroke:'var(--border)','stroke-dasharray':'4 4'}));}
      view.layer=svgNode('g');view.svg.append(view.layer);view.overlay=svgNode('g');view.svg.append(view.overlay);
    }
    const marks=[];
    for(let i=0;i<run.n;) {if(f.done[i]<0){i++;continue;}const a=i,w=f.done[i];while(i<run.n&&f.done[i]===w)i++;marks.push({id:'done-'+a,a,b:i,w,opacity:.18,done:true});}
    f.queues.forEach((q,w)=>q.forEach(t=>marks.push({id:'t'+t.id,a:t.cursor,b:t.end,w,opacity:.3,queued:true})));
    f.active.forEach((t,w)=>{if(!t||t.cursor>=t.end)return;marks.push({id:'t'+t.id,a:t.cursor,b:t.end,w,opacity:.45});if(t.reserved>t.cursor)marks.push({id:'r'+t.id,a:t.cursor,b:Math.min(t.reserved,t.end),w,opacity:.9});});
    const alive=new Set();const animate=!reduced&&!resize&&index===view.previous+1;
    for(const m of marks) {
      alive.add(m.id);let rect=view.marks.get(m.id);
      if(!rect){rect=svgNode('rect');view.layer.append(rect);view.marks.set(m.id,rect);}
      rect.style.transition=animate?'transform 180ms ease, height 180ms ease, opacity 180ms ease':'none';
      rect.style.transform='translate('+(left+m.w*col+3)+'px,'+(top+m.a*scale+1)+'px)';
      rect.setAttribute('width',col-6);rect.setAttribute('height',Math.max(2,(m.b-m.a)*scale-2));rect.setAttribute('fill',colors[m.w]);rect.setAttribute('opacity',m.opacity);
      rect.replaceChildren(svgNode('title',{},'П'+m.w+' ['+m.a+', '+m.b+') · '+(m.done?'выполнено':m.queued?'обычная задача в очереди':m.id[0]==='r'?'зарезервировано':'активный остаток')));
    }
    for(const [key,node] of view.marks)if(!alive.has(key)){node.remove();view.marks.delete(key);}
    view.overlay.replaceChildren();
    f.active.forEach((t,w)=>{
      for(const chunk of t?.privateChunks||[]) {
        if(chunk.a<=t.cursor)continue;
        const line=svgNode('line',{x1:left+w*col+3,y1:top+chunk.a*scale,x2:left+(w+1)*col-3,y2:top+chunk.a*scale,stroke:'var(--foreground)','stroke-dasharray':'3 3'});
        line.append(svgNode('title',{},'Приватный буфер П'+w+': ['+chunk.a+', '+chunk.b+'). Это ещё не задача в очереди.'));
        view.overlay.append(line);
      }
    });
    marks.filter(m=>m.done&&m.b-m.a>=4).forEach(m=>text(view.overlay,left+(m.w+.5)*col,top+(m.a+m.b)*scale/2+4,'✓','middle'));
    if(f.move) {
      const m=f.move,y=top+(m.a+m.b)*scale/2,x1=left+(m.from+.5)*col,x2=left+(m.to+.5)*col;
      if(m.from!==m.to) {
        view.overlay.append(svgNode('path',{d:'M '+x1+' '+y+' L '+x2+' '+y,fill:'none',stroke:'var(--foreground)','stroke-width':2}));
        const dir=x2>x1?1:-1;view.overlay.append(svgNode('path',{d:'M '+(x2-dir*6)+' '+(y-5)+' L '+x2+' '+y+' L '+(x2-dir*6)+' '+(y+5),fill:'none',stroke:'var(--foreground)','stroke-width':2}));
      } else view.overlay.append(svgNode('line',{x1:left+m.from*col+2,y1:top+m.a*scale,x2:left+(m.from+1)*col-2,y2:top+m.a*scale,stroke:'var(--foreground)','stroke-width':2}));
    }
    for(let w=0;w<run.workers;w++){const t=f.active[w],label=t&&t.cursor<t.end?'▶':f.queues[w].length?'Q':'—';text(view.overlay,left+(w+.5)*col,height-12,col<55?label:t&&t.cursor<t.end?'работа':f.queues[w].length?'очередь':'ждёт','middle');}
    view.jobs.replaceChildren();
    if(run.jobs.length>1)for(const job of f.jobs){const row=document.createElement('div');row.className='sp-job';row.textContent='J'+(job.id+1)+' ['+job.begin+', '+job.end+') · '+patternNames[job.pattern]+' · t='+job.at+' · '+({future:'ещё не пришло',waiting:'ожидает',running:'в работе',done:'готово'})[job.status];view.jobs.append(row);}
    view.event.textContent=f.text;view.count.textContent='Такт модели '+f.tick+' · готово '+f.done.filter(w=>w>=0).length+'/'+run.n+' · обычных задач создано в модели: '+f.created+(f.privateSplits===undefined?'':' · делений приватного буфера: '+f.privateSplits);
    view.svg.setAttribute('aria-label',definitions.find(d=>d.id===view.id).name+'. '+f.text);view.previous=index;
  }
  function draw() {
    const last=lastIndex();index=Math.max(0,Math.min(index,last));
    $('sp-seek').max=last;$('sp-seek').value=index;$('sp-position').textContent=index+' / '+last;
    $('sp-back').disabled=index===0;$('sp-next').disabled=index>=last;$('sp-play').disabled=!chosen.size;
    $('sp-event').disabled=index>=last;panelViews.forEach(drawPanel);
    if(index>=last)stop();
  }
  function rebuild() {
    stop();index=0;const pattern=$('sp-pattern').value,workers=Number($('sp-workers').value),scenario=$('sp-scenario').value;
    definitions.forEach(d=>runs[d.id]=buildRun(d.id,pattern,64,workers,2,scenario));
    root.style.setProperty('--panel-min',workers===8?'360px':'240px');
    $('sp-description').textContent=(scenario==='stream'?'4 задания × 64 итерации':'64 итерации')+' · '+workers+' потоков · grain = 2. Вертикаль — сквозной индекс итерации, цвет — поток. Бледное — очередь/остаток, насыщенное — резерв, пунктир — приватный буфер (не очередь), ✓ — готово; ▶ работа, Q очередь, — ждёт.';
    $('sp-legend').replaceChildren();
    colors.slice(0,workers).forEach((color,w)=>{const item=document.createElement('span');item.className='sp-key';const swatch=document.createElement('span');swatch.className='sp-swatch';swatch.style.background=color;item.append(swatch,document.createTextNode('П'+w));$('sp-legend').append(item);});
    $('sp-cost').textContent=({balanced:'Стоимость каждой итерации — 1 условная единица.',tail:'[0, 48): стоимость 1; [48, 64): стоимость 16 на итерацию.',cluster:'[8, 16) и [40, 48): стоимость 6; остальное — 1.',single:'Итерация 63: стоимость 32; все остальные — 1.'})[pattern];
    if(scenario==='stream')$('sp-cost').textContent='Поступления: J1 выбранного паттерна — t=0; J2 равномерное — t=8; J3 кластеры — t=24; J4 одна тяжёлая итерация — t=40. Такт — обход работников в модели, не миллисекунда. Внешний отправитель не занимает worker; запускаем ожидающие задания перед новой кражей. Это учебная политика допуска.';
    makePanels();
  }
  function seek(i){stop();index=i;draw();persist();$('sp-announcement').textContent='Кадр '+index;}
  definitions.forEach(d=>{
    const label=document.createElement('label');label.className='form-check';
    const box=document.createElement('input');box.type='checkbox';box.className='form-check-input';box.value=d.id;box.checked=chosen.has(d.id);
    const span=document.createElement('span');span.className='form-check-label';span.textContent=d.name;
    box.addEventListener('change',()=>{stop();if(box.checked)chosen.add(d.id);else chosen.delete(d.id);makePanels();persist();});
    label.append(box,span);$('sp-choices').append(label);
  });
  $('sp-pattern').addEventListener('change',()=>{rebuild();persist();});
  $('sp-workers').addEventListener('change',()=>{rebuild();persist();});
  $('sp-scenario').addEventListener('change',()=>{rebuild();persist();});
  $('sp-speed').addEventListener('change',()=>{stop();speed=Number($('sp-speed').value);persist();});
  $('sp-restart').addEventListener('click',()=>seek(0));$('sp-back').addEventListener('click',()=>seek(index-1));$('sp-next').addEventListener('click',()=>seek(index+1));
  $('sp-seek').addEventListener('input',()=>seek(Number($('sp-seek').value)));
  $('sp-event').addEventListener('click',()=>{let next=index+1;const structural=new Set(['arrival','activate','publish','split','private-split','split-steal','steal']);while(next<lastIndex()&&!selectedDefinitions().some(d=>{const f=runs[d.id].frames[next];return f&&structural.has(f.type);}))next++;seek(next);});
  $('sp-compare-native').addEventListener('click',()=>{
    stop();chosen=new Set(['lazy','native-auto']);index=0;
    root.querySelectorAll('#sp-choices input').forEach(box=>box.checked=chosen.has(box.value));
    makePanels();persist();
  });
  $('sp-play').addEventListener('click',()=>{
    if(timer!==null){stop();persist();return;}if(index>=lastIndex())index=0;
    $('sp-play').textContent='Ⅱ Пауза';timer=setInterval(()=>{index++;draw();if(timer===null)persist();},1000/speed);persist();
  });
  function restore(state) {
    if(!state)return;const model=state.modelContent||{},savedSpeed=state.privateContent?.speed;
    const nextPattern=['balanced','tail','cluster','single'].includes(model.pattern)?model.pattern:$('sp-pattern').value;
    const nextChosen=Array.isArray(model.schedulers)?model.schedulers.filter(id=>definitions.some(d=>d.id===id)):[...chosen];
    const nextFrame=Number.isInteger(model.frame)?model.frame:index;
    const nextSpeed=[1,4,12].includes(savedSpeed)?savedSpeed:speed;
    const nextWorkers=[4,8].includes(model.workers)?String(model.workers):$('sp-workers').value;
    const nextScenario=['single','stream'].includes(model.scenario)?model.scenario:$('sp-scenario').value;
    if(nextPattern===$('sp-pattern').value&&nextWorkers===$('sp-workers').value&&nextScenario===$('sp-scenario').value&&nextFrame===index&&nextSpeed===speed&&JSON.stringify([...new Set(nextChosen)].sort())===JSON.stringify([...chosen].sort()))return;
    $('sp-workers').value=nextWorkers;$('sp-scenario').value=nextScenario;
    if(['balanced','tail','cluster','single'].includes(model.pattern))$('sp-pattern').value=model.pattern;
    if(Array.isArray(model.schedulers))chosen=new Set(model.schedulers.filter(id=>definitions.some(d=>d.id===id)));
    if([1,4,12].includes(savedSpeed)){speed=savedSpeed;$('sp-speed').value=String(speed);}
    root.querySelectorAll('#sp-choices input').forEach(box=>box.checked=chosen.has(box.value));
    rebuild();if(Number.isInteger(model.frame))index=model.frame;draw();
  }
  rebuild();
  try { restore(JSON.parse(localStorage.getItem('scheduler-viewer-v1'))); } catch (_) { /* Storage is optional. */ }
  const query=new URLSearchParams(window.location?.search||'');
  if(definitions.some(d=>d.id===query.get('model')))restore({modelContent:{schedulers:[query.get('model')],workers:Number(query.get('workers')),pattern:query.get('pattern'),scenario:'single',frame:0}});
  if(query.has('model')) {
    document.body.classList.add('paired');
    $('sp-compare-native').hidden=true;
    for(const id of ['sp-pattern','sp-workers','sp-scenario'])$(id).disabled=true;
    root.querySelectorAll('#sp-choices input').forEach(box=>box.disabled=true);
  }
  let width=0;new ResizeObserver(()=>{const next=root.getBoundingClientRect().width;if(Math.abs(width-next)>1){width=next;panelViews.forEach(drawPanel);}}).observe(root);
  document.addEventListener('visibilitychange',()=>{if(document.hidden)stop();});
})();
