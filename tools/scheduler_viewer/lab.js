'use strict';
(() => {
  const E=SchedulerLab,$=id=>document.getElementById(id),copy=x=>JSON.parse(JSON.stringify(x));
  const measured=globalThis.SchedulerMeasurements||null;
  const names={take:'Взять своё',split:'Разделить и опубликовать',steal:'Украсть из очереди',tail:'Украсть свободный хвост',compute:'Вычислить порцию',wait:'Ждать / повторить',stop:'Остановить worker'};
  const costNames={publish:'Публикация задачи',rapid:'Публикация Rapid-команды',take:'Local queue',split:'Разбиение + публикация',steal:'Попытка queue steal',tail:'Попытка tail steal',reserve:'Резерв порции',migration:'Успешная передача',wait:'Интервал ожидания',join:'Join задания'};
  let config=E.preset(),baseline=copy(config),result=null,time=0,playing=null,lastStamp=null;
  function el(tag,text){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;}
  function select(options,value,change){const n=el('select');for(const [v,t] of options)n.add(new Option(t,v));n.value=value;n.onchange=()=>change(n.value);return n;}
  function number(value,change){const n=el('input');n.type='number';n.value=value;n.style.width='100px';n.oninput=n.onchange=()=>change(Number(n.value));return n;}
  function pause(){if(playing!==null)cancelAnimationFrame(playing);playing=null;lastStamp=null;$('lab-play').textContent='▶ Проиграть';}
  function dirty(){pause();result=null;$('lab-results').hidden=true;$('lab-state').textContent='Изменено — запусти A/B. База сохранена отдельно.';check();drawGraph();}
  function check(){const errs=E.validate(config);$('lab-error').textContent=errs.join('; ');return !errs.length;}
  function sync(){
    $('lab-workers').value=String(config.workers);$('lab-seed').value=config.seed;$('lab-dispatch').value=config.dispatch;$('lab-grain').value=config.grain;$('lab-grain-policy').value=config.grainPolicy;
    $('lab-queue').value=config.queueCapacity??256;
    $('lab-entry').replaceChildren(...config.nodes.map(n=>new Option(n.id,n.id)));$('lab-entry').value=config.entry;
    for(const [id,op,name] of [['lab-ab-tail','tail','кражу хвоста'],['lab-ab-queue','steal','queue stealing'],['lab-ab-split','split','дробление']]){const node=config.nodes.find(n=>n.op===op);$(id).disabled=!node;$(id).textContent=(node?.enabled?'Отключить ':'Включить ')+name;}
    renderNodes();renderJobs();renderCosts();check();drawGraph();
  }
  function renderNodes(){
    $('lab-nodes').replaceChildren();
    for(const node of config.nodes){const row=el('tr'),flag=el('input');flag.type='checkbox';flag.checked=node.enabled;flag.setAttribute('aria-label','Включён '+node.id);flag.onchange=()=>{node.enabled=flag.checked;dirty();};
      const edges=config.nodes.map(n=>[n.id,n.id]);
      const cells=[flag,el('code',node.id),select(E.operations.map(op=>[op,names[op]]),node.op,v=>{node.op=v;dirty();}),select(edges,node.yes,v=>{node.yes=v;dirty();}),select(edges,node.no,v=>{node.no=v;dirty();})];
      for(const cell of cells){const td=el('td');td.append(cell);row.append(td);}
      const remove=el('button','Удалить');remove.className='btn';remove.onclick=()=>{config.nodes=config.nodes.filter(n=>n!==node);sync();dirty();};const td=el('td');td.append(remove);row.append(td);$('lab-nodes').append(row);
    }
  }
  function renderJobs(){
    $('lab-jobs').replaceChildren();
    for(const job of config.jobs){const row=el('tr');const cells=[el('code',job.id),number(job.arrival,v=>{job.arrival=v;dirty();}),number(job.n,v=>{job.n=v;delete job.values;dirty();}),select(['balanced','tail','cluster','single','random'].map(x=>[x,x]),job.pattern,v=>{job.pattern=v;delete job.values;dirty();}),number(job.unit,v=>{job.unit=v;delete job.values;dirty();})];
      const deps=el('input');deps.value=job.after.join(',');deps.setAttribute('aria-label','Зависимости '+job.id);deps.oninput=deps.onchange=()=>{job.after=deps.value.split(',').map(s=>s.trim()).filter(Boolean);dirty();};cells.push(deps);
      for(const cell of cells){const td=el('td');td.append(cell);row.append(td);}const remove=el('button','Удалить');remove.className='btn';remove.onclick=()=>{config.jobs=config.jobs.filter(j=>j!==job);renderJobs();dirty();};const td=el('td');td.append(remove);row.append(td);$('lab-jobs').append(row);
    }
  }
  function renderCosts(){
    $('lab-costs').replaceChildren();for(const key of E.costKeys){const label=el('label',costNames[key]+' (нс) '),status=el('span');status.className='lab-badge';const describe=()=>status.textContent=measured&&config.costs[key]===measured.proxy_costs[key]?'proxy измерения':'допущение';describe();label.append(status,number(config.costs[key],v=>{config.costs[key]=v;describe();dirty();}));$('lab-costs').append(label);}
  }
  function svg(tag,attrs,text){const n=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const [k,v] of Object.entries(attrs))n.setAttribute(k,v);if(text!==undefined)n.textContent=text;return n;}
  function drawGraph(){
    const g=$('lab-graph'),layout=SchedulerFlowchart.layout(config);
    g.setAttribute('viewBox','0 0 '+layout.width+' '+layout.height);g.replaceChildren();
    const defs=svg('defs',{}),marker=svg('marker',{id:'lab-arrow',viewBox:'0 0 10 10',refX:9,refY:5,markerWidth:6,markerHeight:6,orient:'auto'});marker.append(svg('path',{d:'M 0 0 L 10 5 L 0 10 z',fill:'var(--foreground)'}));defs.append(marker);g.append(defs);
    for(const edge of layout.edges){
      const color=edge.kind==='yes'?'var(--viz-series-3)':edge.kind==='no'?'var(--viz-series-2)':'var(--foreground)';
      g.append(svg('path',{d:edge.points.map(([x,y],i)=>(i?'L ':'M ')+x+' '+y).join(' '),fill:'none',stroke:color,'stroke-width':2,'marker-end':'url(#lab-arrow)','stroke-dasharray':edge.kind==='bypass'?'5 4':'none'}));
      if(edge.label)g.append(svg('text',{x:edge.labelX,y:edge.labelY,fill:color,'font-size':17,'text-anchor':edge.labelAnchor,'paint-order':'stroke',stroke:'var(--lab-panel)','stroke-width':5},edge.label));
    }
    for(const n of layout.nodes){
      const attrs={fill:'var(--background)',stroke:n.enabled?'var(--foreground)':'var(--border)','stroke-width':2,'stroke-dasharray':n.enabled?'none':'6 4'};
      if(n.kind==='decision')g.append(svg('polygon',{...attrs,points:`${n.x+n.w/2},${n.y} ${n.x+n.w},${n.y+n.h/2} ${n.x+n.w/2},${n.y+n.h} ${n.x},${n.y+n.h/2}`}));
      else g.append(svg('rect',{...attrs,x:n.x,y:n.y,width:n.w,height:n.h,rx:n.kind==='terminal'?n.h/2:0}));
      n.lines.forEach((line,i)=>g.append(svg('text',{x:n.x+n.w/2,y:n.y+n.h/2+(i-(n.lines.length-1)/2)*24+6,fill:n.enabled?'var(--foreground)':'var(--lab-muted)','font-size':18,'text-anchor':'middle'},line)));
      if(!n.id.startsWith('@'))g.append(svg('text',{x:n.x,y:n.y-10,fill:'var(--lab-muted)','font-size':12},n.id));
    }
  }
  function persist(){try{localStorage.setItem('scheduler-lab-v1',JSON.stringify({config,baseline}));}catch{}}
  function resultTable(base,next){
    const t=el('table'),header=el('tr');for(const s of ['При заданных стоимостях','База','Изменённая'])header.append(el('th',s));t.append(header);
    const metrics=[['Завершение, мкс',r=>(r.time/1000).toFixed(3)],['Полезная работа, processor·мкс',r=>(r.useful/1000).toFixed(3)],['Scheduler + contention, processor·мкс',r=>(r.scheduled/1000).toFixed(3)],['Простой, processor·мкс',r=>(r.idle/1000).toFixed(3)],['Успешные queue / tail steals',r=>r.count.steal+' / '+r.count.tail],['Неудачные попытки',r=>String(r.count.failed)],['Нижняя граница полезной работы, мкс',r=>(r.lower/1000).toFixed(3)]];
    for(const [name,get] of metrics){const row=el('tr');row.append(el('td',name),el('td',get(base)),el('td',get(next)));t.append(row);}return t;
  }
  function run(){
    pause();if(!check())return;
    const a=E.simulate(baseline),b=E.simulate(config);if(!a.ok||!b.ok){$('lab-error').textContent='Не сравниваем скорость: '+[...(a.errors||[]),...(b.errors||[])].join('; ');$('lab-results').hidden=true;return;}
    const samples=[.5,1,2].map(scale=>{const candidate=copy(config);candidate.costScale=config.costScale*scale;return E.simulate(candidate,{record:false});});
    result={a,b};time=Math.max(a.time,b.time);$('lab-time').max=time;$('lab-time').value=time;
    $('lab-kpis').replaceChildren();const comparable=JSON.stringify(baseline.jobs)===JSON.stringify(config.jobs)&&baseline.seed===config.seed;
    for(const [label,value,note,kind] of [['База',(a.time/1000).toFixed(2)+' мкс',baseline.workers+' потоков · модель',''],['Изменённая',(b.time/1000).toFixed(2)+' мкс',config.workers+' потоков · модель',''],['Изменение времени',comparable?((b.time/a.time-1)*100).toFixed(1)+'%':'Разные входы',comparable?'Меньше — лучше. Только внутри модели.':'Не сравнивайте как ускорение.',comparable?(b.time<a.time?'lab-good':'lab-bad'):'']]){const card=el('div');card.className='lab-kpi';const l=el('div',label);l.className='lab-kpi-label';const v=el('div',value);v.className='lab-kpi-value '+kind;const n=el('div',note);n.className='lab-kpi-note';card.append(l,v,n);$('lab-kpis').append(card);}
    $('lab-summary').replaceChildren(resultTable(a,b));
    const valid=samples.filter(r=>r.ok).map(r=>r.time/1000);
    $('lab-sensitivity').textContent='Чувствительность к всем служебным стоимостям ×0.5 / ×1 / ×2: '+valid.map(v=>v.toFixed(3)).join(' / ')+' мкс. Это сценарии допущений, НЕ доверительный интервал. Невалидных сценариев: '+(samples.length-valid.length)+'.';
    $('lab-job-results').replaceChildren();const table=el('table');const head=el('tr');['Задание','База: завершение, мкс','Изменённая: завершение, мкс'].forEach(t=>head.append(el('th',t)));table.append(head);
    for(const j of b.jobs){const row=el('tr'),old=a.jobs.find(q=>q.id===j.id);row.append(el('td',j.id),el('td',old?(old.end/1000).toFixed(3):'—'),el('td',(j.end/1000).toFixed(3)));table.append(row);}$('lab-job-results').append(table);
    $('lab-results').hidden=false;$('lab-state').textContent='Проверено: каждая итерация выполнена ровно один раз. Модельные времена, точность на CPU не установлена.'+(JSON.stringify(baseline.jobs)!==JSON.stringify(config.jobs)||baseline.seed!==config.seed?' Внимание: нагрузки или seed базы и варианта различаются.':'');drawResults();persist();
  }
  function chart(canvas,r,P){
    const width=Math.max(280,canvas.parentElement.getBoundingClientRect().width),height=65+P*24,dpr=devicePixelRatio||1;canvas.width=width*dpr;canvas.height=height*dpr;canvas.style.height=height+'px';
    const ctx=canvas.getContext('2d');ctx.scale(dpr,dpr);const fg=getComputedStyle(document.body).color,limit=Math.max(result.a.time,result.b.time),x=t=>45+t/limit*(width-60);ctx.font='12px system-ui';ctx.fillStyle=fg;
    for(let w=0;w<P;w++)ctx.fillText('П'+w,4,35+w*24);
    for(let i=0;i<4;i++)ctx.fillText((limit*i/3/1000).toFixed(1),x(limit*i/3)-(i===3?28:0),14);
    for(const s of r.segments){const end=Math.min(s.end,time,r.time);if(end<=s.start)continue;ctx.fillStyle=s.kind==='work'?`hsl(${s.worker*137.5%360} 55% 50%)`:fg;ctx.globalAlpha=s.kind==='work'?.85:s.kind==='scheduler'?.35:.06;ctx.fillRect(x(s.start),22+s.worker*24,Math.max(.5,x(end)-x(s.start)),18);}
    ctx.globalAlpha=1;ctx.strokeStyle=fg;ctx.beginPath();ctx.moveTo(x(time),20);ctx.lineTo(x(time),height-25);ctx.stroke();ctx.fillText('Время модели, мкс',45,height-5);
  }
  function drawResults(){if(!result)return;$('lab-time').value=time;$('lab-clock').textContent=(time/1000).toFixed(3)+' мкс';chart($('lab-canvas-base'),result.a,baseline.workers);chart($('lab-canvas-new'),result.b,config.workers);}
  function play(now){if(lastStamp!==null)time=Math.min(Math.max(result.a.time,result.b.time),time+(now-lastStamp)*20);lastStamp=now;drawResults();if(time>=Math.max(result.a.time,result.b.time)){pause();return;}playing=requestAnimationFrame(play);}
  function evidence(){const data=SchedulerEvidence;$('lab-evidence-title').textContent='Источники: '+data.curated.length+' карточек · '+data.formulas.length+' формула · '+data.tables.length+' таблиц';
    for(const s of data.curated){const box=el('div');box.className='lab-source';const title=s.url?el('a',s.name):el('strong',s.name);if(s.url){title.href=s.url;title.target='_blank';title.rel='noopener noreferrer';}box.append(title,el('p',s.status+' · '+s.formula),el('p',s.use),el('p','Условия: '+s.limits));$('lab-sources').append(box);}
    for(const [key,target] of [['formulas','lab-formulas'],['tables','lab-tables']])for(const item of data[key]){const box=el('details');box.append(el('summary',item.source+':'+item.line+' · '+item.status),el('pre',item.text));$(target).append(box);}
    for(const doc of data.documents){const box=el('details');box.append(el('summary',doc.path),el('pre',doc.text));$('lab-documents').append(box);}
  }
  function renderMeasurements(){
    $('lab-profile').disabled=!measured;
    if(!measured){$('lab-measurement-status').textContent='Нет измеренного профиля. Запусти measure.py и analyze_measurements.cjs.';return;}
    const threadCounts=measured.protocol.threads||[...new Set(measured.measurements.map(r=>r.workers))].sort((a,b)=>a-b),kernel=measured.protocol.kernel||'cpu_relax';
    for(const p of threadCounts)if(p<=32&&!Array.from($('lab-workers').options||[]).some(o=>o.value===String(p)))$('lab-workers').add(new Option(String(p),String(p)));
    $('lab-measurement-status').textContent=measured.hardware.cpu+' · '+new Date(measured.recorded_at).toLocaleString('ru-RU',{timeZone:'Europe/Moscow'})+' МСК · без per-thread pinning · '+measured.protocol.repetitions+' парных повторов; потоки: '+threadCounts.join(', ')+'. Ядро: '+kernel+'. IQR — разброс, не доверительный интервал.';
    $('lab-profile-note').textContent='Измерены тело '+kernel+' ×1000 и горячие операции очереди. Кнопка профиля меняет эти входы у базы и варианта; Rapid-активация, tail steal, миграция и прочее остаются допущениями.';
    const table=el('table'),head=el('tr');['Микропроба (без конкуренции)','Медиана, нс','IQR, нс'].forEach(s=>head.append(el('th',s)));table.append(head);
    const labels={push_front_hot:'PushFront',pop_front_hot:'PopFront',pop_back_uncontended_same_thread:'PopBack на том же потоке',empty_pop_back:'Пустой PopBack',sc_store_acquire_load:'SC store + acquire load'};
    for(const [key,m] of Object.entries(measured.primitives_ns)){const row=el('tr');row.append(el('td',labels[key]||key),el('td',m.median.toFixed(3)),el('td',m.q25.toFixed(3)+'–'+m.q75.toFixed(3)));table.append(row);}
    $('lab-measurement-primitives').append(el('p','Средняя цена операции в плотном цикле, не межпоточная задержка. Базовая итерация '+kernel+' ×1000: '+measured.body_ns[1000].toFixed(2)+' нс.'),table);
    const selected=threadCounts.includes(8)?8:threadCounts[0];
    const label=el('label','Настоящий A/B · потоки '),picker=select(threadCounts.map(p=>[String(p),String(p)]),String(selected),()=>renderReal(picker.value));label.append(picker);$('lab-real-ab').append(el('h4','Неинструментированные C++ запуски'),label);
    const target=el('div');target.className='lab-scroll';$('lab-real-ab').append(target);
    function renderReal(workers){
      workers=Number(workers);const t=el('table'),h=el('tr');['Нагрузка','Битмаска, мкс','Наш Rapid, мкс','TBB, мкс','Rapid / битмаска','Предыдущий прогон'].forEach(s=>h.append(el('th',s)));t.append(h);
      for(const r of measured.paired.filter(r=>r.workers===workers&&r.reference==='RAPID_START')){const row=el('tr');const get=mode=>measured.measurements.find(m=>m.workers===workers&&m.case===r.case&&m.mode===mode).ns.median/1000;
        [r.case,get('RAPID_START').toFixed(3),get('RAPID_AUTO').toFixed(3),get('TBB_AUTO').toFixed(3),r.ratio.median.toFixed(3)+' ('+r.slower_pairs+'/'+r.ratio.n+' медленнее)',r.previous_ratio===null?'—':r.previous_ratio.toFixed(3)].forEach(s=>row.append(el('td',s)));t.append(row);}
      target.replaceChildren(t);
    }
    renderReal(selected);
    $('lab-real-ab').append(el('p','<1 в отношении времени — наш Rapid быстрее. Это сравнение реализаций целиком, не native-абляция одного механизма. Повтор теми же бинарниками: '+(measured.replication_same_binaries?'да':'нет')+'. Полный manifest и сырые процессы: '+measured.source));
    const check=el('table'),hh=el('tr');['Контрольные нагрузки','Static-модель: средняя абсолютная ошибка','Событийная Rapid-модель: ошибка'].forEach(s=>hh.append(el('th',s)));check.append(hh);
    for(const p of threadCounts){const r=measured.holdout[p];if(!r)continue;const row=el('tr');row.append(el('td',p+' потоков · '+r.n+' случаев'),el('td',(r.static_holdout_mape*100).toFixed(1)+'%'),el('td',r.event_holdout_mape===null?'Не рассчитано':(r.event_holdout_mape*100).toFixed(1)+'% · '+(r.event_n??r.n)+'/'+r.n));check.append(row);}
    $('lab-model-check').append(el('h4','Проверка математических оценок'));
    if(Object.keys(measured.holdout).length)$('lab-model-check').append(check,el('p','Эти случаи не использованы для вычисления u. Но это та же машина и та же сессия — не независимая валидация переносимости. Большая ошибка событийной модели означает, что её нельзя считать откалиброванной целиком.'));
    else $('lab-model-check').append(el('p','Не рассчитано. Исходные измерения доступны; для дополнительного отчёта модели нужен Node.js 18+.'));
  }
  $('lab-operation').replaceChildren(...E.operations.map(op=>new Option(names[op],op)));
  $('lab-preset').onchange=()=>{config=E.preset($('lab-preset').value);baseline=copy(config);sync();dirty();};
  $('lab-workers').onchange=()=>{config.workers=Number($('lab-workers').value);dirty();};$('lab-seed').onchange=()=>{config.seed=Number($('lab-seed').value);dirty();};
  $('lab-entry').onchange=()=>{config.entry=$('lab-entry').value;dirty();};$('lab-dispatch').onchange=()=>{config.dispatch=$('lab-dispatch').value;dirty();};$('lab-grain').onchange=()=>{config.grain=Number($('lab-grain').value);dirty();};$('lab-grain-policy').onchange=()=>{config.grainPolicy=$('lab-grain-policy').value;dirty();};
  $('lab-queue').onchange=()=>{config.queueCapacity=Number($('lab-queue').value);dirty();};
  for(const id of ['lab-seed','lab-grain','lab-queue'])$(id).oninput=$(id).onchange;
  $('lab-add-node').onclick=()=>{let i=1;while(config.nodes.some(n=>n.id==='node'+i))i++;config.nodes.push({id:'node'+i,op:$('lab-operation').value,yes:config.entry,no:config.entry,enabled:true});sync();dirty();};
  $('lab-add-job').onclick=()=>{let i=1;while(config.jobs.some(j=>j.id==='J'+i))i++;config.jobs.push({id:'J'+i,arrival:i*2000,n:64,pattern:'cluster',unit:200,after:[]});renderJobs();dirty();};
  $('lab-baseline').onclick=()=>{if(check()){baseline=copy(config);dirty();$('lab-state').textContent='Текущая схема сохранена как база; теперь меняй блоки.';persist();}};
  for(const [id,op] of [['lab-ab-tail','tail'],['lab-ab-queue','steal'],['lab-ab-split','split']])$(id).onclick=()=>{const node=config.nodes.find(n=>n.op===op);if(node){node.enabled=!node.enabled;sync();dirty();}};
  $('lab-profile').onclick=()=>{if(!measured)return;for(const c of [config,baseline]){Object.assign(c.costs,measured.proxy_costs);for(const j of c.jobs)if(!j.values)j.unit=measured.body_ns[1000];}sync();dirty();$('lab-profile-note').textContent='Применены измеренное тело и 4 прокси стоимости к базе и варианту. Остальные 6 стоимостей не измерены и остаются допущениями.';persist();};
  $('lab-run').onclick=run;$('lab-time').oninput=()=>{pause();time=Number($('lab-time').value);drawResults();};
  $('lab-play').onclick=()=>{if(!result)return;if(playing!==null){pause();return;}time=0;lastStamp=null;$('lab-play').textContent='Ⅱ Пауза';playing=requestAnimationFrame(play);};
  $('lab-export').onclick=()=>{const url=URL.createObjectURL(new Blob([JSON.stringify({config,baseline},null,2)],{type:'application/json'}));const a=el('a');a.href=url;a.download='scheduler-experiment.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
  $('lab-import').onchange=async e=>{try{const f=e.target.files[0];if(!f)return;if(f.size>256*1024)throw Error('Файл больше 256 KiB');const data=JSON.parse(await f.text());const errs=[...E.validate(data.config),...E.validate(data.baseline)];if(errs.length)throw Error(errs.join('; '));config=data.config;baseline=data.baseline;sync();dirty();}catch(error){$('lab-error').textContent=error.message;}};
  try{const saved=JSON.parse(localStorage.getItem('scheduler-lab-v1'));if(saved&&!E.validate(saved.config).length&&!E.validate(saved.baseline).length){config=saved.config;baseline=saved.baseline;}}catch{}
  window.addEventListener('resize',()=>{drawGraph();drawResults();});document.addEventListener('visibilitychange',()=>{if(document.hidden)pause();});
  sync();evidence();renderMeasurements();run();
})();
