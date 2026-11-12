'use strict';
  const definitions = [
    {id:'eigen',name:'Eigen + simple split',note:'Пул Eigen + условный двоичный partitioner до grain. Владелец публикует правую половину в свою очередь.'},
    {id:'sharing',name:'EgorkaZ: sharing + stealing',note:'Упрощение: адресная публикация каждому потоку, затем дробление до grain. Не воспроизводит дерево доставки оригинала.'},
    {id:'auto',name:'Принцип TBB Auto',note:'Учебный бюджет по числу потоков; затем хозяин отдаёт остаток при спросе. Не точный алгоритм TBB Auto.'},
    {id:'mask',name:'Битмасочный Rapid',note:'Одна последовательная группа: следующий запуск ждёт завершения текущего. Callback-остатки не крадутся.'},
    {id:'lazy',name:'Rapid + Lazy: вор делит хвост',note:'После старта вор сам забирает половину незарезервированного остатка работающего владельца. Обычная задача появляется при краже.'},
    {id:'native-auto',name:'Rapid + Native Auto',note:'После старта владелец продолжает Auto: бюджет, приватный буфер, публикация обычных задач. Вор берёт задачи из очереди, а не активный хвост.'},
    {id:'native-simple',name:'Rapid + Native Simple',note:'Rapid раздаёт префикс одного дерева. Каждый владелец затем публикует правые половины до grain; воры берут готовые задачи.'},
    {id:'native-static',name:'Rapid + Native Static',note:'Пропорциональные стартовые части. При полном захвате деление уже закончено; адаптивной кражи активного остатка нет.'},
    {id:'native-affinity',name:'Rapid + Native Affinity',note:'Пропорции и предпочтения workers, затем адаптивный приватный буфер и обычные задачи. Модель не воспроизводит lifetime proxy и историю между вызовами.'}
  ];
  function costsFor(pattern,n) {
    return Array.from({length:n},(_,i)=>pattern==='single'&&i===n-1?32:pattern==='tail'&&i>=Math.floor(n*3/4)?16:pattern==='cluster'&&((i>=Math.floor(n/8)&&i<Math.floor(n/4))||(i>=Math.floor(n*5/8)&&i<Math.floor(n*3/4)))?6:1);
  }
  function buildRun(mode,pattern,n=64,workers=4,grain=2,scenario='single',options={}) {
    if(mode.startsWith('native-')) {
      const native=typeof module!=='undefined'?require('./native-model.js'):globalThis.SchedulerNativeModel;
      return native.run(mode,pattern,n,workers,grain,scenario,costsFor,options);
    }
    const jobSize=n;
    const jobs=(scenario==='stream'?[{pattern,at:0},{pattern:'balanced',at:8},{pattern:'cluster',at:24},{pattern:'single',at:40}]:[{pattern,at:0}])
      .map((job,id)=>({...job,id,begin:id*jobSize,end:(id+1)*jobSize}));
    n=jobSize*jobs.length;
    const arrived=new Set(), launched=new Set(), pending=[];
    const costs=jobs.flatMap(job=>costsFor(job.pattern,jobSize)), queues=Array.from({length:workers},()=>[]), active=Array(workers).fill(null), done=Array(n).fill(-1), executions=Array(n).fill(0), frames=[];
    let serial=0, created=0, turn=0;
    const task=(a,b,worker,budget=1,ordinary=true)=>{if(ordinary)created++;return {id:++serial,cursor:a,end:b,worker,budget,reserved:a,quantum:grain,cap:Math.max(grain,Math.ceil((b-a)/8)),remaining:0};};
    function snapshot(text,type='work',move=null) {
      frames.push({text,type,move,created,tick:Math.floor(turn/workers),jobs:jobs.map(job=>({...job,status:!arrived.has(job.id)?'future':!launched.has(job.id)?'waiting':done.slice(job.begin,job.end).every(w=>w>=0)?'done':'running'})),done:done.slice(),active:active.map(t=>t?{...t}:null),queues:queues.map(q=>q.map(t=>({...t})))});
    }
    function admitArrivals() {
      const tick=Math.floor(turn/workers);
      for(const job of jobs)if(job.at<=tick&&!arrived.has(job.id)) {
        arrived.add(job.id);pending.push(job);
        snapshot('Пришло J'+(job.id+1)+' ['+job.begin+', '+job.end+'), паттерн '+job.pattern+'.','arrival');
      }
      while(pending.length) {
        const job=pending[0];
        const free=active.map((t,w)=>({t,w})).filter(({t,w})=>(!t||t.cursor===t.end)&&!queues[w].length).map(({w})=>w);
        if((mode==='mask'&&free.length!==workers)||(mode==='lazy'&&!free.length))break;
        pending.shift();launched.add(job.id);
        if(mode==='mask'||mode==='lazy') {
          free.forEach((w,i)=>active[w]=task(job.begin+Math.floor(i*jobSize/free.length),job.begin+Math.floor((i+1)*jobSize/free.length),w,1,false));
          snapshot('J'+(job.id+1)+' захватывает свободных участников: '+free.map(w=>'П'+w).join(', ')+'. Остальные продолжают предыдущие задания.','activate');
        } else if(mode==='sharing') {
          for(let w=0;w<workers;w++)queues[w].push(task(job.begin+Math.floor(w*jobSize/workers),job.begin+Math.floor((w+1)*jobSize/workers),w));
          snapshot('J'+(job.id+1)+': адресные задачи добавлены в mailboxes, в том числе занятым потокам.','publish');
        } else {
          const target=job.id%workers;queues[target].push(task(job.begin,job.end,target,mode==='auto'?workers:1));
          snapshot('Корневая задача J'+(job.id+1)+' ['+job.begin+', '+job.end+') опубликована в очередь П'+target+'.','publish');
        }
      }
    }
    snapshot('Диапазон [0, '+n+') ещё не запущен.','initial');
    if(scenario==='stream') {
      admitArrivals();
    } else if(mode==='mask'||mode==='lazy') {
      arrived.add(0);launched.add(0);
      for(let w=0;w<workers;w++)active[w]=task(Math.floor(w*n/workers),Math.floor((w+1)*n/workers),w,1,false);
      snapshot('Группа получает команду: каждому участнику выдан стартовый диапазон. Обычных задач пока нет.','activate');
    } else if(mode==='sharing') {
      arrived.add(0);launched.add(0);
      for(let w=0;w<workers;w++) {
        const t=task(Math.floor(w*n/workers),Math.floor((w+1)*n/workers),w);queues[w].push(t);
        snapshot('Адресная публикация ['+t.cursor+', '+t.end+') в mailbox П'+w+'.','publish',{from:0,to:w,a:t.cursor,b:t.end});
      }
    } else {
      arrived.add(0);launched.add(0);
      active[0]=task(0,n,0,mode==='auto'?workers:1);
      snapshot('П0 начинает корневую задачу [0, '+n+').','activate');
    }
    for(let guard=0;guard<100000;guard++) {
      if(scenario==='stream')admitArrivals();
      if(done.every(x=>x>=0)) {
        snapshot('Все итерации завершены. В настоящем runtime здесь также нужен протокол join.','finish');
        return {frames,costs,executions,n,workers,jobs,scenario};
      }
      const w=turn++%workers;
      let t=active[w];
      if(t&&t.cursor===t.end) {active[w]=null;t=null;}
      if(!t) {
        if(queues[w].length) {
          t=queues[w].pop();active[w]=t;t.worker=w;
          snapshot('П'+w+' берёт свою задачу ['+t.cursor+', '+t.end+').','take');continue;
        }
        const victim=queues.findIndex((q,j)=>j!==w&&q.length);
        if(victim>=0) {
          t=queues[victim].shift();t.worker=w;active[w]=t;
          snapshot('П'+w+' крадёт готовую задачу ['+t.cursor+', '+t.end+') из очереди П'+victim+'.','steal',{from:victim,to:w,a:t.cursor,b:t.end});continue;
        }
        if(mode==='lazy') {
          const donor=active.findIndex((v,j)=>j!==w&&v&&v.end-v.reserved>grain);
          if(donor>=0) {
            const owner=active[donor], first=owner.reserved, last=owner.end, mid=first+Math.floor((last-first)/2);
            owner.end=mid;active[w]=task(mid,last,w);
            snapshot('П'+w+' делит доступный хвост П'+donor+' ['+first+', '+last+'): забирает ['+mid+', '+last+'). Создана задача.','split-steal',{from:donor,to:w,a:mid,b:last});continue;
          }
        }
        continue;
      }
      const size=t.end-t.cursor;
      const boundary=t.remaining===0&&t.reserved<=t.cursor;
      const idle=active.some((v,j)=>j!==w&&(!v||v.cursor===v.end)&&queues[j].length===0);
      const eager=mode==='eigen'||mode==='sharing';
      const autoSplit=mode==='auto'&&boundary&&size>grain&&(t.budget>1||(idle&&size>grain*2));
      if(boundary&&size>grain&&(eager||autoSplit)) {
        const oldEnd=t.end, mid=t.cursor+Math.floor(size/2), budget=Math.max(1,Math.floor(t.budget/2));
        t.end=mid;t.budget=budget;const child=task(mid,oldEnd,w,budget);queues[w].push(child);
        snapshot('П'+w+' оставляет ['+t.cursor+', '+mid+'), публикует ['+mid+', '+oldEnd+')'+(mode==='auto'?' — бюджет / спрос.':'.'),'split',{from:w,to:w,a:mid,b:oldEnd});continue;
      }
      if(boundary) {
        const amount=mode==='mask'||eager?size:t.quantum;
        t.reserved=Math.min(t.end,t.cursor+amount);
        snapshot('П'+w+' резервирует ['+t.cursor+', '+t.reserved+'). Эта часть недоступна для кражи.','reserve');
        if(mode==='lazy')t.quantum=Math.min(t.cap,t.quantum*2);
        continue;
      }
      if(t.remaining===0)t.remaining=costs[t.cursor];
      t.remaining--;
      if(t.remaining>0) {
        snapshot('П'+w+' внутри process('+t.cursor+'); осталось '+t.remaining+' условных единиц работы. Другие не могут разделить этот вызов.');
      } else {
        const i=t.cursor++;if(done[i]!==-1)throw Error('duplicate iteration '+i);
        done[i]=w;executions[i]++;
        snapshot('П'+w+' завершил итерацию '+i+'.'+(t.cursor===t.end?' Его диапазон закончен.':''));
      }
    }
    throw Error('Teaching model did not terminate: '+mode+'/'+pattern);
  }

const schedulerModel = { definitions, costsFor, buildRun };
if (typeof module !== 'undefined' && module.exports) module.exports = schedulerModel;
else globalThis.SchedulerModel = schedulerModel;
