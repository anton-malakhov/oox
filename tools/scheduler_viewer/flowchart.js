// SPDX-License-Identifier: Apache-2.0
'use strict';
// Layout only. No simulation state or transition semantics are changed here.
(() => {
  const questions = {take:['Есть своя','работа?'],split:['Удалось разделить','и опубликовать?'],
    steal:['Удалось взять','задачу из очереди?'],tail:['Удалось взять','свободный хвост?']};
  const operations = {take:'Своя очередь',split:'Дробление',steal:'Кража задачи',tail:'Кража хвоста',compute:'Вычисление',wait:'Ожидание',stop:'Остановка'};
  function layout(config) {
    const nodes = [{id:'@start',kind:'terminal',lines:['Начало'],enabled:true}], edges = [];
    const add = (from,to,label,kind='normal') => edges.push({from,to,label,kind});
    add('@start',config.entry,'');
    for (const n of config.nodes) {
      if (!n.enabled) {
        nodes.push({id:n.id,kind:'disabled',lines:['Пропуск:',operations[n.op]],enabled:false});
        add(n.id,n.no,'обход','bypass');
      } else if (n.op === 'compute') {
        nodes.push({id:n.id,kind:'decision',lines:['Есть работа?'],enabled:true});
        nodes.push({id:'@run:'+n.id,kind:'process',lines:['Выполнить','локальную порцию'],enabled:true});
        add(n.id,'@run:'+n.id,'да','yes');add(n.id,n.no,'нет','no');add('@run:'+n.id,n.yes,'повторить');
      } else if (n.op === 'stop') nodes.push({id:n.id,kind:'terminal',lines:['Остановить worker'],enabled:true});
      else if (n.op === 'wait') {
        nodes.push({id:n.id,kind:'process',lines:['Ожидание','и повтор'],enabled:true});add(n.id,n.yes,'повторить');
      } else {
        nodes.push({id:n.id,kind:'decision',lines:questions[n.op]||[n.op],enabled:true});
        if(n.yes===n.no)add(n.id,n.yes,'любой исход');
        else {add(n.id,n.yes,'да','yes');add(n.id,n.no,'нет','no');}
      }
    }
    const expected={local:['take','split','queue'],split:['split','local','body'],queue:['steal','split','range'],range:['tail','body','sleep'],body:['compute','local','sleep'],sleep:['wait','local','local']};
    const canonical=config.entry==='local'&&config.nodes.length===6&&config.nodes.every(n=>expected[n.id]&&[n.op,n.yes,n.no].every((v,i)=>v===expected[n.id][i]));
    const positions={'@start':[280,20,220,60],local:[280,160,220,110],split:[280,375,220,110],
      body:[280,590,220,110],'@run:body':[280,805,220,90],queue:[760,160,220,110],range:[760,375,220,110],sleep:[760,805,220,90]};
    let height=1000;
    if(canonical)for(const n of nodes){[n.x,n.y,n.w,n.h]=positions[n.id];if(n.kind==='decision'){n.x-=20;n.w+=40;}}
    else {
      // Arbitrary user graphs stay readable as a vertical flow, with return
      // links on outside rails. Do not guess a different execution graph.
      nodes.forEach((n,i)=>{n.x=360;n.y=25+i*170;n.w=300;n.h=n.kind==='decision'?110:80;});
      height=nodes.at(-1).y+nodes.at(-1).h+70;
    }
    const byId=new Map(nodes.map(n=>[n.id,n]));
    const point=(n,side)=>side==='top'?[n.x+n.w/2,n.y]:side==='bottom'?[n.x+n.w/2,n.y+n.h]:side==='left'?[n.x,n.y+n.h/2]:[n.x+n.w,n.y+n.h/2];
    const valid=edges.filter(e=>byId.has(e.from)&&byId.has(e.to));
    valid.forEach((e,i)=>{
      const a=byId.get(e.from),b=byId.get(e.to),key=e.from+'>'+e.to;
      const routes={
        'local>queue':[point(a,'right'),point(b,'left')],
        'split>local':[point(a,'left'),[220,430],[220,215],point(b,'left')],
        'queue>split':[point(a,'bottom'),[870,310],[650,310],[650,430],point(b,'right')],
        'queue>range':[point(a,'right'),[1080,215],[1080,335],[870,335],point(b,'top')],
        'range>body':[point(a,'bottom'),[870,540],[390,540],point(b,'top')],
        'range>sleep':[point(a,'right'),[1130,430],[1130,850],point(b,'right')],
        'body>sleep':[point(a,'right'),[650,645],[650,750],[870,750],point(b,'top')],
        '@run:body>local':[point(a,'left'),[160,850],[160,110],[390,110],point(b,'top')],
        'sleep>local':[point(a,'bottom'),[870,950],[90,950],[90,110],[390,110],point(b,'top')]
      };
      if(canonical)e.points=routes[key]||[point(a,'bottom'),point(b,'top')];
      else if(b.y>a.y&&nodes.indexOf(b)===nodes.indexOf(a)+1)e.points=[point(a,'bottom'),point(b,'top')];
      else {
        const right=e.kind!=='no'&&e.kind!=='bypass',rail=right?720+(i%8)*35:300-(i%8)*30,side=right?'right':'left';
        const start=point(a,side),end=point(b,side);e.points=[start,[rail,start[1]],[rail,end[1]],end];
      }
      // Put labels beside the first segment, away from the node text.
      const [a0,b0]=e.points;
      e.labelAnchor=b0[0]<a0[0]?'end':'start';
      e.labelX=a0[0]+(e.labelAnchor==='end'?-12:12);e.labelY=a0[1]+(b0[1]>a0[1]?28:-12);
    });
    return {width:1180,height,nodes,edges:valid,canonical};
  }
  const api={layout};
  if(typeof module!=='undefined')module.exports=api;
  else globalThis.SchedulerFlowchart=api;
})();
