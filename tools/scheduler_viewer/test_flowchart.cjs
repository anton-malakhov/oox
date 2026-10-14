const assert=require('node:assert/strict');
const {layout}=require('./flowchart.js'),{preset}=require('./lab-engine.js');
function check(config){
  const graph=layout(config),ids=new Set(graph.nodes.map(n=>n.id));
  assert.equal(ids.size,graph.nodes.length);
  assert.equal(graph.nodes.find(n=>n.id==='@start').kind,'terminal');
  for(const e of graph.edges){
    assert(ids.has(e.from)&&ids.has(e.to));
    for(let i=1;i<e.points.length;i++)assert(e.points[i][0]===e.points[i-1][0]||e.points[i][1]===e.points[i-1][1],e.from+'>'+e.to);
  }
  for(const a of graph.nodes){
    assert(a.x>=0&&a.y>=0&&a.x+a.w<=graph.width&&a.y+a.h<=graph.height);
    for(const b of graph.nodes)if(a!==b)assert(a.x+a.w<=b.x||b.x+b.w<=a.x||a.y+a.h<=b.y||b.y+b.h<=a.y,`${a.id}/${b.id}`);
  }
  for(const n of config.nodes){
    const exits=graph.edges.filter(e=>e.from===n.id);
    if(!n.enabled)assert.deepEqual(exits.map(e=>e.to),[n.no]);
    else if(n.op==='wait')assert.deepEqual(exits.map(e=>e.to),[n.yes]);
    else if(n.op==='stop')assert.equal(exits.length,0);
    else if(n.op==='compute'){
      assert.equal(graph.nodes.find(x=>x.id===n.id).kind,'decision');
      assert.equal(graph.nodes.find(x=>x.id==='@run:'+n.id).kind,'process');
      assert(exits.some(e=>e.to===n.no));
    }else assert.equal(graph.nodes.find(x=>x.id===n.id).kind,'decision');
  }
  return graph;
}
for(const name of ['lazy','static','stealing','sharing']){
  const c=preset(name);assert(check(c).canonical);
  c.entry='queue';assert(!check(c).canonical);
  c.nodes.find(n=>n.id==='sleep').op='stop';check(c);
  c.nodes.push({id:'custom',op:'wait',yes:'local',no:'local',enabled:true});check(c);
}
console.log('PASS: real diamonds/actions, branch semantics, disabled bypass, orthogonal edges, no node overlap');
