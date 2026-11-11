// Generates a compact, inspectable UI report; does not fit operation costs to A/B results.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const E=require('./lab-engine.js');
function analyze(d,previous,source){
  if(d.schema!=='scheduler-local-measurement-v1'||d.protocol.instrumented)throw Error('Unsupported measurement');
  const body={100:(d.serial_cases.balanced64_100.median-d.serial_cases.loop64.median)/64,
              1000:(d.serial_cases.balanced64_1000.median-d.serial_cases.loop64.median)/64};
  if(Object.values(body).some(v=>!Number.isFinite(v)||v<=0)||d.primitives_ns.sc_store_acquire_load.median<=.001)throw Error('Invalid calibration/probe');
  const proxies={publish:d.primitives_ns.push_front_hot.median,take:d.primitives_ns.pop_front_hot.median,
    steal:d.primitives_ns.pop_back_uncontended_same_thread.median,reserve:d.primitives_ns.sc_store_acquire_load.median};
  const cases=d.jobs[0].data.cases,rows=[],ablation=[];
  const threadCounts=d.protocol.threads||[...new Set(d.measurements.map(r=>r.workers))].sort((a,b)=>a-b);
  const value=(workers,name,mode)=>d.measurements.find(r=>r.workers===workers&&r.case===name&&r.mode===mode).ns.median;
  for(const workers of threadCounts)for(const c of cases){
    if(!c.work)continue;
    const shape=['balanced','tail','cluster','single'][c.pattern],unit=body[c.work];
    const config=E.preset('lazy');config.workers=workers;config.jobs[0]={id:'J1',arrival:0,n:c.n,unit,pattern:shape,after:[]};Object.assign(config.costs,proxies);
    const quotient=Math.floor(c.n/workers),remainder=c.n%workers;
    const boundary=w=>w*quotient+Math.min(w,remainder);
    const costs=E.workload(config.jobs[0],1),loads=Array.from({length:workers},(_,w)=>costs.slice(boundary(w),boundary(w+1)).reduce((a,b)=>a+b,0));
    const predictedStatic=value(workers,'loop'+c.n,'RAPID_START')+Math.max(...loads);
    const actualStatic=value(workers,c.case,'RAPID_START'),actualRapid=value(workers,c.case,'RAPID_AUTO');
    const simulation=E.simulate(config,{record:false});
    rows.push({workers,case:c.case,shape,n:c.n,unit_ns:unit,static_prediction_ns:predictedStatic,static_actual_ns:actualStatic,
      static_error:(predictedStatic/actualStatic-1),event_prediction_ns:simulation.ok?simulation.time:null,event_actual_ns:actualRapid,
      event_error:simulation.ok?simulation.time/actualRapid-1:null,simulation_error:simulation.ok?null:simulation.errors.join('; '),holdout:c.case!=='balanced64_100'&&c.case!=='balanced64_1000'});
    if(c.case==='tail64_1000'&&workers<=32){
      const disabled=JSON.parse(JSON.stringify(config));disabled.nodes.find(n=>n.op==='tail').enabled=false;
      const b=E.simulate(disabled);
      ablation.push({workers,on_ns:simulation.ok?simulation.time:null,off_ns:b.ok?b.time:null,tail_steals:simulation.ok?simulation.count.tail:null,
        simulation_error:simulation.ok&&b.ok?null:[...(simulation.errors||[]),...(b.errors||[])].join('; '),
        label:'Model ablation with proxy inputs; not a native tail-disabled build'});
    }
  }
  const checks={};for(const workers of threadCounts){const subset=rows.filter(r=>r.workers===workers&&r.holdout),events=subset.filter(r=>r.event_error!==null);checks[workers]={n:subset.length,event_n:events.length,
    static_holdout_mape:subset.reduce((s,r)=>s+Math.abs(r.static_error),0)/subset.length,
    event_holdout_mape:events.length?events.reduce((s,r)=>s+Math.abs(r.event_error),0)/events.length:null};}
  const comparable=previous&&(d.protocol.kernel||'cpu_relax')===(previous.protocol.kernel||'cpu_relax')&&(d.protocol.suite_version||1)===(previous.protocol.suite_version||1)&&d.protocol.grain===previous.protocol.grain;
  const sameBinaries=!!comparable&&JSON.stringify(d.binaries)===JSON.stringify(previous.binaries);
  const paired=d.paired.map(r=>({...r,previous_ratio:comparable?previous.paired.find(p=>p.workers===r.workers&&p.case===r.case&&p.reference===r.reference)?.ratio.median??null:null}));
  return {schema:'scheduler-lab-measured-report-v1',recorded_at:d.recorded_at,hardware:d.hardware,protocol:d.protocol,
    source,source_sha256:crypto.createHash('sha256').update(JSON.stringify(d)).digest('hex'),
    primitive_status:'Measured hot/uncontended batch throughput. Not cross-thread latency.',
    primitives_ns:d.primitives_ns,body_ns:body,proxy_costs:proxies,
    remaining_assumptions:['rapid','split','tail','migration','wait','join'],
    measurements:d.measurements,paired,model_checks:rows,holdout:checks,ablation,replication_same_binaries:sameBinaries,previous_protocol_compatible:!!comparable,
    caveats:['No per-thread pinning; placement and background load affect results. See recorded CPU set and environment.',
      'Synthetic '+(d.protocol.kernel||'cpu_relax')+' work, not SpMV; warm startup only; full resident groups.',
      'A/B changes entire implementations. It does not identify the causal effect of just tail stealing.',
      'An earlier probe was optimized away; its atomic result is excluded. Only matching binary hashes qualify as same-build replication.',
      'IQR is dispersion of paired process means, not a confidence interval.']};
}
if(require.main===module){const input=process.argv[2],previous=process.argv[3]&&!process.argv[3].startsWith('--')?JSON.parse(fs.readFileSync(process.argv[3],'utf8')):null;
  const report=analyze(JSON.parse(fs.readFileSync(input,'utf8')),previous,input);
  if(process.argv.includes('--publish'))fs.writeFileSync(path.join(__dirname,'local-measurements.js'),"'use strict';\nglobalThis.SchedulerMeasurements = "+JSON.stringify(report).replaceAll('<','\\u003c')+';\n');
  process.stdout.write(JSON.stringify(report,null,2)+'\n');
}
module.exports={analyze};
