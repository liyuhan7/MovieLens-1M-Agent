// Run with node tests/test_governance_ui.cjs. Exercise asynchronous ownership in
// the shipped script without starting a database, a worker or an LLM.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
class Element {
  constructor(tag='div'){this.tag=tag;this.children=[];this.value='';this.hidden=false;this.disabled=false;this.ownText='';}
  set textContent(v){this.ownText=String(v??'');this.children=[];}
  get textContent(){return this.ownText+this.children.map(x=>x.textContent).join('');}
  get options(){return this.children;}
  append(...nodes){this.children.push(...nodes);}
  replaceChildren(...nodes){this.ownText='';this.children=[...nodes];}
  createTHead(){const e=new Element();this.append(e);return e;}
  createTBody(){return this.createTHead();}
  insertRow(){return this.createTHead();}
}
const elements=new Map(), pending=new Map(), submittedKeys=[];
const get=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
const storage=new Map();
let failSubmit=true;
const fixture=id=>({run_id:id,status:'RUNNING',stage:'clean-users',attempts:[],input_version:'raw',rule_version:'rules',metric_version:'metrics'});
const response=value=>({ok:true,json:async()=>value});
const context=vm.createContext({document:{getElementById:get,createElement:tag=>new Element(tag)},
  URLSearchParams, crypto:{randomUUID:()=> 'stable-key'},
  sessionStorage:{getItem:k=>storage.get(k)||null,setItem:(k,v)=>storage.set(k,v),removeItem:k=>storage.delete(k)},
  setInterval:()=>0,
  fetch:async(url,options)=>{
    if(url==='/api/runs'&&options?.method==='POST'){
      submittedKeys.push(options.headers['Idempotency-Key']);
      if(failSubmit){failSubmit=false;return {ok:false,status:503,json:async()=>({detail:'offline'})};}
      return response({run_id:'run-C'});
    }
    if(url.startsWith('/api/runs?'))return response({items:[],next_before:null});
    if(url==='/api/runs/run-C')return response(fixture('run-C'));
    return new Promise(resolve=>pending.set(url,resolve));
  }});
vm.runInContext(fs.readFileSync('web/governance.js','utf8'),context);
const flush=()=>new Promise(resolve=>setImmediate(resolve));
const answer=(url,value)=>{assert.ok(pending.has(url),'expected request '+url);pending.get(url)(response(value));pending.delete(url);};
(async()=>{
  await flush();
  const a=vm.runInContext('selectRun("run-A")',context);
  const b=vm.runInContext('selectRun("run-B")',context);
  answer('/api/runs/run-B',fixture('run-B'));await b;
  answer('/api/runs/run-A',fixture('run-A'));await a;
  assert.ok(get('identity').textContent.includes('run-B'));
  assert.ok(!get('identity').textContent.includes('run-A'));

  get('rule').value='U6';
  const old=vm.runInContext('evidence()',context);
  get('rule').value='U7';
  const fresh=vm.runInContext('evidence()',context);
  const item=marker=>({source_table:'users',rule_id:'U7',source_record_id:'source',after:marker});
  answer('/api/runs/run-B/evidence?limit=25&rule_id=U7',{items:[item('fresh')],next_after:null});await fresh;
  answer('/api/runs/run-B/evidence?limit=25&rule_id=U6',{items:[item('stale')],next_after:null});await old;
  assert.ok(get('evidence').textContent.includes('fresh'));
  assert.ok(!get('evidence').textContent.includes('stale'));

  await get('submit').onclick();
  await get('submit').onclick();
  assert.deepEqual(submittedKeys,['stable-key','stable-key']);
  assert.equal(storage.size,0);
  assert.ok(get('identity').textContent.includes('run-C'));
  console.log('PASS: stale task responses, stale evidence filters, idempotent submit retry');
})().catch(e=>{console.error(e);process.exitCode=1;});
