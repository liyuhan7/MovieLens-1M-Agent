"""Version-bound JobHistory measurements, preserving missing values explicitly."""
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import os
import re
import json
import math
import uuid

import httpx

from agent.published import ReportUnavailable
from metadata.store import Store, fingerprint
from metadata.connection import ROOT

EXPECTED_STAGES = {'extractUsers','extractMovies','cleanUsers','cleanMovies','cleanMoviesTitle','cleanRatings',
                   'score-raw-users','score-raw-movies','score-raw-ratings',
                   'score-clean-users','score-clean-movies','score-clean-ratings'}


def _distribution(values):
    if not values:
        return {'count':0,'min_ms':None,'median_ms':None,'p95_ms':None,'max_ms':None}
    values = sorted(values)
    n = len(values)
    return {'count':n,'min_ms':values[0], 'median_ms':(values[(n-1)//2]+values[n//2])/2,
            'p95_ms':values[math.ceil(n*.95)-1],'max_ms':values[-1]}


def _task_measurements(client, url, job_id, detail, *, include_inventory=False):
    try:
        totals = {kind:detail[key] for kind,key in [('MAP','mapsTotal'),('REDUCE','reducesTotal')]}
        if any(type(v) is not int or not 0 <= v <= 100000 for v in totals.values()):
            raise ValueError('Invalid or oversized task inventory')
        with client.stream('GET',url + '/tasks') as response:
            response.raise_for_status()
            body = bytearray()
            for chunk in response.iter_bytes():
                body.extend(chunk)
                if len(body) > 32 * 1024 * 1024:
                    raise ValueError('Task inventory exceeds capture limit')
        payload = json.loads(body)
        if not isinstance(payload,dict):
            raise ValueError('Invalid task response')
        container = payload.get('tasks') or {}
        if not isinstance(container,dict):
            raise ValueError('Invalid tasks container')
        tasks = container.get('task',[])
        if not isinstance(tasks,list) or len(tasks) != sum(totals.values()):
            raise ValueError('Incomplete task inventory')
        values,identities = {'MAP':[],'REDUCE':[]},set()
        job_suffix = job_id.removeprefix('job_')
        for task in tasks:
            identity,kind = task['id'],task['type']
            if (kind not in values or not re.fullmatch('task_' + re.escape(job_suffix) +
                    ('_m_' if kind=='MAP' else '_r_') + '[0-9]+',identity) or identity in identities or
                    task['state'] not in {'SUCCEEDED','FAILED','KILLED'} or
                    (detail['state']=='SUCCEEDED' and task['state']!='SUCCEEDED')):
                raise ValueError('Invalid task identity or terminal state')
            identities.add(identity)
            start,end,elapsed = task['startTime'],task['finishTime'],task['elapsedTime']
            if (any(type(v) is not int for v in (start,end,elapsed)) or
                    not 0 <= start <= end or elapsed != end-start):
                raise ValueError('Invalid task timing')
            values[kind].append(elapsed)
        if any(len(values[k]) != totals[k] for k in totals):
            raise ValueError('Task type counts differ from job')
        return {'available':True,'distributions':{k:_distribution(v) for k,v in values.items()},
                **({'inventory':tasks} if include_inventory else {}),
                'definition':'终态 Task 耗时分布，包含该 Task 的重试等待；不是单次 Attempt 耗时。'}
    except (httpx.HTTPError,KeyError,TypeError,ValueError) as error:
        return {'available':False,'distributions':None,'reason':type(error).__name__ + ': task capture unavailable or inconsistent'}


def measure_job(job, base_url, *, client=None, task_details=False):
    result = {k:job[k] for k in ('stage','job_id','submission_id','status')}
    result.update(available=False, duration_ms=None, counters=None)
    if not re.fullmatch(r'job_[0-9]+_[0-9]+',job.get('job_id') or ''):
        return {**result,'reason':'Job identity not yet available'}
    owned = client is None
    client = client or httpx.Client(timeout=10,trust_env=False)
    try:
        url = base_url.rstrip('/') + '/ws/v1/history/mapreduce/jobs/' + job['job_id']
        response = client.get(url)
        response.raise_for_status()
        detail = response.json()['job']
        if detail['id'] != job['job_id'] or detail['name'] != job['submission_id']:
            raise ValueError('JobHistory identity mismatch')
        if detail['state'] != job['status'] or detail['state'] not in {'SUCCEEDED','FAILED','KILLED'}:
            raise ValueError('JobHistory terminal state differs from metadata')
        start,finish = detail['startTime'],detail['finishTime']
        if type(start) is not int or type(finish) is not int or not 0 <= start <= finish:
            raise ValueError('JobHistory timing is invalid')
        response = client.get(url + '/counters')
        response.raise_for_status()
        payload = response.json()['jobCounters']
        if payload['id'] != job['job_id']:
            raise ValueError('Counter identity mismatch')
        counters = {}
        for group in payload.get('counterGroup',[]):
            for counter in group['counter']:
                key = group['counterGroupName'] + '/' + counter['name']
                if key in counters or type(counter['totalCounterValue']) is not int or counter['totalCounterValue'] < 0:
                    raise ValueError('Invalid JobHistory counter')
                counters[key] = counter['totalCounterValue']
        tasks = _task_measurements(client,url,job['job_id'],detail,include_inventory=task_details)
        detailed = None
        if task_details:
            from governance.task_metrics import capture_task_details
            detailed = (capture_task_details(client,url,tasks.pop('inventory')) if tasks['available'] else
                        {'available':False,'reason':'Task inventory unavailable'})
            if detailed['available']:
                for kind,label in (('MAP','Map'),('REDUCE','Reduce')):
                    for state,prefix in (('SUCCEEDED','successful'),('FAILED','failed'),('KILLED','killed')):
                        key = prefix+label+'Attempts'
                        actual = sum(a['state']==state for t in detailed['tasks'] if t['type']==kind for a in t['attempts'])
                        if key in detail and detail[key]!=actual:
                            detailed = {'available':False,'reason':'Attempt counts differ from JobHistory job summary'}
                            break
                    if not detailed['available']:
                        break
                if detailed['available'] and detailed['partition_counts_complete']:
                    key = 'org.apache.hadoop.mapreduce.TaskCounter/REDUCE_INPUT_RECORDS'
                    if key in counters and detailed['partition_input_records']['total']!=counters[key]:
                        detailed = {'available':False,'reason':'Partition counts differ from job counters'}
        return {**result,'available':True,'duration_ms':finish-start,'counters':counters,
                'tasks':tasks,'task_details':detailed,
                'maps':detail.get('mapsTotal'),'reduces':detail.get('reducesTotal'),
                'failed_map_attempts':detail.get('failedMapAttempts'),
                'failed_reduce_attempts':detail.get('failedReduceAttempts'),
                'avg_shuffle_ms':detail.get('avgShuffleTime'), 'avg_merge_ms':detail.get('avgMergeTime')}
    except (httpx.HTTPError,KeyError,TypeError,ValueError) as error:
        return {**result,'reason':type(error).__name__ + ': JobHistory measurement unavailable or inconsistent'}
    finally:
        if owned:
            client.close()


def performance_baseline(run_id, *, store=None, base_url=None, task_details=False):
    store = store or Store()
    run = store.get_run(run_id)
    if not run:
        raise ReportUnavailable('run not found',404)
    pub = store.get_publish(run_id)
    attempt = pub['attempt_id'] if pub and pub['status'] == 'PUBLISHED' else run['active_attempt']
    if not attempt:
        raise ReportUnavailable('run has no execution Attempt',409)
    jobs = store.list_jobs(attempt)
    if len(jobs) > 12:
        raise ReportUnavailable('unexpected execution job set',503)
    url = base_url or os.environ.get('ML_JOBHISTORY_URL','http://127.0.0.1:19888')
    with ThreadPoolExecutor(max_workers=4) as pool:
        measurements = list(pool.map(lambda job:measure_job(job,url,task_details=task_details),jobs))
    return {'schema_version':'performance-baseline-v2','run_id':run_id,'attempt_id':attempt,
            'dataset_id':run['dataset_id'],
            'captured_at':dt.datetime.now(dt.timezone.utc).isoformat(),
            'versions':{k:run[k] for k in ('input_version','rule_version','metric_version')},
            'execution_hash':fingerprint(run['request'].get('execution')),
            'execution':run['request'].get('execution'),
            'details_requested':task_details,'details_complete':(all(j.get('task_details',{}).get('available',False) and
                j['task_details'].get('partition_counts_complete',False)
                for j in measurements) and len(jobs)==12) if task_details else None,
            'jobs':measurements,'complete':len(jobs)==12 and {j['stage'] for j in jobs}==EXPECTED_STAGES and
                 all(j['available'] and j['status']=='SUCCEEDED' and j['tasks']['available'] for j in measurements),
            'sum_job_duration_ms':sum(j['duration_ms'] for j in measurements) if measurements and all(j['available'] for j in measurements) else None,
            'duration_note':'作业耗时之和不是端到端耗时；缺失采集值不按零处理。'}


def capture_baseline(run_id, *, store=None, base_url=None, task_details=False):
    baseline = performance_baseline(run_id,store=store,base_url=base_url,task_details=task_details)
    identity = uuid.uuid4().hex
    directory = ROOT / 'outputs/performance-baselines'
    if directory.resolve()!=directory:
        raise ValueError('Redirected performance workspace')
    directory.mkdir(parents=True,exist_ok=True)
    envelope = {'baseline_id':identity,'payload':baseline,'sha256':fingerprint(baseline)}
    content = json.dumps(envelope,ensure_ascii=False,indent=2,allow_nan=False)
    if len(content.encode('utf-8'))>64*1024*1024:
        raise ValueError('Performance baseline exceeds storage limit')
    with (directory / (identity + '.json')).open('x',encoding='utf-8') as target:
        target.write(content)
    return {'baseline_id':identity,'sha256':envelope['sha256'],'complete':baseline['complete']}


def load_baseline(identity):
    if not isinstance(identity,str) or not re.fullmatch('[a-f0-9]{32}',identity):
        raise ValueError('Invalid baseline identity')
    try:
        source = ROOT / 'outputs/performance-baselines' / (identity + '.json')
        if source.resolve()!=source or source.stat().st_size > 64 * 1024 * 1024:
            raise ValueError('Oversized baseline')
        envelope = json.loads(source.read_text(encoding='utf-8'))
        payload = envelope['payload']
        if (envelope['baseline_id'] != identity or fingerprint(payload) != envelope['sha256'] or
                payload['schema_version'] != 'performance-baseline-v2' or
                fingerprint(payload['execution']) != payload['execution_hash']):
            raise ValueError('Baseline integrity or schema mismatch')
        if type(payload['complete']) is not bool or not isinstance(payload['jobs'],list):
            raise ValueError('Malformed baseline completeness')
        if payload['complete']:
            jobs = payload['jobs']
            if (len(jobs)!=12 or {j['stage'] for j in jobs}!=EXPECTED_STAGES or
                    len({j['job_id'] for j in jobs})!=12 or
                    any(j['available'] is not True or j['status']!='SUCCEEDED' or
                        j['tasks']['available'] is not True or type(j['duration_ms']) is not int or
                        j['duration_ms']<0 for j in jobs)):
                raise ValueError('Baseline completeness differs from its measurements')
        return payload
    except (OSError,KeyError,TypeError,json.JSONDecodeError) as error:
        raise ValueError('Baseline unavailable or malformed') from error


def compare_baselines(left_id, right_id):
    left,right = load_baseline(left_id),load_baseline(right_id)
    if not left['complete'] or not right['complete']:
        raise ValueError('Incomplete baselines cannot produce performance deltas')
    if left['versions'] != right['versions'] or left['dataset_id'] != right['dataset_id']:
        raise ValueError('Performance comparison requires identical input, rule and metric versions')
    left_jobs,right_jobs = ({j['stage']:j for j in item['jobs']} for item in (left,right))
    if len(left['jobs'])!=12 or len(right['jobs'])!=12 or left_jobs.keys()!=EXPECTED_STAGES or right_jobs.keys()!=EXPECTED_STAGES:
        raise ValueError('Baseline job stages differ')
    deltas = []
    for stage,first in left_jobs.items():
        second = right_jobs[stage]
        before,after = first['duration_ms'],second['duration_ms']
        if any(type(v) is not int or v < 0 for v in (before,after)):
            raise ValueError('Baseline duration is invalid')
        deltas.append({'stage':stage,'before_ms':before,'after_ms':after,'delta_ms':after-before,
                       'change_percent':(after-before)*100/before if before else None,
                       'before_tasks':first['tasks'],'after_tasks':second['tasks']})
    return {'left_baseline':left_id,'right_baseline':right_id,'versions':left['versions'],'stages':deltas,
            'execution_changed':left['execution_hash']!=right['execution_hash'],
            'details_complete':{'left':left.get('details_complete'),'right':right.get('details_complete')},
            'limitations':['两次采样不足以证明稳定的性能提升；集群负载和缓存可能影响耗时。',
                           '输入和规则版本相同不证明输出语义相同；优化验收仍需独立比对产物。']}
