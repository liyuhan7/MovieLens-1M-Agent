"""Bounded full Task Attempt and reducer-partition measurements from JobHistory."""
from concurrent.futures import ThreadPoolExecutor
import re
import time

import httpx

TASK_GROUP = 'org.apache.hadoop.mapreduce.TaskCounter'
FS_GROUP = 'org.apache.hadoop.mapreduce.FileSystemCounter'
SELECTED_COUNTERS = {TASK_GROUP + '/' + name for name in (
    'MAP_INPUT_RECORDS','MAP_OUTPUT_RECORDS','MAP_OUTPUT_BYTES','MAP_OUTPUT_MATERIALIZED_BYTES',
    'REDUCE_INPUT_GROUPS','REDUCE_INPUT_RECORDS','REDUCE_OUTPUT_RECORDS','REDUCE_SHUFFLE_BYTES',
    'SPILLED_RECORDS','CPU_MILLISECONDS','GC_TIME_MILLIS')}
SELECTED_COUNTERS |= {FS_GROUP + '/' + name for name in ('HDFS_BYTES_READ','HDFS_BYTES_WRITTEN',
                                                       'FILE_BYTES_READ','FILE_BYTES_WRITTEN')}


def _fetch(client,url,deadline):
    remaining = deadline-time.monotonic()
    if remaining <= 0:
        raise ValueError('Detailed capture deadline exceeded')
    with client.stream('GET',url,timeout=min(10,remaining)) as response:
        response.raise_for_status()
        body = bytearray()
        for chunk in response.iter_bytes():
            body.extend(chunk)
            if len(body)>1024*1024 or time.monotonic()>deadline:
                raise ValueError('Detailed capture size or time limit exceeded')
    import json
    payload = json.loads(body)
    if not isinstance(payload,dict):
        raise ValueError('Malformed JobHistory response')
    return payload


def _counters(client,url,identity,deadline,attempt=False):
    root = 'jobTaskAttemptCounters' if attempt else 'jobTaskCounters'
    group_key = 'taskAttemptCounterGroup' if attempt else 'taskCounterGroup'
    payload = _fetch(client,url+'/counters',deadline)[root]
    if payload['id'] != identity:
        raise ValueError('Foreign counter identity')
    # Hadoop JSON uses capital C here, despite the spelling in the element table.
    groups = payload.get(group_key,[])
    counters,seen = {},set()
    for group in groups:
        for counter in group['counter']:
            key = group['counterGroupName']+'/'+counter['name']
            value = counter['value']
            if key in seen or type(value) is not int or value<0:
                raise ValueError('Invalid task counter')
            seen.add(key)
            if key in SELECTED_COUNTERS:
                counters[key] = value
    return counters


def _task_detail(client,url,task,deadline):
    task_url = url+'/tasks/'+task['id']
    container = _fetch(client,task_url+'/attempts',deadline).get('taskAttempts') or {}
    if not isinstance(container,dict):
        raise ValueError('Malformed attempt container')
    attempts = container.get('taskAttempt',[])
    if not isinstance(attempts,list) or not 1 <= len(attempts) <= 16:
        raise ValueError('Missing or oversized task attempts')
    identities,successes,details = set(),set(),[]
    for attempt in attempts:
        identity = attempt['id']
        if (not re.fullmatch('attempt_'+re.escape(task['id'].removeprefix('task_'))+'_[0-9]+',identity) or
                identity in identities or attempt['type']!=task['type'] or
                attempt['state'] not in {'SUCCEEDED','FAILED','KILLED'}):
            raise ValueError('Foreign, repeated or nonterminal task attempt')
        identities.add(identity)
        start,end,elapsed = (attempt[k] for k in ('startTime','finishTime','elapsedTime'))
        if (any(type(v) is not int for v in (start,end,elapsed)) or not 0 <= start <= end or
                elapsed!=end-start):
            raise ValueError('Invalid attempt timing')
        item = {k:attempt[k] for k in ('id','type','state','startTime','finishTime','elapsedTime')}
        if task['type']=='REDUCE':
            phases = [attempt.get(k) for k in ('elapsedShuffleTime','elapsedMergeTime','elapsedReduceTime')]
            if any(type(v) is not int or v<0 for v in phases) or sum(phases)>elapsed:
                raise ValueError('Invalid reduce attempt phases')
            item.update(zip(('shuffle_ms','merge_ms','reduce_ms'),phases))
        if attempt['state']=='SUCCEEDED':
            successes.add(identity)
        item['counters'] = _counters(client,task_url+'/attempts/'+identity,identity,deadline,attempt=True)
        details.append(item)
    winner = task.get('successfulAttempt')
    if (task['state']=='SUCCEEDED' and (not winner or winner not in successes) or
            task['state']!='SUCCEEDED' and winner):
        raise ValueError('Successful attempt differs from task identity')
    return {'task_id':task['id'],'type':task['type'],'state':task['state'],'successful_attempt':winner,
            'duration_ms':task['elapsedTime'],'counters':_counters(client,task_url,task['id'],deadline),
            'attempts':sorted(details,key=lambda a:(a['startTime'],a['id']))}


def capture_task_details(client,url,tasks):
    """No sampling: every selected job task succeeds or this capture is incomplete."""
    try:
        if len(tasks)>256:
            raise ValueError('Detailed capture supports at most 256 tasks per job')
        deadline = time.monotonic()+60
        with ThreadPoolExecutor(max_workers=4) as pool:
            details = list(pool.map(lambda task:_task_detail(client,url,task,deadline),tasks))
        attempts = [a for t in details for a in t['attempts']]
        partitions = []
        for task in details:
            if task['type']=='REDUCE':
                value = task['counters'].get(TASK_GROUP+'/REDUCE_INPUT_RECORDS')
                partitions.append({'task_id':task['task_id'],'input_records':value,
                    'input_groups':task['counters'].get(TASK_GROUP+'/REDUCE_INPUT_GROUPS'),
                    'shuffle_bytes':task['counters'].get(TASK_GROUP+'/REDUCE_SHUFFLE_BYTES'),
                    'duration_ms':task['duration_ms']})
        values = [p['input_records'] for p in partitions if p['input_records'] is not None]
        reconciled = len(values)==len(partitions)
        positive = sorted(v for v in values if v>0)
        middle = ((positive[(len(positive)-1)//2]+positive[len(positive)//2])/2) if positive else None
        return {'available':True,'tasks':details,'attempt_counts':{
                    state:sum(a['state']==state for a in attempts) for state in ('SUCCEEDED','FAILED','KILLED')},
                'nonwinning_attempt_duration_ms':sum(a['elapsedTime'] for t in details for a in t['attempts']
                                                     if a['id']!=t['successful_attempt']),
                'reduce_partitions':partitions,'partition_counts_complete':reconciled,
                'partition_input_records':{'total':sum(values) if reconciled else None,
                    'empty_partitions':sum(v==0 for v in values) if reconciled else None,
                    'max_over_positive_median':max(values)/middle if reconciled and middle else None},
                'definitions':['Reducer Task 是该作业的逻辑分区；计数器不证明输出文件的业务记录数量。',
                               '非获胜 Attempt 耗时之和可能重叠，不是任务的额外墙钟耗时。']}
    except (httpx.HTTPError,KeyError,TypeError,ValueError) as error:
        return {'available':False,'tasks':None,'reduce_partitions':None,
                'reason':type(error).__name__+': detailed task capture unavailable or inconsistent'}
