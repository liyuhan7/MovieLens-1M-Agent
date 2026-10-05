"""Exact disk-backed semantic comparison of two formally published results."""
import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3
import uuid

import pyarrow.parquet as pq

from agent.published import ReportUnavailable, read_published_report
from agent.results import artifact_file, publication_context
from governance.artifacts import SCHEMAS
from governance.manifest import required_files
from metadata.connection import ROOT
from metadata.store import Store, fingerprint

VERSIONS = ('input_version','rule_version','metric_version')
ROW_IDENTITIES = {'run_id','attempt_id','evidence_id'}
REPORT_OPERATIONS = {'task_id','run_id','attempt_id','output_data_version','created_at',
                     'stages','jobs','execution','execution_mode'}


def _schema_name(relative):
    if relative == 'quality/summary.parquet':
        return 'quality_summary'
    if relative == 'quality/groups.parquet':
        return 'quality_groups'
    family,table,_ = relative.split('/',2)
    return {'cleaned':table,'quarantine':'quarantine','dispositions':'disposition',
            'evidence':'evidence','quality':'quality_detail'}[family]


def _body(row):
    # Preserve nulls, list order, source IDs, event order and every business value.
    return json.dumps({k:v for k,v in row.items() if k not in ROW_IDENTITIES},
                      ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def _ingest(database, run, pub, cache, side, hdfs):
    items = pub['manifest']['files']
    paths = [item['path'] for item in items]
    if len(paths) != len(set(paths)) or set(paths) != required_files():
        raise ReportUnavailable('unsupported or incomplete semantic artifact inventory',503)
    counts = {}
    for item in items:
        relative = item['path']
        if relative == 'report.json':
            continue
        schema = SCHEMAS[_schema_name(relative)]
        path = artifact_file(pub,cache,relative,hdfs=hdfs)
        parquet = pq.ParquetFile(path)
        if (item.get('format') != 'parquet' or
                item.get('schema_sha256') != hashlib.sha256(schema.serialize().to_pybytes()).hexdigest() or
                not parquet.schema_arrow.equals(schema,check_metadata=False) or
                type(item.get('rows')) is not int or parquet.metadata.num_rows != item['rows']):
            raise ReportUnavailable('semantic comparison found incompatible Parquet metadata',503)
        total = 0
        for batch in parquet.iter_batches(batch_size=5000):
            records = []
            for row in batch.to_pylist():
                if (row['run_id'] != run['run_id'] or row['attempt_id'] != pub['attempt_id'] or
                        any(row[k] != run[k] for k in VERSIONS)):
                    raise ReportUnavailable('semantic comparison found foreign result rows',503)
                if relative.startswith('evidence/'):
                    if row['kind'] == 'TRANSFORMATION':
                        suffix = ['event',row['event_order']]
                    elif row['kind'] == 'QUALITY':
                        suffix = [row['phase'],row['metric']]
                    else:
                        raise ReportUnavailable('semantic comparison found unknown evidence kind',503)
                    expected = fingerprint([row['run_id'],row['attempt_id'],row['source_record_id'],*suffix])
                    if row['evidence_id'] != expected:
                        raise ReportUnavailable('semantic comparison found invalid evidence identity',503)
                records.append((relative,_body(row),side))
            database.executemany('INSERT INTO rows VALUES (?,?,?,1) ON CONFLICT(family,body,side) '
                                 'DO UPDATE SET n=n+1',records)
            total += len(records)
        if total != item['rows']:
            raise ReportUnavailable('semantic comparison read count differs from manifest',503)
        counts[relative] = total
        database.commit()
    return counts


def compare_semantics(left_run, right_run, *, store=None, root=ROOT, hdfs=None):
    """Equality is exact row-multiset equality, never a score-only or sample gate."""
    if left_run == right_run:
        raise ValueError('Semantic comparison requires two distinct formal Run identities')
    store = store or Store()
    root = Path(root).resolve()
    contexts = [publication_context(identity,store,root) for identity in (left_run,right_run)]
    left,right = (c[0] for c in contexts)
    if left['dataset_id'] != right['dataset_id'] or any(left[k] != right[k] for k in VERSIONS):
        raise ReportUnavailable('semantic equivalence requires the same dataset, input, rule and metric versions',409)
    reports = []
    for identity,(_,pub,_) in zip((left_run,right_run),contexts):
        _,report,bound = read_published_report(identity,store=store,root=root)
        if bound['publish_id'] != pub['publish_id'] or bound['manifest_hash'] != pub['manifest_hash']:
            raise ReportUnavailable('formal publication changed during semantic comparison',503)
        reports.append({k:v for k,v in report.items() if k not in REPORT_OPERATIONS})
    directory = root / 'outputs/semantic-comparisons' / uuid.uuid4().hex
    if directory.resolve() != directory:
        raise ReportUnavailable('redirected semantic comparison workspace',503)
    directory.mkdir(parents=True,exist_ok=False)
    database = sqlite3.connect(directory / 'rows.sqlite')
    try:
        database.execute('CREATE TABLE rows (family TEXT, body TEXT, side INTEGER, n INTEGER NOT NULL, '
                         'PRIMARY KEY(family,body,side)) WITHOUT ROWID')
        counts = [_ingest(database,run,pub,cache,side,hdfs)
                  for side,(run,pub,cache) in enumerate(contexts)]
        groups = []
        for family in sorted(counts[0]):
            # Exact canonical bodies are keys, rather than truncated/checksum keys.
            differences = database.execute('SELECT COUNT(*) FROM (SELECT body FROM rows WHERE family=? '
                'GROUP BY body HAVING SUM(CASE side WHEN 0 THEN n ELSE -n END) != 0)',(family,)).fetchone()[0]
            groups.append({'artifact':family,'left_rows':counts[0][family],'right_rows':counts[1][family],
                           'different_row_values':differences,'equivalent':differences==0})
        report_keys = sorted(set(reports[0]) | set(reports[1]))
        different_fields = [k for k in report_keys if k not in reports[0] or k not in reports[1] or
                            fingerprint(reports[0][k]) != fingerprint(reports[1][k])]
        result = {'schema_version':'semantic-comparison-v1','captured_at':dt.datetime.now(dt.timezone.utc).isoformat(),
            'left':{**{k:contexts[0][1][k] for k in ('run_id','publish_id','attempt_id','manifest_hash')},
                    'execution_hash':fingerprint(contexts[0][1]['manifest'].get('execution'))},
            'right':{**{k:contexts[1][1][k] for k in ('run_id','publish_id','attempt_id','manifest_hash')},
                     'execution_hash':fingerprint(contexts[1][1]['manifest'].get('execution'))},
            'dataset_id':left['dataset_id'],'versions':{k:left[k] for k in VERSIONS},'artifacts':groups,
            'different_report_fields':different_fields,
            'equivalent':not different_fields and all(g['equivalent'] for g in groups),
            'ignored_row_fields':sorted(ROW_IDENTITIES),'ignored_report_fields':sorted(REPORT_OPERATIONS),
            'definition':'全量业务行的精确多重集合比较；保留重复次数、来源、动作顺序和数组顺序。',
            'limitations':['结果等价不证明任一版本的规则本身正确，也不证明稳定的性能收益。']}
        # Recheck the authoritative identities before exposing the completed result.
        for identity,(_,pub,_) in zip((left_run,right_run),contexts):
            _,current,_ = publication_context(identity,store,root)
            if (current['publish_id'],current['manifest_hash']) != (pub['publish_id'],pub['manifest_hash']):
                raise ReportUnavailable('formal publication changed during semantic comparison',503)
        (directory/'comparison.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        return {**result,'record':str(directory/'comparison.json')}
    finally:
        database.close()


def compare_optimization(left_id,right_id, *, store=None, root=ROOT, hdfs=None):
    from governance.performance import compare_baselines, load_baseline
    measured = compare_baselines(left_id,right_id)
    left,right = load_baseline(left_id),load_baseline(right_id)
    semantic = compare_semantics(left['run_id'],right['run_id'],store=store,root=root,hdfs=hdfs)
    if (semantic['left']['attempt_id'] != left['attempt_id'] or
            semantic['right']['attempt_id'] != right['attempt_id'] or semantic['versions'] != left['versions'] or
            semantic['left']['execution_hash'] != left['execution_hash'] or
            semantic['right']['execution_hash'] != right['execution_hash']):
        raise ReportUnavailable('performance baselines are not bound to the compared formal Attempts',409)
    return {'performance':measured,'semantics':semantic,'semantics_preserved':semantic['equivalent'],
            'optimization_accepted':False,
            'acceptance_note':'语义等价是优化的必要门槛；稳定性能收益仍需多次同环境实测验收。'}
