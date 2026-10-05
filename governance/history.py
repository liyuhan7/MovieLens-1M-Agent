"""Hive projections and bounded history queries over formally published Parquet."""
import csv
import hashlib
import io
import math
import os
from pathlib import PurePosixPath
import re
import subprocess
import uuid
from urllib.parse import urlsplit
import xml.etree.ElementTree as ET

import pyarrow as pa

from agent.published import ReportUnavailable, read_published_report
from agent.results import publication_context
from governance.artifacts import SCHEMAS
from metadata.connection import ROOT
from metadata.store import Store
from storage.hdfs import Hdfs


class HistoryUnavailable(RuntimeError):
    pass


def literal(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_./=-]+',value) or '..' in value:
        raise ValueError('Invalid history identity or location')
    return "'" + value + "'"


def hive_type(t):
    if pa.types.is_string(t):
        return 'STRING'
    if pa.types.is_int8(t):
        return 'TINYINT'
    if pa.types.is_int32(t):
        return 'INT'
    if pa.types.is_int64(t):
        return 'BIGINT'
    if pa.types.is_list(t):
        return 'ARRAY<' + hive_type(t.value_type) + '>'
    raise ValueError('Unsupported Hive projection type')


def projection(pub, *, database='ml_history'):
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,63}',database):
        raise ValueError('Invalid history database')
    statements = ['CREATE DATABASE IF NOT EXISTS `' + database + '`']
    expected = {}
    tables = {}
    output_version = pub.get('output_version')
    literal(output_version)
    layouts = [(t,'cleaned/'+t+'/',t,t,None) for t in ('users','movies','ratings')]
    layouts += [('evidence_'+t,'evidence/'+t+'/','evidence',t,None) for t in ('users','movies','ratings')]
    layouts += [('quality_'+phase,'quality/'+phase+'/','quality_detail',None,phase) for phase in ('before','after')]
    for table,prefix,schema_name,source_table,phase in layouts:
        files = [item for item in pub['manifest']['files'] if item['path'].startswith(prefix)]
        schema = SCHEMAS[schema_name]
        schema_hash = hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
        if not files or any(PurePosixPath(f['path']).parent.as_posix() != prefix.rstrip('/') or
                            not f['path'].endswith('.parquet') or f.get('format') != 'parquet' or
                            f.get('schema_sha256') != schema_hash or type(f.get('rows')) is not int or f['rows'] < 0
                            for f in files):
            raise ReportUnavailable('published inventory is incompatible with Hive',503)
        if phase and {PurePosixPath(f['path']).name for f in files}!={'users.parquet','movies.parquet','ratings.parquet'}:
            raise ReportUnavailable('published quality-detail inventory is incomplete',503)
        expected[table] = sum(f['rows'] for f in files)
        table_name = 'ml_cleaned_'+table if schema_name in ('users','movies','ratings') else 'ml_'+table
        name = f'`{database}`.`{table_name}`'
        columns = ', '.join('`' + f.name + '` ' + hive_type(f.type) for f in schema)
        statements.append(f'CREATE EXTERNAL TABLE IF NOT EXISTS {name} ({columns}) '
                          "PARTITIONED BY (`output_version` STRING) STORED AS PARQUET "
                          f"LOCATION '/ml/hive-projections/{database}/{table}' "
                          "TBLPROPERTIES ('external.table.purge'='false', 'parquet.column.index.access'='false')")
        location = Hdfs.path(pub['storage_path'] + '/' + prefix.rstrip('/'))
        partition_sql = (f'ALTER TABLE {name} ADD IF NOT EXISTS PARTITION '
                         f"(output_version={literal(output_version)}) LOCATION {literal(location)}")
        tables[table] = {'name':name,'create_sql':statements[-1], 'partition_sql':partition_sql,
                         'location':location,'files':files,'schema':schema_name,'source_table':source_table,
                         'phase':phase}
        statements.append(partition_sql)
    return {'sql': ';\n'.join(statements) + ';\n', 'expected_rows': expected,
            'publish_id': pub['publish_id'],'output_version':output_version,'tables':tables,'database_sql':statements[0]}


class HiveClient:
    def __init__(self, *, administrative=False):
        self.administrative = administrative

    def execute(self, sql):
        prefix = 'ML_HIVE_ADMIN' if self.administrative else 'ML_HIVE'
        url = os.environ.get(prefix + '_JDBC_URL','')
        if not url.startswith('jdbc:hive2://'):
            raise HistoryUnavailable('Hive 未配置，请设置 ' + prefix + '_JDBC_URL。')
        work = ROOT / 'outputs/hive-queries'
        work.mkdir(parents=True,exist_ok=True)
        source = work / (uuid.uuid4().hex + '.sql')
        source.write_text(sql,encoding='utf-8')
        try:
            result = subprocess.run(['beeline','-u',url,'-n',os.environ.get(prefix + '_USER','ml_history'),
                                     '--silent=true','--showHeader=true','--outputformat=csv2',
                                     '-f',str(source)],capture_output=True,text=True,encoding='utf-8',timeout=90)
        except (OSError,subprocess.TimeoutExpired) as error:
            raise HistoryUnavailable('Hive 查询客户端不可用或查询超时。') from error
        if result.returncode:
            raise HistoryUnavailable('Hive 查询失败，请检查分区登记和只读账号权限。')
        return list(csv.DictReader(io.StringIO(result.stdout)))


def _description(rows):
    """Interpret DESCRIBE FORMATTED csv2 rows, including shifted properties."""
    columns, partitions, properties = [], [], {}
    section = 'columns'
    for row in rows:
        cells = [str(value or '').strip() for value in row.values()]
        if len(cells) != 3:
            raise HistoryUnavailable('Hive 元数据格式不兼容。')
        first, second, third = cells
        if first.startswith('#'):
            if first == '# Partition Information':
                section = 'partitions'
            elif first.startswith('# Detailed') or first == '# Storage Information':
                section = 'properties'
            continue
        if not any(cells):
            continue
        if section in {'columns','partitions'} and first and second:
            target = columns if section == 'columns' else partitions
            target.append((first,second.lower().replace(' ','')))
        else:
            key, value = (first.rstrip(':'),second) if first else (second.rstrip(':'),third)
            if key and value:
                if key in properties and properties[key] != value:
                    raise HistoryUnavailable('Hive 元数据包含冲突属性。')
                properties[key] = value
    return columns, partitions, properties


def _hdfs_location(value):
    default = os.environ.get('ML_HDFS_URI')
    if not default:
        configuration = ET.parse(ROOT / 'runtime/hadoop-conf/core-site.xml')
        default = next((p.findtext('value') for p in configuration.findall('property')
                        if p.findtext('name') == 'fs.defaultFS'),None)
    base = urlsplit(default or '')
    uri = urlsplit(value)
    if (base.scheme != 'hdfs' or not base.netloc or base.username or base.password or
            base.query or base.fragment or base.path not in {'','/'} or
            uri.scheme not in {'','hdfs'} or uri.query or uri.fragment or
            uri.username or uri.password or (uri.netloc and uri.netloc != base.netloc) or
            (uri.scheme and not uri.netloc)):
        raise HistoryUnavailable('Hive 分区的 HDFS 服务身份不匹配。')
    try:
        path = Hdfs.path(uri.path)
    except ValueError as error:
        raise HistoryUnavailable('Hive 分区存储路径不合法。') from error
    return base.netloc, path.rstrip('/')


def _verify_table(client, table, spec):
    columns, partitions, properties = _description(client.execute('DESCRIBE FORMATTED ' + spec['name'] + ';'))
    expected = [(f.name,hive_type(f.type).lower()) for f in SCHEMAS[spec['schema']]]
    # Hive can repeat partition columns in its initial column section.
    if columns == expected + [('output_version','string')]:
        columns = columns[:-1]
    if columns != expected or partitions != [('output_version','string')]:
        raise HistoryUnavailable('Hive 表字段与正式 Parquet 契约不一致。')
    if (properties.get('Table Type') != 'EXTERNAL_TABLE' or
            properties.get('external.table.purge','').lower() != 'false' or
            properties.get('parquet.column.index.access','').lower() != 'false' or
            properties.get('SerDe Library') != 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' or
            properties.get('InputFormat') != 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'):
        raise HistoryUnavailable('Hive 表不是符合契约的安全 Parquet 外部投影。')


def _verify_partition(client, publication, spec):
    _,_,properties = _description(client.execute('DESCRIBE FORMATTED ' + spec['name'] +
        ' PARTITION (output_version=' + literal(publication['output_version']) + ');'))
    if _hdfs_location(properties.get('Location','')) != _hdfs_location(spec['location']):
        raise HistoryUnavailable('Hive 已有分区未指向该正式发布版本。')
    if (properties.get('SerDe Library') != 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' or
            properties.get('InputFormat') != 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'):
        raise HistoryUnavailable('Hive 分区存储格式不符合 Parquet 契约。')


def verify_projection(publication, client, spec):
    for table, item in spec['tables'].items():
        _verify_table(client,table,item)
        _verify_partition(client,publication,item)


def register_projection(run_id, *, store=None, client=None, hdfs=None, database='ml_history'):
    """Register immutable formal data; never repair/drop incompatible metadata."""
    store, client, hdfs = store or Store(), client or HiveClient(administrative=True), hdfs or Hdfs()
    _,pub,_ = publication_context(run_id,store)
    spec = projection(pub,database=database)
    for item in spec['tables'].values():
        expected = {pub['storage_path'] + '/' + f['path']:f for f in item['files']}
        inventory = hdfs.inventory((item['location'],))
        if len(inventory) != len(expected) or {f['path'] for f in inventory} != set(expected):
            raise HistoryUnavailable('正式 HDFS 分区文件清单不一致，拒绝登记。')
        for remote,file in expected.items():
            if hdfs.digest(remote) != {'sha256':file['sha256'],'bytes':file['bytes']}:
                raise HistoryUnavailable('正式 HDFS 分区字节不一致，拒绝登记。')
    client.execute(spec['database_sql'] + ';')
    for table,item in spec['tables'].items():
        client.execute(item['create_sql'] + ';')
        _verify_table(client,table,item)
        client.execute(item['partition_sql'] + ';')
        _verify_partition(client,pub,item)
    result = history_analysis(run_id,store=store,client=client,database=database)
    if result['publish_id'] != pub['publish_id']:
        raise HistoryUnavailable('登记期间正式发布身份发生变化。')
    return {**result,'registration_verified':True}


def history_analysis(run_id, *, store=None, client=None, database='ml_history'):
    store = store or Store()
    run,pub,_ = publication_context(run_id,store)
    spec = projection(pub,database=database)
    predicates = ' AND '.join(f'`{k}`={literal(v)}' for k,v in {
        'output_version':pub['output_version'],'run_id':run_id,'attempt_id':pub['attempt_id'],
        'input_version':run['input_version'],'rule_version':run['rule_version'],'metric_version':run['metric_version']}.items())
    checks = []
    # Count all rows in the partition as well as correctly bound rows, so foreign
    # records cannot disappear silently through a WHERE predicate.
    for table,item in spec['tables'].items():
        binding = predicates
        if item['source_table']:
            binding += ' AND source_table='+literal(item['source_table'])
        else:
            binding += " AND source_table IN ('users','movies','ratings')"
        if item['phase']:
            binding += ' AND phase='+literal(item['phase'])
        checks.append(f"SELECT '{table}' AS source_table, COUNT(*) AS actual_rows, "
                      f"COALESCE(SUM(CASE WHEN {binding} THEN 1 ELSE 0 END),0) AS bound_rows "
                      f"FROM {item['name']} WHERE output_version={literal(pub['output_version'])}")
    client = client or HiveClient()
    verify_projection(pub,client,spec)
    counts = client.execute(' UNION ALL '.join(checks) + ';')
    try:
        actual = {r['source_table']:int(r['actual_rows']) for r in counts}
        bound = {r['source_table']:int(r['bound_rows'] or 0) for r in counts}
    except (KeyError,TypeError,ValueError) as error:
        raise HistoryUnavailable('Hive 行数结果不可读。') from error
    if len(counts) != len(spec['tables']) or actual != spec['expected_rows'] or bound != actual:
        raise HistoryUnavailable('Hive 分区与正式清单行数或版本身份不一致。')
    distribution = client.execute(f'SELECT rating, COUNT(*) AS records FROM `{database}`.`ml_cleaned_ratings` '
                                  f'WHERE {predicates} GROUP BY rating ORDER BY rating;')
    try:
        distribution = [{'rating':int(r['rating']),'records':int(r['records'])} for r in distribution]
        if (sum(r['records'] for r in distribution) != actual['ratings'] or
                len({r['rating'] for r in distribution}) != len(distribution) or
                any(not 1 <= r['rating'] <= 5 or r['records'] < 0 for r in distribution)):
            raise ValueError('Distribution differs from formal count')
    except (KeyError,TypeError,ValueError) as error:
        raise HistoryUnavailable('Hive 评分分布与正式结果不一致。') from error
    return {'run_id':run_id,'publish_id':pub['publish_id'],'source':'Hive external Parquet projection',
            'output_version':pub['output_version'],'projection_rows':actual,
            'rows':{t:actual[t] for t in ('users','movies','ratings')},
            'rating_distribution':distribution,'manifest_reconciled':True}


def history_series(run_ids, *, store=None, client=None, database='ml_history'):
    """Explicit, bounded versions; never select an implicit latest partition."""
    if (not isinstance(run_ids,list) or not 2 <= len(run_ids) <= 10 or
            any(not isinstance(r,str) or not r or len(r) > 128 for r in run_ids) or
            len(set(run_ids)) != len(run_ids)):
        raise ValueError('History series requires 2 to 10 distinct Run identities')
    store,client = store or Store(),client or HiveClient()
    contexts = [publication_context(r,store) for r in run_ids]
    if len({r['dataset_id'] for r,_,_ in contexts}) != 1:
        raise ReportUnavailable('history series requires the same dataset',409)
    points = []
    for run_id,(run,pub,_) in zip(run_ids,contexts):
        result = history_analysis(run_id,store=store,client=client,database=database)
        if result['publish_id'] != pub['publish_id']:
            raise HistoryUnavailable('历史查询期间正式发布身份发生变化。')
        _,report,report_pub = read_published_report(run_id,store=store)
        if (report_pub['publish_id'] != pub['publish_id'] or
                report_pub['attempt_id'] != pub['attempt_id'] or
                report_pub['output_version'] != pub['output_version'] or
                report_pub['manifest'] != pub['manifest']):
            raise HistoryUnavailable('历史报告与 Hive 查询的正式发布身份不一致。')
        quality,actions = _history_report(report,pub)
        points.append({**result,'quality':quality,'action_counts':actions,
                       'summary_source':'manifest-verified formal report.json',
                       'versions':{k:run[k] for k in
                       ('input_version','rule_version','metric_version')}})
    transitions = []
    for left,right in zip(points,points[1:]):
        transitions.append({'left_run':left['run_id'],'right_run':right['run_id'],
            'changed_versions':[k for k in left['versions'] if left['versions'][k] != right['versions'][k]],
            **_summary_transition(left,right),
            'row_delta':{t:right['rows'][t]-left['rows'][t] for t in left['rows']},
            'rating_count_delta':{str(rating):
                next((r['records'] for r in right['rating_distribution'] if r['rating']==rating),0) -
                next((r['records'] for r in left['rating_distribution'] if r['rating']==rating),0)
                for rating in range(1,6)}})
    return {'dataset_id':contexts[0][0]['dataset_id'],'points':points,'transitions':transitions,
            'ordering':'caller supplied Run order',
            'limitations':['数量变化是版本间的描述性比较，不能据此归因于某项清洗规则。',
                           '质量与动作汇总来自同一正式发布的报告；Hive 提供正式记录数量和评分分布。',
                           '指标版本不同时不计算质量差值；规则版本不同时不计算动作差值。',
                           '动作按事件计数，一条来源记录可能有多个动作，不能相加作为处置记录数。',
                           '仅覆盖明确选择且已经正式发布的版本，不代表完整历史。']}


def _history_report(report,publication):
    """Expose verified summaries, without deriving global facts from samples."""
    try:
        quality = {}
        for table in ('users','movies','ratings'):
            scores = report['scores'][table]
            quality[table] = {phase:dict(scores[phase]) for phase in ('raw','clean')}
            for phase in ('raw','clean'):
                if set(quality[table][phase]) != {'accurate','complete','unique','up_to_date','consistent'}:
                    raise ValueError('Missing quality dimension')
            quality[table].update({k:scores[k] for k in ('composite_raw','composite_clean')})
        quality['dataset_composite'] = {phase:report['dataset_composite'][phase] for phase in ('raw','clean')}
        values = [v for table in ('users','movies','ratings') for phase in ('raw','clean')
                  for v in quality[table][phase].values()]
        composites = [quality[t][k] for t in ('users','movies','ratings')
                      for k in ('composite_raw','composite_clean')] + list(quality['dataset_composite'].values())
        if any(type(v) not in (int,float) or not math.isfinite(v) or not 0 <= v <= 100
               for v in composites + [v for v in values if v is not None]):
            raise ValueError('Invalid quality value')
        actions = report['action_counts']
        if (not isinstance(actions,dict) or set(actions) != {'users','movies','ratings'} or
                actions != publication['manifest']['action_counts'] or
                any(not isinstance(counts,dict) or any(not isinstance(k,str) or not k or
                    type(v) is not int or v < 0 for k,v in counts.items()) for counts in actions.values())):
            raise ValueError('Invalid action summary')
    except (KeyError,TypeError,ValueError) as error:
        raise HistoryUnavailable('正式历史报告的质量或动作汇总不完整或与清单不一致。') from error
    return quality,actions


def _summary_transition(left,right):
    metric_ok = left['versions']['metric_version'] == right['versions']['metric_version']
    rule_ok = left['versions']['rule_version'] == right['versions']['rule_version']
    def difference(a,b):
        return round(b-a,6) if metric_ok and a is not None and b is not None else None
    scores = []
    for table in ('users','movies','ratings'):
        for phase in ('raw','clean'):
            for metric,a in left['quality'][table][phase].items():
                b = right['quality'][table][phase][metric]
                scores.append({'table':table,'phase':phase,'metric':metric,
                               'left':a,'right':b,'delta':difference(a,b)})
        for phase in ('raw','clean'):
            a,b = (p['quality'][table]['composite_'+phase] for p in (left,right))
            scores.append({'table':table,'phase':phase,'metric':'composite',
                           'left':a,'right':b,'delta':difference(a,b)})
    composites = {phase:difference(left['quality']['dataset_composite'][phase],
                                   right['quality']['dataset_composite'][phase]) for phase in ('raw','clean')}
    actions = []
    for table in ('users','movies','ratings'):
        a,b = left['action_counts'][table],right['action_counts'][table]
        for action in sorted(a.keys() | b.keys()):
            actions.append({'table':table,'action_rule':action,'left':a.get(action,0),'right':b.get(action,0),
                            'delta':b.get(action,0)-a.get(action,0) if rule_ok else None})
    return {'metric_compatible':metric_ok,'rule_compatible':rule_ok,'quality_delta':scores,
            'dataset_composite_delta':composites,'action_delta':actions}
