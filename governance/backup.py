"""Quiescent metadata checkpoint plus immutable Raw/Published/legacy bytes.

Restore writes into an empty, separately named recovery database. Files are
verified and materialized before metadata becomes visible; no overwrite/delete.
"""
import base64
import datetime as dt
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil

import pymysql

from metadata.connection import ROOT, connect, migrate
from metadata.store import canonical, fingerprint
from storage.hdfs import Hdfs


TABLES = ('schema_migration','runtime_control','runtime_control_event','dataset','definition_version','dataset_version','dataset_location',
          'logical_run','physical_attempt','hadoop_job','quality_result','evidence_index',
          'publish_version','dataset_current','run_event','worker_slot',
          'legacy_report_archive','legacy_report_alias')
CAPTURED = {'logical_run','physical_attempt','worker_slot','dataset_version','dataset_location','publish_version','legacy_report_archive'}


def encode(value):
    if isinstance(value, dt.datetime):
        return {'type':'datetime','value':value.isoformat()}
    if isinstance(value, Decimal):
        return {'type':'decimal','value':str(value)}
    if isinstance(value, bytes):
        return {'type':'bytes','value':base64.b64encode(value).decode('ascii')}
    if value is None or type(value) in (str,int,float,bool):
        return {'type':'scalar','value':value}
    raise ValueError('Unsupported metadata cell type')


def decode(cell):
    if not isinstance(cell,dict) or set(cell) != {'type','value'}:
        raise ValueError('Invalid metadata cell')
    kind,value = cell['type'],cell['value']
    if kind == 'datetime':
        return dt.datetime.fromisoformat(value)
    if kind == 'decimal':
        return Decimal(value)
    if kind == 'bytes':
        return base64.b64decode(value,validate=True)
    if kind == 'scalar' and (value is None or type(value) in (str,int,float,bool)):
        return value
    raise ValueError('Invalid metadata cell type')


def digest(path):
    result,size = hashlib.sha256(),0
    with Path(path).open('rb') as stream:
        while chunk := stream.read(1 << 20):
            result.update(chunk)
            size += len(chunk)
    return {'sha256':result.hexdigest(),'bytes':size}


def safe_file(root, relative):
    name = PurePosixPath(relative)
    root = Path(root).resolve()
    path = root / relative
    if (name.is_absolute() or '..' in name.parts or name.as_posix() != relative or '\\' in relative or
            not path.is_relative_to(root) or path.resolve() != path or path.is_symlink()):
        raise ValueError('Backup path is redirected or outside its root')
    return path


def write_metadata(root, table, columns, rows):
    if table not in TABLES or not columns or len(set(columns)) != len(columns) or any(not re.fullmatch('[a-zA-Z_][a-zA-Z0-9_]*',c) for c in columns):
        raise ValueError('Invalid metadata table or columns')
    path = safe_file(root,'metadata/'+table+'.jsonl')
    path.parent.mkdir(parents=True,exist_ok=True)
    count, captured = 0, []
    with path.open('x',encoding='utf-8',newline='\n') as out:
        for row in rows:
            if set(row) != set(columns):
                raise ValueError('Metadata row differs from declared columns')
            out.write(canonical({key:encode(row[key]) for key in columns})+'\n')
            count += 1
            if table in CAPTURED:
                captured.append(row)
    return {'path':'metadata/'+table+'.jsonl','table':table,'columns':columns,'rows':count,**digest(path)},captured


def export_metadata(root, config):
    connection = connect(config)
    files, captured = [], {}
    try:
        with connection.cursor() as cursor:
            cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            cursor.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT')
            cursor.execute('SHOW TABLES')
            existing = {next(iter(row.values())) for row in cursor.fetchall()}
        for table in TABLES:
            if table not in existing:
                if table.startswith('legacy_'):
                    continue
                raise ValueError('Missing metadata table: '+table)
            # Streaming cursor keeps the Evidence Index bounded in memory.
            with connection.cursor(pymysql.cursors.SSDictCursor) as cursor:
                cursor.execute('SELECT * FROM `'+table+'`')
                columns = [item[0] for item in cursor.description]
                item, rows = write_metadata(root,table,columns,iter(cursor.fetchone,None))
                files.append(item)
                captured[table] = rows
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    assert_quiescent(captured)
    return files,captured


def json_column(value):
    return json.loads(value) if isinstance(value,str) else value


def assert_quiescent(snapshot):
    if any(r['status'] not in {'PUBLISHED','FAILED'} for r in snapshot.get('logical_run',[])):
        raise ValueError('Backup checkpoint requires no queued or executing Run')
    if any(p['status'] != 'PUBLISHED' for p in snapshot.get('publish_version',[])):
        raise ValueError('Resolve incomplete publication intents before making a recovery checkpoint')
    if any(a['status'] not in {'PUBLISHED','FAILED'} for a in snapshot.get('physical_attempt',[])):
        raise ValueError('Backup checkpoint has unfinished Attempts')
    if any(s.get('run_id') or s.get('lease_owner') for s in snapshot.get('worker_slot',[])):
        raise ValueError('Backup checkpoint has an occupied worker slot')
    runs = {r['run_id']:r for r in snapshot.get('logical_run',[])}
    attempts = {a['attempt_id']:a for a in snapshot.get('physical_attempt',[])}
    publications = {p['run_id']:p for p in snapshot.get('publish_version',[])}
    if len(publications) != len(snapshot.get('publish_version',[])):
        raise ValueError('Recovery checkpoint has conflicting publications')
    if {r for r,v in runs.items() if v['status']=='PUBLISHED'} != set(publications):
        raise ValueError('Published Run identities differ from publication records')
    for pub in publications.values():
        attempt = attempts.get(pub['attempt_id'])
        if not attempt or attempt['run_id'] != pub['run_id'] or attempt['status'] != 'PUBLISHED':
            raise ValueError('Formal publication has no matching published Attempt')


def storage_materials(snapshot):
    versions = {v['version_id']:json_column(v['manifest']) for v in snapshot.get('dataset_version',[])}
    result = {}
    def add(remote, sha256, size):
        remote = Hdfs.path(remote)
        if not remote.startswith(('/ml/raw/','/ml/published/')) or not re.fullmatch('[0-9a-f]{64}',sha256) or type(size) is not int or size < 0:
            raise ValueError('Invalid immutable backup material')
        value = {'remote':remote,'sha256':sha256,'bytes':size,'path':'objects/'+sha256}
        if remote in result and result[remote] != value:
            raise ValueError('Conflicting storage material')
        result[remote] = value
    for loc in snapshot.get('dataset_location',[]):
        source = versions[loc['version_id']]['tables'][loc['source_table']]
        if source['sha256'] != loc['sha256'] or source['bytes'] != loc['bytes']:
            raise ValueError('Raw location differs from dataset version')
        if 'files' in source:
            for file in source['files']:
                name = PurePosixPath(file['name'])
                if name.is_absolute() or '..' in name.parts or name.as_posix() != file['name'] or '\\' in file['name']:
                    raise ValueError('Raw member name must be an unredirected relative path')
                add(loc['storage_uri']+'/'+file['name'],file['sha256'],file['bytes'])
        else:
            add(loc['storage_uri'],loc['sha256'],loc['bytes'])
    for pub in snapshot.get('publish_version',[]):
        manifest = json_column(pub['manifest'])
        if fingerprint(manifest) != pub['manifest_hash'] or manifest['run_id'] != pub['run_id'] or manifest['attempt_id'] != pub['attempt_id']:
            raise ValueError('Invalid formal publication manifest')
        for file in manifest['files']:
            name = PurePosixPath(file['path'])
            if name.is_absolute() or '..' in name.parts or name.as_posix() != file['path']:
                raise ValueError('Invalid publication file path')
            add(pub['storage_path']+'/'+file['path'],file['sha256'],file['bytes'])
        # The manifest's physical byte checksum differs from its canonical identity.
        result[pub['storage_path']+'/manifest.json'] = {'remote':Hdfs.path(pub['storage_path']+'/manifest.json'),
            'manifest_identity':pub['manifest_hash'],'path':'manifests/'+pub['publish_id']+'.json'}
    return list(result.values())


def create_backup(target, config, *, hdfs=None, exporter=None):
    target = Path(target).absolute()
    if target.resolve() != target or not target.is_relative_to(ROOT / 'outputs'):
        raise ValueError('Backup package must be under unredirected outputs/')
    target.mkdir(parents=True,exist_ok=False)
    hdfs = hdfs or Hdfs()
    files,snapshot = (exporter or export_metadata)(target,config)
    assert_quiescent(snapshot)
    materials = storage_materials(snapshot)
    objects = {f['path']:f for f in files}
    for material in materials:
        path = safe_file(target,material['path'])
        if material['path'] not in objects:
            hdfs.get(material['remote'],path)
            actual = digest(path)
            if 'manifest_identity' in material:
                if fingerprint(json.loads(path.read_bytes())) != material['manifest_identity']:
                    raise ValueError('Downloaded manifest identity differs')
                material.update(actual)
            elif actual != {k:material[k] for k in ('sha256','bytes')}:
                raise ValueError('Downloaded backup bytes differ from registered material')
            objects[material['path']] = {'path':material['path'],**actual}
        elif digest(path) != {k:material[k] for k in ('sha256','bytes')}:
            raise ValueError('Shared immutable object differs')
    legacy = []
    for archive in snapshot.get('legacy_report_archive',[]):
        path = safe_file(ROOT,archive['source_path'])
        if not path.is_relative_to(ROOT / 'outputs') or digest(path) != {'sha256':archive['source_sha256'],'bytes':archive['source_bytes']}:
            raise ValueError('Historical report differs from archive metadata')
        relative = 'objects/'+archive['source_sha256']
        output = safe_file(target,relative)
        if relative not in objects:
            output.parent.mkdir(parents=True,exist_ok=True)
            with output.open('xb') as out,path.open('rb') as source:
                shutil.copyfileobj(source,out)
            objects[relative] = {'path':relative,**digest(output)}
        legacy.append({'source_path':archive['source_path'],'path':relative,'sha256':archive['source_sha256'],'bytes':archive['source_bytes']})
    manifest = {'schema_version':'governance-backup-v1','source_database':config['database'],
                'created_at':dt.datetime.now(dt.timezone.utc).isoformat(),'checkpoint':'QUIESCENT',
                'files':list(objects.values()),'storage':materials,'legacy':legacy,
                'restore_policy':'EMPTY_RECOVERY_DATABASE_IMMUTABLE_FILES_FIRST'}
    body = canonical(manifest).encode('utf-8')
    (target/'manifest.json').write_bytes(body)
    (target/'manifest.sha256').write_text(hashlib.sha256(body).hexdigest(),encoding='ascii')
    verify_backup(target)
    return manifest


def metadata_rows(target, item):
    count = 0
    with safe_file(target,item['path']).open(encoding='utf-8') as source:
        for line in source:
            row = json.loads(line)
            if set(row) != set(item['columns']):
                raise ValueError('Backup metadata columns differ')
            count += 1
            yield {k:decode(v) for k,v in row.items()}
    if count != item['rows']:
        raise ValueError('Backup metadata row count differs')


def verify_backup(target):
    target = Path(target).resolve()
    body = safe_file(target,'manifest.json').read_bytes()
    if hashlib.sha256(body).hexdigest() != safe_file(target,'manifest.sha256').read_text(encoding='ascii').strip():
        raise ValueError('Backup manifest checksum mismatch')
    manifest = json.loads(body)
    if manifest['schema_version'] != 'governance-backup-v1' or manifest['checkpoint'] != 'QUIESCENT':
        raise ValueError('Unsupported recovery checkpoint')
    files, tables, snapshot = {}, set(), {}
    for item in manifest['files']:
        path = safe_file(target,item['path'])
        if item['path'] in files or digest(path) != {k:item[k] for k in ('sha256','bytes')}:
            raise ValueError('Backup file checksum or inventory differs')
        files[item['path']] = item
        if 'table' in item:
            if item['table'] not in TABLES or item['table'] in tables or item['path'] != 'metadata/'+item['table']+'.jsonl':
                raise ValueError('Invalid metadata table inventory')
            if not item['columns'] or any(not re.fullmatch('[a-zA-Z_][a-zA-Z0-9_]*',c) for c in item['columns']):
                raise ValueError('Invalid metadata column identity')
            tables.add(item['table'])
            captured = []
            for row in metadata_rows(target,item):
                if item['table'] in CAPTURED:
                    captured.append(row)
            snapshot[item['table']] = captured
    if not (set(TABLES[:-2])-{'runtime_control','runtime_control_event'}).issubset(tables):
        raise ValueError('Recovery checkpoint lacks required metadata tables')
    assert_quiescent(snapshot)
    actual = {p.relative_to(target).as_posix() for p in target.rglob('*') if p.is_file()}
    if actual != set(files)|{'manifest.json','manifest.sha256'}:
        raise ValueError('Undeclared backup files')
    for material in [*manifest['storage'],*manifest['legacy']]:
        if material['path'] not in files or any(material[k] != files[material['path']][k] for k in ('sha256','bytes')):
            raise ValueError('Backup reference differs from object inventory')
        if 'remote' in material:
            remote = Hdfs.path(material['remote'])
            if not remote.startswith(('/ml/raw/','/ml/published/')):
                raise ValueError('Recovery target is not immutable storage')
        else:
            if not safe_file(ROOT,material['source_path']).is_relative_to(ROOT/'outputs'):
                raise ValueError('Historical restore path escaped outputs/')
    expected = {m['remote']:m for m in storage_materials(snapshot)}
    actual_storage = {m['remote']:m for m in manifest['storage']}
    if len(actual_storage) != len(manifest['storage']) or set(expected) != set(actual_storage):
        raise ValueError('Backup storage does not cover metadata references')
    for remote, item in expected.items():
        actual_item = actual_storage[remote]
        if any(actual_item.get(k) != v for k,v in item.items()):
            raise ValueError('Backup storage reference differs from metadata')
        if 'manifest_identity' in item:
            body = json.loads(safe_file(target,item['path']).read_bytes())
            if fingerprint(body) != item['manifest_identity']:
                raise ValueError('Restored publication manifest identity differs')
    expected_legacy = [{'source_path':a['source_path'],'path':'objects/'+a['source_sha256'],
                        'sha256':a['source_sha256'],'bytes':a['source_bytes']}
                       for a in snapshot.get('legacy_report_archive',[])]
    if sorted(expected_legacy,key=canonical) != sorted(manifest['legacy'],key=canonical):
        raise ValueError('Backup does not cover historical archive references')
    return manifest


def restore_backup(target, config, *, hdfs=None):
    if not re.fullmatch('ml_governance_restore_[a-zA-Z0-9_]+',config['database']):
        raise ValueError('Restore requires a separate ml_governance_restore_<name> database')
    manifest = verify_backup(target)
    migrate(config)
    from agent.legacy_archive import install_archive_schema
    from metadata.store import Store
    install_archive_schema(Store(config))
    hdfs = hdfs or Hdfs()
    connection = connect(config)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT GET_LOCK('ml_governance_restore',30) AS acquired")
            if cursor.fetchone()['acquired'] != 1:
                raise ValueError('Another recovery operation is running')
            for table in TABLES:
                if table in {'schema_migration','worker_slot','runtime_control'}:
                    continue
                cursor.execute('SELECT COUNT(*) AS n FROM `'+table+'` FOR UPDATE')
                if cursor.fetchone()['n']:
                    raise ValueError('Recovery target database must be empty')
            cursor.execute('SELECT * FROM worker_slot FOR UPDATE')
            if any(row['run_id'] or row['lease_owner'] for row in cursor.fetchall()):
                raise ValueError('Recovery target has an active worker slot')
            # A failed file copy leaves no visible recovered Run/current records.
            for material in manifest['storage']:
                hdfs.put_immutable(safe_file(target,material['path']),material['remote'])
            for material in manifest['legacy']:
                source,destination = safe_file(target,material['path']),safe_file(ROOT,material['source_path'])
                destination.parent.mkdir(parents=True,exist_ok=True)
                if destination.exists():
                    if digest(destination) != digest(source):
                        raise ValueError('Existing historical report differs; refusing overwrite')
                else:
                    os.link(source,destination)
            by_table = {f['table']:f for f in manifest['files'] if 'table' in f}
            for table in TABLES:
                if table not in by_table:
                    continue
                item = by_table[table]
                cursor.execute('SHOW COLUMNS FROM `'+table+'`')
                if set(r['Field'] for r in cursor.fetchall()) != set(item['columns']):
                    raise ValueError('Restore schema columns differ: '+table)
                columns = item['columns']
                sql = ('INSERT IGNORE' if table == 'schema_migration' else 'REPLACE' if table in {'worker_slot','runtime_control'} else 'INSERT')
                sql += ' INTO `'+table+'` ('+','.join('`'+c+'`' for c in columns)+') VALUES ('+','.join(['%s']*len(columns))+')'
                rows = metadata_rows(target,item)
                if table == 'dataset_version':
                    # Self-referencing parent versions must be inserted first.
                    pending = list(rows)
                    ordered, known = [],set()
                    while pending:
                        ready = [r for r in pending if not r['parent_version_id'] or r['parent_version_id'] in known]
                        if not ready:
                            raise ValueError('Dataset version parents are missing or cyclic')
                        ordered.extend(ready)
                        known.update(r['version_id'] for r in ready)
                        pending = [r for r in pending if r not in ready]
                    rows = iter(ordered)
                batch = []
                for row in rows:
                    batch.append(tuple(row[c] for c in columns))
                    if len(batch) == 500:
                        cursor.executemany(sql,batch)
                        batch = []
                if batch:
                    cursor.executemany(sql,batch)
            connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        # Closing the dedicated connection releases its advisory lock even when
        # a failed transaction has left the connection unable to run another SQL.
        connection.close()
    return {'database':config['database'],'restored_files':len(manifest['storage']),
            'metadata_restored':True,'real_environment_verified':False}
