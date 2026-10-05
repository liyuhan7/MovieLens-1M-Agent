"""Small manifest fixtures covering every supported history projection."""
import hashlib

from governance.artifacts import SCHEMAS


def projection_files():
    files = []
    for table in ('users','movies','ratings'):
        for family,schema in (('cleaned',table),('evidence','evidence')):
            files.append((f'{family}/{table}/part-00000.parquet',schema))
        for phase in ('before','after'):
            files.append((f'quality/{phase}/{table}.parquet','quality_detail'))
    return [{'path':path,'format':'parquet','rows':2,'bytes':20,'sha256':'a'*64,
             'schema_sha256':hashlib.sha256(SCHEMAS[schema].serialize().to_pybytes()).hexdigest()}
            for path,schema in files]
