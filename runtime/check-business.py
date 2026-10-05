"""Model-free business contract checks; emits a reviewable verification record."""
import datetime as dt
import hashlib
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def main():
    suite = unittest.defaultTestLoader.discover(str(ROOT/'tests'),pattern='test_*.py')
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    sources = {}
    sources_to_hash = {ROOT/'requirements.txt',ROOT/'.dockerignore'}
    for directory,patterns in (
        ('agent',('*.py',)),('governance',('*.py',)),('metadata',('*.py',)),
        ('storage',('*.py',)),('pipeline',('*.py',)),('tests',('*.py',)),
        ('runtime',('*.py','*.ps1','*.yaml','*.xml','*.sh','requirements*.txt','Dockerfile*')),
        ('hadoop/src/main/java',('*.java',)),('schemas',('*.json','*.sql')),
        ('.github/workflows',('*.yml','*.yaml'))):
        for pattern in patterns:
            sources_to_hash.update((ROOT/directory).rglob(pattern))
    for path in sorted(sources_to_hash):
        if path.is_file():
            sources[path.relative_to(ROOT).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    report = {'schema_version':'business-check-v1','completed_at':dt.datetime.now(dt.timezone.utc).isoformat(),
              'python':sys.version,'tests_run':result.testsRun,'failures':len(result.failures),
              'errors':len(result.errors),'skipped':[{'test':str(t),'reason':reason} for t,reason in result.skipped],
              'passed':result.testsRun-len(result.failures)-len(result.errors)-len(result.skipped),
              'successful':result.wasSuccessful(),'source_sha256':sources,
              'scope':'unit and module contracts, including temporary real Parquet bytes',
              'real_environment_verified':False,'frontend_operations_verified':False}
    target = ROOT/'outputs/business-validation'/dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    target.mkdir(parents=True,exist_ok=False)
    (target/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'verification':str(target/'verification.json'),'successful':result.wasSuccessful(),
                      'tests_run':result.testsRun,'passed':report['passed'],'skipped':len(result.skipped)},ensure_ascii=False))
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(main())
