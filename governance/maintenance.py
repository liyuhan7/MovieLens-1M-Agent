"""Read-only retention preview. No delete command is provided."""
import datetime as dt
from pathlib import PurePosixPath

from governance.reconciliation import classify_inventory


def retention_preview(inventory, references, *, retain_days=30, now=None):
    if type(retain_days) is not int or not 1 <= retain_days <= 3650:
        raise ValueError("Retention must be between 1 and 3650 days")
    now = now or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        raise ValueError("Preview time must include a timezone")
    cutoff = now.astimezone(dt.timezone.utc).replace(tzinfo=None) - dt.timedelta(days=retain_days)
    report = classify_inventory(inventory, references)
    candidates, protected = [], []
    publications = {(p['run_id'],p['attempt_id']) for ref in references for p in ref['publications']}
    attempts = {}
    for ref in references:
        for attempt in ref['attempts']:
            attempts.setdefault((attempt['run_id'],attempt['attempt_id']), []).append(attempt)
    for item in report['files']:
        parts = PurePosixPath(item['path']).parts
        reason = item['classification']
        eligible = False
        if len(parts) >= 6 and parts[1:3] == ('ml','staging') and parts[3].startswith('run=') and parts[4].startswith('attempt='):
            key = (parts[3][4:],parts[4][8:])
            owners = attempts.get(key, [])
            if key not in publications and owners:
                eligible = all(a['status'] == 'FAILED' and not a.get('lease_alive') and
                               a.get('ended_at') is not None and a['ended_at'] < cutoff and
                               a.get('active_attempt') != a['attempt_id'] for a in owners)
                # Unknown or fresh file timestamps require review even for an old Attempt.
                try:
                    eligible = eligible and dt.datetime.strptime(item['modified'], '%Y-%m-%d %H:%M') < cutoff
                except (ValueError, KeyError):
                    eligible = False
            reason = 'FAILED_ATTEMPT_RETENTION_EXPIRED' if eligible else reason
        entry = {**item, 'retention_reason': reason}
        (candidates if eligible else protected).append(entry)
    return {'schema_version':'retention-preview-v1','generated_at':now.isoformat(),'retain_days':retain_days,
            'candidates':candidates,'candidate_bytes':sum(i['bytes'] for i in candidates),
            'protected':protected,'deletion_authorized':False,
            'requires_recheck_before_execution':True,'metadata_scopes':report['metadata_scopes'],
            'incomplete_publications':report['incomplete_publications'],
            'publication_mismatches':report['publication_mismatches']}


def preview_storage(stores, *, hdfs=None, retain_days=30):
    from storage.hdfs import Hdfs
    hdfs = hdfs or Hdfs()
    # Read references both before and after inventory. Any state change cancels candidates.
    before = [store.storage_references() for store in stores]
    inventory = hdfs.inventory()
    after = [store.storage_references() for store in stores]
    if before != after:
        raise ValueError("Metadata changed while scanning; refresh the preview")
    return retention_preview(inventory, after, retain_days=retain_days)
