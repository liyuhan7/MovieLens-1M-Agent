"""Durable Run/Attempt/job state with database-clock leases and fencing."""
import dataclasses
import hashlib
import json
import re
import uuid
from decimal import Decimal

from metadata.connection import load_config, transaction


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def decoded(row):
    if row:
        for key in ("request", "manifest", "definition", "error", "environment", "specification", "detail"):
            if isinstance(row.get(key), str):
                row[key] = json.loads(row[key])
    return row


class Conflict(ValueError):
    """An immutable identity was reused with different content."""


class LostLease(RuntimeError):
    """This executor is no longer allowed to update business state."""


class AdmissionPaused(Conflict):
    """A durable maintenance window rejects new work, while recovery may drain."""


@dataclasses.dataclass(frozen=True)
class Claim:
    run_id: str
    attempt_id: str
    token: int
    owner: str
    recovering: bool
    work_path: str
    status: str


TRANSITIONS = {
    "RUNNING": {"VALIDATING", "FAILED", "RECOVERING"},
    "RECOVERING": {"RUNNING", "VALIDATING", "FAILED"},
    "VALIDATING": {"PUBLISHING", "FAILED"},
    "PUBLISHING": {"PUBLISHED", "FAILED"},
}
TERMINAL = {"FAILED", "PUBLISHED"}
EXPECTED_CURRENT_UNSET = object()


class Store:
    def __init__(self, config=None):
        self.config = config or load_config()

    def transaction(self):
        return transaction(self.config)

    @staticmethod
    def admission(cursor):
        cursor.execute("SELECT * FROM runtime_control WHERE control_id=1 FOR UPDATE")
        row = cursor.fetchone()
        if not row or type(row.get('admission_open')) not in (bool,int):
            raise RuntimeError('Runtime admission control is unavailable; migrate schema first')
        return row

    def control_admission(self, *, open_admission, owner, reason, expected_revision):
        if (type(open_admission) is not bool or type(expected_revision) is not int or expected_revision<0 or
                not isinstance(owner,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}',owner) or
                not isinstance(reason,str) or not reason.strip() or len(reason)>1000):
            raise ValueError('Explicit admission mode, owner, reason and revision are required')
        with self.transaction() as cursor:
            row = self.admission(cursor)
            if row['revision']!=expected_revision:
                raise Conflict('Runtime admission revision changed; inspect before retrying')
            if row.get('switch_id'):
                raise Conflict('A service switch is reserved; reconcile it before changing admission')
            if not row['admission_open'] and row['owner']!=owner:
                raise Conflict('Maintenance window belongs to another owner')
            if bool(row['admission_open'])==open_admission:
                if row['owner']==owner and row['reason']==reason:
                    return row
                raise Conflict('Admission is already in this mode with different authority')
            revision = row['revision']+1
            cursor.execute('UPDATE runtime_control SET admission_open=%s,revision=%s,owner=%s,reason=%s,'
                           'updated_at=NOW(6) WHERE control_id=1',(open_admission,revision,owner,reason))
            cursor.execute('INSERT INTO runtime_control_event (revision,admission_open,owner,reason) '
                           'VALUES (%s,%s,%s,%s)',(revision,open_admission,owner,reason))
            return {'control_id':1,'admission_open':open_admission,'revision':revision,'owner':owner,'reason':reason}

    def reserve_switch(self, *, owner, expected_revision, switch_id, finish=False):
        if (not isinstance(switch_id,str) or not re.fullmatch('[a-f0-9]{32}',switch_id) or
                not isinstance(owner,str) or not re.fullmatch('[A-Za-z0-9_-]{1,80}',owner) or
                type(expected_revision) is not int or expected_revision<0 or type(finish) is not bool):
            raise ValueError('Explicit switch identity, owner and revision are required')
        with self.transaction() as cursor:
            row = self.admission(cursor)
            if row['admission_open'] or row['owner']!=owner or row['revision']!=expected_revision:
                raise Conflict('Switch requires the inspected closed maintenance window')
            if finish:
                if row.get('switch_id')!=switch_id:
                    raise Conflict('Reserved switch identity differs')
            else:
                if row.get('switch_id'):
                    raise Conflict('Another service switch is reserved')
                cursor.execute("SELECT COUNT(*) AS n FROM logical_run WHERE status NOT IN ('FAILED','PUBLISHED','QUEUED')")
                if cursor.fetchone()['n']:
                    raise Conflict('Active Runs prevent switching')
                cursor.execute("SELECT COUNT(*) AS n FROM physical_attempt WHERE status NOT IN ('FAILED','PUBLISHED')")
                if cursor.fetchone()['n']:
                    raise Conflict('Active Attempts prevent switching')
                cursor.execute("SELECT COUNT(*) AS n FROM publish_version WHERE status!='PUBLISHED'")
                if cursor.fetchone()['n']:
                    raise Conflict('Unfinished publication prevents switching')
                cursor.execute('SELECT * FROM worker_slot WHERE slot_id=1 FOR UPDATE')
                slot = cursor.fetchone()
                if not slot or slot['run_id'] or slot['lease_owner']:
                    raise Conflict('Occupied execution slot prevents switching')
            revision = row['revision']+1
            cursor.execute('UPDATE runtime_control SET switch_id=%s,revision=%s,updated_at=NOW(6) WHERE control_id=1',
                           (None if finish else switch_id,revision))
            cursor.execute('INSERT INTO runtime_control_event (revision,admission_open,owner,reason) VALUES (%s,FALSE,%s,%s)',
                           (revision,owner,('switch-finished:' if finish else 'switch-reserved:')+switch_id))
            return {'revision':revision,'switch_id':None if finish else switch_id,'admission_open':False,'owner':owner}

    def maintenance_status(self):
        with self.transaction() as cursor:
            control = self.admission(cursor)
            cursor.execute('SELECT status,COUNT(*) AS n FROM logical_run GROUP BY status')
            runs = {r['status']:r['n'] for r in cursor.fetchall()}
            cursor.execute("SELECT COUNT(*) AS n FROM physical_attempt WHERE status NOT IN ('FAILED','PUBLISHED')")
            attempts = cursor.fetchone()['n']
            cursor.execute("SELECT COUNT(*) AS n FROM publish_version WHERE status!='PUBLISHED'")
            publications = cursor.fetchone()['n']
            cursor.execute('SELECT * FROM worker_slot WHERE slot_id=1')
            slot = cursor.fetchone()
            occupied = not slot or bool(slot['run_id'] or slot['lease_owner'])
        active = sum(n for status,n in runs.items() if status not in TERMINAL|{'QUEUED'})
        return {'control':control,'run_counts':runs,'active_runs':active,'active_attempts':attempts,
                'incomplete_publications':publications,'slot_occupied':occupied,
                'ready_for_switch':not control['admission_open'] and not control.get('switch_id') and not(active or attempts or publications or occupied),
                'queued_runs':runs.get('QUEUED',0),
                'note':'排队任务保留；关闭接入阻止新提交、重试和首次领取，已有执行可恢复直至终态。'}

    @staticmethod
    def event(cursor, run_id, kind, detail, attempt_id=None):
        cursor.execute("INSERT INTO run_event (run_id,attempt_id,event_type,detail) VALUES (%s,%s,%s,%s)",
                       (run_id, attempt_id, kind, canonical(detail)))

    def register_definition(self, kind, version, definition):
        if kind not in {"rule", "metric"}:
            raise ValueError("Unknown definition kind")
        digest = fingerprint(definition)
        with self.transaction() as cursor:
            cursor.execute("INSERT INTO definition_version (kind,version_id,content_hash,definition) "
                           "VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE version_id=version_id",
                           (kind, version, digest, canonical(definition)))
            cursor.execute("SELECT content_hash FROM definition_version WHERE kind=%s AND version_id=%s",
                           (kind, version))
            if cursor.fetchone()["content_hash"] != digest:
                raise Conflict("Registered rule/metric definitions cannot change in place")
        return digest

    def register_input(self, dataset_id, version_id, manifest, status="LOCAL_AVAILABLE"):
        digest = fingerprint(manifest)
        with self.transaction() as cursor:
            cursor.execute("INSERT INTO dataset (dataset_id,description) VALUES (%s,%s) "
                           "ON DUPLICATE KEY UPDATE dataset_id=dataset_id", (dataset_id, dataset_id))
            cursor.execute("INSERT INTO dataset_version (version_id,dataset_id,content_hash,manifest,status) "
                           "VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE version_id=version_id",
                           (version_id, dataset_id, digest, canonical(manifest), status))
            cursor.execute("SELECT dataset_id,content_hash FROM dataset_version WHERE version_id=%s", (version_id,))
            existing = cursor.fetchone()
            if existing["dataset_id"] != dataset_id or existing["content_hash"] != digest:
                raise Conflict("Dataset version already identifies different content")
        return digest

    def submit(self, request, idempotency_key, *, scope="governance", run_id=None):
        if not idempotency_key or len(idempotency_key) > 120 or not scope or len(scope) > 80:
            raise ValueError("A bounded idempotency key and scope are required")
        run_id = run_id or "run-" + uuid.uuid4().hex
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
            raise ValueError("Invalid Run ID")
        digest = fingerprint(request)
        with self.transaction() as cursor:
            control = self.admission(cursor)
            if not control['admission_open']:
                cursor.execute('SELECT * FROM logical_run WHERE idempotency_scope=%s AND idempotency_key=%s FOR UPDATE',
                               (scope,idempotency_key))
                existing = cursor.fetchone()
                if existing and existing['request_hash']==digest:
                    return decoded(existing),False
                if existing:
                    raise Conflict('The same request key cannot identify different parameters or versions')
                raise AdmissionPaused('New submissions are paused for maintenance')
            for kind in ("rule", "metric"):
                cursor.execute("SELECT version_id FROM definition_version WHERE kind=%s AND version_id=%s",
                               (kind, request[kind + "_version"]))
                if not cursor.fetchone():
                    raise ValueError(f"Unregistered {kind} version")
            cursor.execute("SELECT dataset_id,status FROM dataset_version WHERE version_id=%s",
                           (request["input_version"],))
            dataset = cursor.fetchone()
            if not dataset or dataset["dataset_id"] != request["dataset_id"]:
                raise ValueError("Input does not belong to the requested dataset")
            if request.get("execution_mode") == "yarn" and dataset["status"] != "AVAILABLE":
                raise ValueError("YARN requires an imported and verified HDFS input version")
            cursor.execute("INSERT INTO logical_run "
                           "(run_id,dataset_id,input_version,rule_version,metric_version,idempotency_scope,"
                           "idempotency_key,request_hash,request,status,stage) "
                           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'QUEUED','queued') "
                           "ON DUPLICATE KEY UPDATE run_id=run_id",
                           (run_id, request["dataset_id"], request["input_version"], request["rule_version"],
                            request["metric_version"], scope, idempotency_key, digest, canonical(request)))
            created = cursor.rowcount == 1
            cursor.execute("SELECT * FROM logical_run WHERE idempotency_scope=%s AND idempotency_key=%s FOR UPDATE",
                           (scope, idempotency_key))
            row = cursor.fetchone()
            if row is None or row["request_hash"] != digest:
                raise Conflict("The same request key cannot identify different parameters or versions")
            if created:
                self.event(cursor, run_id, "SUBMITTED", {"request_hash": digest})
            return decoded(row), created

    def get_run(self, run_id):
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM logical_run WHERE run_id=%s", (run_id,))
            return decoded(cursor.fetchone())

    def list_runs(self, *, before=None, limit=50, status=None, dataset_id=None,
                  input_version=None, rule_version=None, metric_version=None):
        if not 1 <= limit <= 100 or (before is not None and before < 1):
            raise ValueError("Invalid history page")
        clauses, args = [], []
        if before is not None:
            clauses.append("r.request_seq < %s")
            args.append(before)
        for column,value in (('status',status),('dataset_id',dataset_id),('input_version',input_version),
                             ('rule_version',rule_version),('metric_version',metric_version)):
            if value is not None:
                if not isinstance(value,str) or not value or len(value)>100:
                    raise ValueError('Invalid history filter: '+column)
                clauses.append('r.'+column+' = %s')
                args.append(value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.transaction() as cursor:
            cursor.execute("SELECT r.run_id,r.request_seq,r.dataset_id,r.input_version,r.rule_version,"
                           "r.metric_version,r.status,r.stage,r.created_at,r.updated_at,r.error,"
                           "p.publish_id,p.output_version,c.publish_id AS current_publish_id "
                           "FROM logical_run r LEFT JOIN publish_version p ON p.run_id=r.run_id "
                           "AND p.status='PUBLISHED' LEFT JOIN dataset_current c ON c.dataset_id=r.dataset_id"
                           + where + " ORDER BY r.request_seq DESC LIMIT %s", (*args, limit + 1))
            rows = [decoded(row) for row in cursor.fetchall()]
        return {"items": rows[:limit], "next_before": rows[limit-1]["request_seq"] if len(rows) > limit else None}

    def query_evidence(self, attempt_id, *, after="", limit=50, source_table=None,
                       rule_id=None, source_record_id=None, metric=None, evidence_id=None):
        if not 1 <= limit <= 100:
            raise ValueError("Evidence page must contain 1 to 100 rows")
        clauses, args = ["attempt_id=%s", "evidence_id>%s"], [attempt_id, after]
        for column, value in (("source_table", source_table), ("rule_id", rule_id),
                              ("source_record_id", source_record_id), ("metric", metric),
                              ("evidence_id", evidence_id)):
            if value is not None:
                clauses.append(column + "=%s")
                args.append(value)
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM evidence_index WHERE " + " AND ".join(clauses)
                           + " ORDER BY evidence_id LIMIT %s", (*args, limit + 1))
            rows = [decoded(row) for row in cursor.fetchall()]
        return {"items": rows[:limit], "next_after": rows[limit-1]["evidence_id"] if len(rows) > limit else None}

    def get_input(self, version):
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM dataset_version WHERE version_id=%s", (version,))
            return decoded(cursor.fetchone())

    def current_run(self, dataset_id="movielens-1m"):
        with self.transaction() as cursor:
            cursor.execute("SELECT p.run_id FROM dataset_current c JOIN publish_version p "
                           "ON p.publish_id=c.publish_id JOIN logical_run r ON r.run_id=p.run_id "
                           "WHERE c.dataset_id=%s AND p.status='PUBLISHED' AND r.status='PUBLISHED'", (dataset_id,))
            row = cursor.fetchone()
        return row["run_id"] if row else None

    def current_result(self,dataset_id):
        if not isinstance(dataset_id,str) or not re.fullmatch('[A-Za-z0-9_-]{1,80}',dataset_id):
            raise ValueError('Invalid dataset identity')
        with self.transaction() as cursor:
            cursor.execute('SELECT c.dataset_id,c.publish_id,c.revision,c.request_seq,p.run_id,p.attempt_id,'
                'p.output_version,p.manifest_hash,r.input_version,r.rule_version,r.metric_version '
                'FROM dataset_current c JOIN publish_version p ON p.publish_id=c.publish_id '
                'JOIN logical_run r ON r.run_id=p.run_id '
                "WHERE c.dataset_id=%s AND p.status='PUBLISHED' AND r.status='PUBLISHED' "
                'AND p.attempt_id=r.active_attempt AND r.dataset_id=c.dataset_id',(dataset_id,))
            return cursor.fetchone()

    def mark_input_available(self, version, locations):
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM dataset_version WHERE version_id=%s FOR UPDATE", (version,))
            row = decoded(cursor.fetchone())
            if not row or set(locations) != set(row["manifest"]["tables"]):
                raise ValueError("All source tables must be verified before input becomes available")
            for table, location in locations.items():
                source = row["manifest"]["tables"][table]
                if location["sha256"] != source["sha256"] or location["bytes"] != source["bytes"]:
                    raise ValueError("Storage copy does not match the registered input")
                cursor.execute("INSERT INTO dataset_location (version_id,source_table,storage_uri,sha256,bytes) "
                               "VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE version_id=version_id",
                               (version, table, location["uri"], location["sha256"], location["bytes"]))
                cursor.execute("SELECT storage_uri,sha256,bytes FROM dataset_location WHERE version_id=%s AND source_table=%s",
                               (version, table))
                existing = cursor.fetchone()
                if existing != {"storage_uri": location["uri"], "sha256": location["sha256"], "bytes": location["bytes"]}:
                    raise Conflict("A registered raw location cannot be replaced in place")
            cursor.execute("UPDATE dataset_version SET status='AVAILABLE' WHERE version_id=%s", (version,))

    def input_locations(self, version):
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM dataset_location WHERE version_id=%s", (version,))
            return {row["source_table"]: row for row in cursor.fetchall()}

    def list_attempts(self, run_id):
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM physical_attempt WHERE run_id=%s ORDER BY attempt_no", (run_id,))
            return [decoded(row) for row in cursor.fetchall()]

    def list_jobs(self, attempt_id):
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM hadoop_job WHERE attempt_id=%s ORDER BY created_at,stage", (attempt_id,))
            return [decoded(row) for row in cursor.fetchall()]

    def claim(self, owner, *, lease_seconds=30, environment=None):
        if not 1 <= lease_seconds <= 3600:
            raise ValueError("Lease duration outside supported range")
        with self.transaction() as cursor:
            # A single durable slot serializes execution across API/worker processes.
            control = self.admission(cursor)
            if control.get('switch_id'):
                return None
            cursor.execute("SELECT *,lease_until>NOW(6) AS alive FROM worker_slot WHERE slot_id=1 FOR UPDATE")
            slot = cursor.fetchone()
            if slot["alive"]:
                return None
            row = None
            if slot["run_id"]:
                cursor.execute("SELECT * FROM logical_run WHERE run_id=%s FOR UPDATE", (slot["run_id"],))
                row = cursor.fetchone()
                if row["status"] in TERMINAL:
                    row = None
            if row is None:
                if not control['admission_open']:
                    return None
                cursor.execute("SELECT * FROM logical_run WHERE status='QUEUED' ORDER BY request_seq LIMIT 1 FOR UPDATE")
                row = cursor.fetchone()
                if row is None:
                    return None
            if row['status']=='QUEUED' and not control['admission_open']:
                return None
            recovering = row["status"] != "QUEUED"
            token = row["fencing_token"] + 1
            if recovering:
                attempt_id = row["active_attempt"]
                cursor.execute("SELECT work_path FROM physical_attempt WHERE attempt_id=%s", (attempt_id,))
                work_path = cursor.fetchone()["work_path"]
                status = "RECOVERING" if row["status"] in {"RUNNING", "RECOVERING"} else row["status"]
                cursor.execute("UPDATE physical_attempt SET fencing_token=%s,status=%s WHERE attempt_id=%s",
                               (token, status, attempt_id))
            else:
                attempt_id = "attempt-" + uuid.uuid4().hex
                cursor.execute("SELECT COALESCE(MAX(attempt_no),0)+1 AS n FROM physical_attempt WHERE run_id=%s", (row["run_id"],))
                attempt_no = cursor.fetchone()["n"]
                work_path = ("outputs/" + row["run_id"] + "/attempts/" + attempt_id)
                status = "RUNNING"
                cursor.execute("INSERT INTO physical_attempt "
                               "(attempt_id,run_id,attempt_no,fencing_token,status,stage,work_path,environment) "
                               "VALUES (%s,%s,%s,%s,%s,'starting',%s,%s)",
                               (attempt_id, row["run_id"], attempt_no, token, status, work_path, canonical(environment or {})))
            cursor.execute("UPDATE logical_run SET active_attempt=%s,fencing_token=%s,lease_owner=%s,"
                           "lease_until=DATE_ADD(NOW(6),INTERVAL %s SECOND),status=%s,updated_at=NOW(6) WHERE run_id=%s",
                           (attempt_id, token, owner, lease_seconds, status, row["run_id"]))
            cursor.execute("UPDATE worker_slot SET run_id=%s,lease_owner=%s,lease_until=DATE_ADD(NOW(6),INTERVAL %s SECOND) "
                           "WHERE slot_id=1", (row["run_id"], owner, lease_seconds))
            self.event(cursor, row["run_id"], "RECOVERY_CLAIMED" if recovering else "ATTEMPT_STARTED",
                       {"owner": owner, "token": token}, attempt_id)
            return Claim(row["run_id"], attempt_id, token, owner, recovering, work_path, status)

    @staticmethod
    def require_lease(cursor, claim):
        cursor.execute("SELECT *,lease_until>NOW(6) AS alive FROM logical_run WHERE run_id=%s FOR UPDATE", (claim.run_id,))
        row = cursor.fetchone()
        if (not row or row["status"] in TERMINAL or not row["alive"] or row["lease_owner"] != claim.owner or
                row["fencing_token"] != claim.token or row["active_attempt"] != claim.attempt_id):
            raise LostLease("Execution lease expired or was replaced; state update rejected")
        return row

    def heartbeat(self, claim, lease_seconds=30):
        with self.transaction() as cursor:
            self.require_lease(cursor, claim)
            cursor.execute("UPDATE logical_run SET lease_until=DATE_ADD(NOW(6),INTERVAL %s SECOND) WHERE run_id=%s",
                           (lease_seconds, claim.run_id))
            cursor.execute("UPDATE worker_slot SET lease_until=DATE_ADD(NOW(6),INTERVAL %s SECOND) "
                           "WHERE slot_id=1 AND run_id=%s AND lease_owner=%s", (lease_seconds, claim.run_id, claim.owner))

    def advance(self, claim, stage, status=None, error=None):
        if status == "PUBLISHED":
            raise ValueError("Only the verified publication transaction may mark a run PUBLISHED")
        with self.transaction() as cursor:
            row = self.require_lease(cursor, claim)
            status = status or row["status"]
            if status != row["status"] and status not in TRANSITIONS.get(row["status"], set()):
                raise ValueError(f"Invalid business transition: {row['status']} -> {status}")
            cursor.execute("UPDATE logical_run SET stage=%s,status=%s,error=%s,updated_at=NOW(6) WHERE run_id=%s",
                           (stage, status, canonical(error) if error else None, claim.run_id))
            cursor.execute("UPDATE physical_attempt SET stage=%s,status=%s,error=%s,"
                           "ended_at=CASE WHEN %s IN ('FAILED','PUBLISHED') THEN NOW(6) ELSE ended_at END WHERE attempt_id=%s",
                           (stage, status, canonical(error) if error else None, status, claim.attempt_id))
            self.event(cursor, claim.run_id, "STAGE_CHANGED", {"stage": stage, "status": status, "error": error}, claim.attempt_id)
            if status in TERMINAL:
                cursor.execute("UPDATE worker_slot SET run_id=NULL,lease_owner=NULL,lease_until=NULL WHERE slot_id=1 AND run_id=%s",
                               (claim.run_id,))

    def prepare_job(self, claim, stage, specification):
        submission_id = "ml-" + fingerprint({"attempt": claim.attempt_id, "stage": stage})[:48]
        with self.transaction() as cursor:
            self.require_lease(cursor, claim)
            cursor.execute("INSERT INTO hadoop_job (submission_id,attempt_id,stage,job_name,status,specification) "
                           "VALUES (%s,%s,%s,%s,'SUBMITTING',%s) ON DUPLICATE KEY UPDATE submission_id=submission_id",
                           (submission_id, claim.attempt_id, stage, submission_id, canonical(specification)))
            created = cursor.rowcount == 1
            cursor.execute("SELECT * FROM hadoop_job WHERE submission_id=%s", (submission_id,))
            row = decoded(cursor.fetchone())
            row["created"] = created
            if row["specification"] != specification:
                raise Conflict("An attempt stage cannot be submitted with a different job specification")
            return row

    def update_job(self, claim, submission_id, status, *, job_id=None, application_id=None, detail=None):
        with self.transaction() as cursor:
            self.require_lease(cursor, claim)
            cursor.execute("SELECT * FROM hadoop_job WHERE submission_id=%s AND attempt_id=%s FOR UPDATE",
                           (submission_id, claim.attempt_id))
            row = cursor.fetchone()
            if row is None:
                raise ValueError("Unknown job for this attempt")
            for key, value in (("job_id", job_id), ("application_id", application_id)):
                if row[key] and value and row[key] != value:
                    raise Conflict("A durable submission cannot refer to two external jobs")
            if row["status"] in {"SUCCEEDED", "FAILED", "KILLED"} and row["status"] != status:
                raise Conflict("A terminal job state cannot be replaced by a transient observation")
            cursor.execute("UPDATE hadoop_job SET status=%s,job_id=COALESCE(job_id,%s),"
                           "application_id=COALESCE(application_id,%s),detail=%s,updated_at=NOW(6) WHERE submission_id=%s",
                           (status, job_id, application_id, canonical(detail or {}), submission_id))

    def retry(self, run_id):
        with self.transaction() as cursor:
            if not self.admission(cursor)['admission_open']:
                raise AdmissionPaused('Retries are paused for maintenance')
            cursor.execute("SELECT * FROM logical_run WHERE run_id=%s FOR UPDATE", (run_id,))
            row = cursor.fetchone()
            if not row or row["status"] != "FAILED":
                raise Conflict("Only a confirmed failed run can start a new attempt")
            cursor.execute("SELECT publish_id FROM publish_version WHERE run_id=%s", (run_id,))
            if cursor.fetchone():
                raise Conflict("A publication intent requires reconciliation, not a fresh computation")
            cursor.execute("UPDATE logical_run SET status='QUEUED',stage='queued',error=NULL,"
                           "lease_owner=NULL,lease_until=NULL,updated_at=NOW(6) WHERE run_id=%s", (run_id,))
            self.event(cursor, run_id, "RETRY_REQUESTED", {"previous_attempt": row["active_attempt"]})

    def request_publication_replacement(self, run_id, *, expected_publish, expected_attempt,
                                        expected_manifest_hash, expected_token, reason):
        """Explicit CAS authorization, only after execution has stopped and jobs are terminal.

        Keep the original intent unchanged until the replacement passes the full gate.
        No frozen request material or old files are rewritten.
        """
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
            raise ValueError("A bounded operator reason is required")
        with self.transaction() as cursor:
            if self.admission(cursor).get('switch_id'):
                raise Conflict('Publication replacement is blocked during a reserved service switch')
            cursor.execute("SELECT *,lease_until>NOW(6) AS alive FROM worker_slot WHERE slot_id=1 FOR UPDATE")
            slot = cursor.fetchone()
            # Reject before taking the Run lock: a live heartbeat takes Run
            # then slot, so waiting for its Run while holding slot can deadlock.
            if slot["run_id"] == run_id and slot["alive"]:
                raise Conflict("A live executor must finish or relinquish its lease before replacement")
            cursor.execute("SELECT *,lease_until>NOW(6) AS alive FROM logical_run WHERE run_id=%s FOR UPDATE", (run_id,))
            run = cursor.fetchone()
            cursor.execute("SELECT * FROM publish_version WHERE run_id=%s FOR UPDATE", (run_id,))
            intent = decoded(cursor.fetchone())
            if (not run or not intent or intent["status"] != "PREPARING" or run["status"] not in {"PUBLISHING", "FAILED"}
                    or intent["publish_id"] != expected_publish or intent["manifest_hash"] != expected_manifest_hash
                    or run["active_attempt"] != expected_attempt or run["fencing_token"] != expected_token):
                raise Conflict("Publication replacement expectation changed or is already confirmed")
            if (run["alive"] and run["status"] != "FAILED") or (slot["run_id"] == run_id and slot["alive"]):
                raise Conflict("A live executor must finish or relinquish its lease before replacement")
            cursor.execute("SELECT COUNT(*) AS n FROM hadoop_job WHERE attempt_id=%s AND status NOT IN ('SUCCEEDED','FAILED','KILLED')",
                           (expected_attempt,))
            if cursor.fetchone()["n"]:
                raise Conflict("External job state must be reconciled before replacing an Attempt")
            cursor.execute("SELECT COALESCE(MAX(attempt_no),0)+1 AS n FROM physical_attempt WHERE run_id=%s", (run_id,))
            attempt_no = cursor.fetchone()["n"]
            cursor.execute("UPDATE physical_attempt SET status='FAILED',stage='publication-replacement',error=%s,ended_at=NOW(6) "
                           "WHERE attempt_id=%s AND status NOT IN ('FAILED','PUBLISHED')",
                           (canonical({"reason": reason}), expected_attempt))
            cursor.execute("UPDATE logical_run SET status='QUEUED',stage='publication-replacement-queued',error=NULL,"
                           "fencing_token=fencing_token+1,lease_owner=NULL,lease_until=NULL,updated_at=NOW(6) WHERE run_id=%s", (run_id,))
            cursor.execute("UPDATE worker_slot SET run_id=NULL,lease_owner=NULL,lease_until=NULL WHERE slot_id=1 AND run_id=%s", (run_id,))
            self.event(cursor, run_id, "PUBLICATION_REPLACEMENT_REQUESTED", {
                "publish_id": expected_publish, "manifest_hash": expected_manifest_hash,
                "previous_attempt": expected_attempt, "expected_attempt_no": attempt_no,
                "revoked_token": expected_token, "reason": reason}, expected_attempt)

    def index_evidence_batch(self, claim, rows):
        """Append immutable positions under the current lease; recovery verifies duplicates."""
        columns = ("evidence_id", "attempt_id", "source_record_id", "source_table", "rule_id", "metric",
                   "file_path", "row_group", "row_in_group", "detail")
        if not rows:
            return
        if len(rows) > 500 or len({row["evidence_id"] for row in rows}) != len(rows):
            raise ValueError("Evidence batches must contain at most 500 distinct identities")
        with self.transaction() as cursor:
            run = self.require_lease(cursor, claim)
            if run["status"] not in {"VALIDATING", "PUBLISHING"}:
                raise Conflict("Evidence may only be indexed during validation/publication")
            for row in rows:
                if row["attempt_id"] != claim.attempt_id or row["detail"].get("run_id") != claim.run_id:
                    raise Conflict("Evidence belongs to another execution")
                for key in ("input_version", "rule_version", "metric_version"):
                    if row["detail"].get(key) != run[key]:
                        raise Conflict("Evidence version differs from the fixed Run")
            placeholders = ",".join(["%s"] * len(rows))
            cursor.execute(f"SELECT * FROM evidence_index WHERE evidence_id IN ({placeholders})",
                           tuple(row["evidence_id"] for row in rows))
            existing = {row["evidence_id"]: decoded(row) for row in cursor.fetchall()}
            additions = []
            for row in rows:
                old = existing.get(row["evidence_id"])
                if old:
                    if any(old[key] != row[key] for key in columns):
                        raise Conflict("An evidence identity cannot change content or position")
                else:
                    additions.append(tuple(canonical(row[key]) if key == "detail" else row[key] for key in columns))
            if additions:
                cursor.executemany("INSERT INTO evidence_index (" + ",".join(columns) + ") VALUES (" +
                                   ",".join(["%s"] * len(columns)) + ")", additions)

    def record_quality_batch(self, claim, rows):
        columns = ("attempt_id", "phase", "source_table", "metric", "metric_version", "numerator", "denominator", "score", "detail")
        with self.transaction() as cursor:
            run = self.require_lease(cursor, claim)
            if run["status"] not in {"VALIDATING", "PUBLISHING"}:
                raise Conflict("Quality results require a validating execution")
            for row in rows:
                if row["attempt_id"] != claim.attempt_id or row["metric_version"] != run["metric_version"]:
                    raise Conflict("Quality result version/execution mismatch")
                values = {**row, "score": Decimal(str(row["score"])) if row["score"] is not None else None}
                cursor.execute("SELECT * FROM quality_result WHERE attempt_id=%s AND phase=%s AND source_table=%s AND metric=%s",
                               tuple(values[key] for key in columns[:4]))
                old = decoded(cursor.fetchone())
                if old:
                    if any(old[key] != values[key] for key in columns):
                        raise Conflict("A quality result cannot change after registration")
                else:
                    cursor.execute("INSERT INTO quality_result (" + ",".join(columns) + ") VALUES (" +
                                   ",".join(["%s"] * len(columns)) + ")",
                                   tuple(canonical(values[key]) if key == "detail" else values[key] for key in columns))

    def evidence_positions(self, attempt_id, *, after="", limit=500):
        if not 1 <= limit <= 500:
            raise ValueError("Evidence page size must be between 1 and 500")
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM evidence_index WHERE attempt_id=%s AND evidence_id>%s ORDER BY evidence_id LIMIT %s",
                           (attempt_id, after, limit))
            return [decoded(row) for row in cursor.fetchall()]

    def get_publish(self, run_id):
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM publish_version WHERE run_id=%s", (run_id,))
            return decoded(cursor.fetchone())

    def evidence_group_positions(self, attempt_id, file_path, row_group):
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM evidence_index WHERE attempt_id=%s AND file_path=%s AND row_group=%s "
                           "ORDER BY row_in_group LIMIT 5001", (attempt_id, file_path, row_group))
            return [decoded(row) for row in cursor.fetchall()]

    def evidence_count(self, attempt_id):
        with self.transaction() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM evidence_index WHERE attempt_id=%s", (attempt_id,))
            return cursor.fetchone()["n"]

    def storage_references(self):
        """Read-only namespace reconciliation; terminal attempts remain historical references."""
        with self.transaction() as cursor:
            cursor.execute("SELECT version_id,source_table,storage_uri FROM dataset_location")
            raw = cursor.fetchall()
            cursor.execute("SELECT a.run_id,a.attempt_id,a.status,a.ended_at,r.active_attempt,"
                           "r.lease_until>NOW(6) AS lease_alive "
                           "FROM physical_attempt a JOIN logical_run r ON r.run_id=a.run_id")
            attempts = cursor.fetchall()
            cursor.execute("SELECT * FROM publish_version WHERE storage_path LIKE '/ml/%'")
            publications = [decoded(row) for row in cursor.fetchall()]
            cursor.execute("SELECT detail FROM run_event WHERE event_type='PUBLICATION_QUALIFICATION_TRANSFERRED'")
            for event in cursor.fetchall():
                previous = decoded(event)["detail"]["previous_intent"]
                publications.append(previous)
        return {"scope": self.config["database"], "raw": raw, "attempts": attempts, "publications": publications}

    def prepare_publication(self, claim, manifest, manifest_hash):
        """Fix one immutable publication intent after the caller's complete gate."""
        with self.transaction() as cursor:
            run = self.require_lease(cursor, claim)
            if run["status"] not in {"VALIDATING", "PUBLISHING"}:
                raise Conflict("Publication requires validated candidate state")
            expected = {"run_id": claim.run_id, "attempt_id": claim.attempt_id,
                        **{key: run[key] for key in ("input_version", "rule_version", "metric_version")}}
            if any(manifest.get(key) != value for key, value in expected.items()):
                raise Conflict("Publication manifest belongs to another execution")
            if hashlib.sha256(canonical(manifest).encode("utf-8")).hexdigest() != manifest_hash:
                raise Conflict("Publication manifest hash does not identify its content")
            cursor.execute("SELECT * FROM publish_version WHERE run_id=%s FOR UPDATE", (claim.run_id,))
            intent = decoded(cursor.fetchone())
            if intent:
                if intent["attempt_id"] != claim.attempt_id:
                    cursor.execute("SELECT detail FROM run_event WHERE run_id=%s AND event_type='PUBLICATION_REPLACEMENT_REQUESTED' "
                                   "ORDER BY event_id DESC LIMIT 1", (claim.run_id,))
                    marker = decoded(cursor.fetchone())
                    cursor.execute("SELECT attempt_no FROM physical_attempt WHERE attempt_id=%s", (claim.attempt_id,))
                    attempt = cursor.fetchone()
                    detail = marker["detail"] if marker else {}
                    if (intent["status"] != "PREPARING" or detail.get("publish_id") != intent["publish_id"]
                            or detail.get("manifest_hash") != intent["manifest_hash"]
                            or not attempt or detail.get("expected_attempt_no") != attempt["attempt_no"]
                            or claim.token <= detail.get("revoked_token", claim.token)):
                        raise Conflict("Candidate replacement has no current operator authorization")
                    previous = {key: value.isoformat() if hasattr(value, "isoformat") else value
                                for key, value in intent.items()}
                    # A distinct immutable generation prevents collision with partial old files.
                    base = intent["storage_path"].split("/generation=", 1)[0]
                    path = base + "/generation=" + claim.attempt_id
                    cursor.execute("UPDATE publish_version SET attempt_id=%s,fencing_token=%s,manifest=%s,manifest_hash=%s,"
                                   "storage_path=%s,report_path=%s WHERE publish_id=%s AND status='PREPARING'",
                                   (claim.attempt_id, claim.token, canonical(manifest), manifest_hash, path, path + "/report.json", intent["publish_id"]))
                    self.event(cursor, claim.run_id, "PUBLICATION_QUALIFICATION_TRANSFERRED", {
                        "previous_intent": previous, "new_attempt": claim.attempt_id,
                        "new_manifest_hash": manifest_hash, "reason": detail.get("reason")}, claim.attempt_id)
                    intent.update(attempt_id=claim.attempt_id, fencing_token=claim.token, manifest=manifest,
                                  manifest_hash=manifest_hash, storage_path=path, report_path=path + "/report.json")
                elif intent["manifest_hash"] != manifest_hash or intent["manifest"] != manifest:
                    raise Conflict("A Run publication intent cannot switch to a different candidate")
                cursor.execute("UPDATE publish_version SET fencing_token=%s WHERE publish_id=%s AND status='PREPARING'",
                               (claim.token, intent["publish_id"]))
                intent["fencing_token"] = claim.token
            else:
                output = "clean-" + claim.run_id
                # Identifiers are encoded to avoid interpreting dataset names as paths.
                dataset_key = fingerprint(run["dataset_id"])[:32]
                path = f"/ml/published/dataset={dataset_key}/version={output}"
                intent = {"publish_id": "publish-" + uuid.uuid5(uuid.NAMESPACE_URL, claim.run_id).hex,
                          "run_id": claim.run_id, "attempt_id": claim.attempt_id, "output_version": output,
                          "fencing_token": claim.token, "status": "PREPARING", "manifest": manifest,
                          "manifest_hash": manifest_hash, "storage_path": path, "report_path": path + "/report.json"}
                cursor.execute("INSERT INTO publish_version (publish_id,run_id,attempt_id,output_version,fencing_token,status,manifest,manifest_hash,storage_path,report_path) "
                               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                               tuple(canonical(intent[key]) if key == "manifest" else intent[key] for key in
                                     ("publish_id", "run_id", "attempt_id", "output_version", "fencing_token", "status", "manifest", "manifest_hash", "storage_path", "report_path")))
            cursor.execute("UPDATE logical_run SET status='PUBLISHING',stage='materializing-publication',updated_at=NOW(6) WHERE run_id=%s", (claim.run_id,))
            cursor.execute("UPDATE physical_attempt SET status='PUBLISHING',stage='materializing-publication' WHERE attempt_id=%s", (claim.attempt_id,))
            self.event(cursor, claim.run_id, "PUBLICATION_INTENT", {"publish_id": intent["publish_id"], "manifest_hash": manifest_hash, "token": claim.token}, claim.attempt_id)
            return intent

    def confirm_publication(self, claim, publish_id, verification):
        """Finalize only the fixed intent after every official HDFS file was verified."""
        with self.transaction() as cursor:
            run = self.require_lease(cursor, claim)
            cursor.execute("SELECT * FROM publish_version WHERE publish_id=%s AND run_id=%s FOR UPDATE", (publish_id, claim.run_id))
            intent = decoded(cursor.fetchone())
            if (not intent or intent["status"] != "PREPARING" or run["status"] != "PUBLISHING"
                    or intent["attempt_id"] != claim.attempt_id or intent["fencing_token"] != claim.token):
                raise Conflict("Publication intent is not owned by this execution")
            expected_files = {item["path"]: {"sha256": item["sha256"], "bytes": item["bytes"]} for item in intent["manifest"]["files"]}
            expected_files["manifest.json"] = {"sha256": intent["manifest_hash"], "bytes": len(canonical(intent["manifest"]).encode("utf-8"))}
            if verification != expected_files:
                raise Conflict("Official storage verification does not cover the fixed manifest")
            cursor.execute("SELECT COUNT(*) AS n FROM quality_result WHERE attempt_id=%s", (claim.attempt_id,))
            if cursor.fetchone()["n"] != 30:
                raise Conflict("Formal quality summary is incomplete")
            expected_evidence = sum(item["rows"] for item in intent["manifest"]["files"] if item["path"].startswith("evidence/"))
            cursor.execute("SELECT COUNT(*) AS n FROM evidence_index WHERE attempt_id=%s", (claim.attempt_id,))
            if cursor.fetchone()["n"] != expected_evidence:
                raise Conflict("Formal evidence index is incomplete")
            cursor.execute("INSERT INTO dataset_version (version_id,dataset_id,parent_version_id,content_hash,manifest,status) VALUES (%s,%s,%s,%s,%s,'PUBLISHED')",
                           (intent["output_version"], run["dataset_id"], run["input_version"], intent["manifest_hash"], canonical(intent["manifest"])))
            cursor.execute("UPDATE publish_version SET status='PUBLISHED',confirmed_at=NOW(6) WHERE publish_id=%s", (publish_id,))
            cursor.execute("UPDATE logical_run SET status='PUBLISHED',stage='published',error=NULL,lease_owner=NULL,lease_until=NULL,updated_at=NOW(6) WHERE run_id=%s", (claim.run_id,))
            cursor.execute("UPDATE physical_attempt SET status='PUBLISHED',stage='published',error=NULL,ended_at=NOW(6) WHERE attempt_id=%s", (claim.attempt_id,))
            self.event(cursor, claim.run_id, "PUBLISHED", {"publish_id": publish_id, "output_version": intent["output_version"]}, claim.attempt_id)
            cursor.execute("UPDATE worker_slot SET run_id=NULL,lease_owner=NULL,lease_until=NULL WHERE slot_id=1 AND run_id=%s", (claim.run_id,))
            return intent["output_version"]

    def promote_current(self, publish_id, *, expected_publish=EXPECTED_CURRENT_UNSET):
        """Separate promotion: CAS expectation and request order protect newer current."""
        with self.transaction() as cursor:
            cursor.execute("SELECT p.*,r.dataset_id,r.request_seq FROM publish_version p JOIN logical_run r ON r.run_id=p.run_id WHERE p.publish_id=%s", (publish_id,))
            publish = cursor.fetchone()
            if not publish or publish["status"] != "PUBLISHED":
                raise Conflict("Only confirmed publication may become current")
            cursor.execute("SELECT dataset_id FROM dataset WHERE dataset_id=%s FOR UPDATE", (publish["dataset_id"],))
            cursor.fetchone()
            cursor.execute("SELECT * FROM dataset_current WHERE dataset_id=%s FOR UPDATE", (publish["dataset_id"],))
            current = cursor.fetchone()
            if expected_publish is not EXPECTED_CURRENT_UNSET and (current["publish_id"] if current else None) != expected_publish:
                raise Conflict("Current version changed before promotion")
            if current and current["publish_id"] == publish_id:
                return True
            if current and current["request_seq"] >= publish["request_seq"]:
                self.event(cursor, publish["run_id"], "CURRENT_SKIPPED", {"current": current["publish_id"], "reason": "older request"})
                return False
            if current:
                cursor.execute("UPDATE dataset_current SET publish_id=%s,request_seq=%s,revision=revision+1,updated_at=NOW(6) WHERE dataset_id=%s AND revision=%s",
                               (publish_id, publish["request_seq"], publish["dataset_id"], current["revision"]))
            else:
                cursor.execute("INSERT INTO dataset_current (dataset_id,publish_id,request_seq,revision) VALUES (%s,%s,%s,1)",
                               (publish["dataset_id"], publish_id, publish["request_seq"]))
            self.event(cursor, publish["run_id"], "CURRENT_PROMOTED", {"publish_id": publish_id, "previous": current["publish_id"] if current else None})
            return True

    def reconcile_current(self):
        """Recover the separate confirmation/promotion window without recomputation."""
        with self.transaction() as cursor:
            cursor.execute("SELECT p.publish_id FROM publish_version p JOIN logical_run r ON r.run_id=p.run_id "
                           "LEFT JOIN dataset_current c ON c.dataset_id=r.dataset_id "
                           "WHERE p.status='PUBLISHED' AND (c.publish_id IS NULL OR c.request_seq<r.request_seq) "
                           "ORDER BY r.request_seq DESC LIMIT 20")
            pending = [row["publish_id"] for row in cursor.fetchall()]
        for publish_id in pending:
            self.promote_current(publish_id)
        return len(pending)
