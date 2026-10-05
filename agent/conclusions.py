"""Validate structured Agent facts; render formal claims from verified values only."""
import json
import math
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from agent.published import read_published_report, ReportUnavailable
from agent.results import read_evidence


class ReportClaim(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    path: str = Field(description="报告中标量字段的 JSON Pointer，例如 /scores/users/clean/unique")
    value: str | int | float | None


class EvidenceClaim(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    evidence_id: str
    source_record_id: str
    source_table: Literal["users", "movies", "ratings"]
    rule_id: str
    metric: str | None
    action: str | None
    before: str | None
    after: str | None
    final_disposition: str | None


class ConclusionProposal(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    run_id: str
    publish_id: str
    input_version: str
    rule_version: str
    metric_version: str
    report_claims: list[ReportClaim]
    evidence_claims: list[EvidenceClaim]
    hypotheses: list[str] = Field(description="未经证据证明的推测，不能写入正式事实")


def report_value(report, pointer):
    if not pointer.startswith("/") or "~" in pointer:
        raise ValueError("INVALID_REPORT_POINTER")
    parts = pointer[1:].split("/")
    if parts[0] not in {"scores", "dataset_composite", "row_change", "split", "disposition", "limitations"}:
        raise ValueError("REPORT_FIELD_NOT_ALLOWED")
    value = report
    for part in parts:
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            raise ValueError("REPORT_FIELD_NOT_FOUND")
    if isinstance(value, (dict, list, bool)) or not isinstance(value, (str, int, float, type(None))):
        raise ValueError("REPORT_FIELD_NOT_SCALAR")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("REPORT_VALUE_NOT_FINITE")
    return value


def validate_conclusion(run_id, proposal, *, report_reader=None, evidence_reader=None):
    proposal = proposal if isinstance(proposal, ConclusionProposal) else ConclusionProposal.model_validate(proposal)
    if len(proposal.report_claims) > 50 or len(proposal.evidence_claims) > 20 or len(proposal.hypotheses) > 10:
        raise ValueError("CONCLUSION_TOO_LARGE")
    report_reader = report_reader or read_published_report
    evidence_reader = evidence_reader or read_evidence
    _, report, publication = report_reader(run_id)
    identity = {"run_id": run_id, "publish_id": publication["publish_id"],
                "input_version": report["input_data_version"], "rule_version": report["rule_version"],
                "metric_version": report["metric_version"]}
    if any(getattr(proposal, key) != value for key, value in identity.items()):
        raise ValueError("CONCLUSION_VERSION_BINDING_MISMATCH")
    accepted, rejected, lines = [], [], []
    seen = set()
    for claim in proposal.report_claims:
        try:
            value = report_value(report, claim.path)
            # A correct number from another field cannot validate this named claim.
            if value != claim.value or (value is None) != (claim.value is None) or type(value) is str and type(claim.value) is not str:
                raise ValueError("REPORT_VALUE_MISMATCH")
            key = "report:" + claim.path
            if key in seen:
                continue
            seen.add(key)
            accepted.append({"kind": "report", "path": claim.path, "value": value, **identity})
            lines.append(f"报告字段 {claim.path}：{json.dumps(value, ensure_ascii=False)}。")
        except ValueError as error:
            rejected.append({"kind": "report", "path": claim.path, "reason": str(error)})
    for claim in proposal.evidence_claims:
        try:
            page = evidence_reader(run_id, evidence_id=claim.evidence_id, limit=1)
            if page["publish_id"] != identity["publish_id"] or len(page["items"]) != 1:
                raise ValueError("EVIDENCE_NOT_FOUND_IN_PUBLICATION")
            body = page["items"][0]
            if any(body.get(key) != value for key, value in claim.model_dump().items()):
                raise ValueError("EVIDENCE_FACT_MISMATCH")
            if claim.evidence_id in seen:
                continue
            seen.add(claim.evidence_id)
            accepted.append({"kind": "evidence", "facts": claim.model_dump(), **identity})
            # Reasons are the stored rule/action facts. Arbitrary model prose never
            # becomes a verified cause by citing an otherwise valid evidence ID.
            lines.append(f"记录 {body['source_record_id']}（{body['source_table']}）：规则 {body['rule_id']}，"
                         f"动作 {body.get('action')}，最终处置 {body.get('final_disposition')}；"
                         f"处理前 {json.dumps(body.get('before'), ensure_ascii=False)}，"
                         f"处理后 {json.dumps(body.get('after'), ensure_ascii=False)}。"
                         f"[evidence:{body['evidence_id']}]")
        except (ValueError, ReportUnavailable) as error:
            rejected.append({"kind": "evidence", "evidence_id": claim.evidence_id, "reason": str(error)})
    if not lines:
        lines.append("当前材料无法形成可核验的正式结论。")
    if report.get("limitations"):
        lines.append("报告局限：" + json.dumps(report["limitations"], ensure_ascii=False))
    return {"answer": "\n".join(lines), "accepted_claims": accepted, "rejected_claims": rejected,
            "hypotheses": proposal.hypotheses, "hypotheses_verified": False,
            "validation_status": "REJECTED" if not accepted else "PARTIAL" if rejected else "VERIFIED",
            "flagged": [r["reason"] for r in rejected], **identity}
