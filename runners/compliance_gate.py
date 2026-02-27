# PDB-pipeline/runners/compliance_gate.py

from datetime import datetime, timezone
from db.mongo_client import get_mongo


class ComplianceError(Exception):
    pass


def utcnow():
    return datetime.now(timezone.utc)


def validate_source(source_key: str) -> bool:
    """
    Validates a source against the Mongo source_registry.
    Hard-stops ingestion if automation is not permitted.
    """

    mongo = get_mongo()
    db = mongo.db

    registry = db["source_registry"]
    source = registry.find_one({"source_key": source_key})

    if not source:
        raise ComplianceError(f"Source not registered: {source_key}")

    if not source.get("enabled", False):
        raise ComplianceError(f"Source disabled: {source_key}")

    if not source.get("automation_allowed", False):
        raise ComplianceError(
            f"Automation not permitted for source: {source_key}"
        )

    if source.get("tos_prohibits_mining", False):
        raise ComplianceError(
            f"Source prohibits automated extraction: {source_key}"
        )

    # Optional: enforce legal review freshness if desired
    # Example:
    # last_review = source.get("last_legal_review")
    # if last_review and (utcnow() - last_review).days > 180:
    #     raise ComplianceError("Legal review expired for source")

    print(f"✅ Compliance validated for source: {source_key}")
    return True
