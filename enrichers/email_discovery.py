import re

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

def discover_emails_from_text(text: str) -> list[str]:
    """
    Returns all emails found in any text block.
    """
    if not text:
        return []
    return EMAIL_REGEX.findall(text)


def choose_best_email(entity: dict, fallback: str | None=None) -> str | None:
    """
    Picks the best email from scraped and enriched fields.
    """
    candidates = []

    if entity.get("email_primary"):
        candidates.append(entity.get("email_primary"))

    if entity.get("email_secondary"):
        candidates.append(entity.get("email_secondary"))

    # from website footer or text dump
    if entity.get("raw_records"):
        for r in entity["raw_records"]:
            if r.get("raw_email"):
                candidates.append(r["raw_email"])

    # from enrichment
    if entity.get("discovered_emails"):
        candidates.extend(entity["discovered_emails"])

    return candidates[0] if candidates else fallback
