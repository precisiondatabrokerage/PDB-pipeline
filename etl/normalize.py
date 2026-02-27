import re


STATE_RE = re.compile(r"\b([A-Z]{2})\b")
ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
PHONE_RE = re.compile(r"\+?1?\D*(\d{3})\D*(\d{3})\D*(\d{4})")


def normalize_phone(phone: str | None) -> str | None:
    """
    Normalize US phone numbers to digits-only (E.164-ish without +1).
    """
    if not phone:
        return None

    m = PHONE_RE.search(phone)
    if not m:
        return None

    return "".join(m.groups())


def normalize(record: dict) -> dict:
    """
    Production-grade normalization (v2).

    - Preserves raw fields
    - Extracts structured address components
    - Normalizes phone numbers
    """

    raw_addr = (record.get("raw_address") or "").strip()
    raw_phone = record.get("raw_phone")

    # -----------------------------
    # Address parsing (best-effort)
    # -----------------------------
    street = city = state = zip_code = None

    # Split on commas but tolerate weird spacing
    parts = [p.strip() for p in re.split(r",|\n", raw_addr) if p.strip()]

    if parts:
        street = parts[0]

    if len(parts) >= 2:
        city = parts[1]

    # State + ZIP can appear anywhere
    state_match = STATE_RE.search(raw_addr.upper())
    zip_match = ZIP_RE.search(raw_addr)

    if state_match:
        state = state_match.group(1)

    if zip_match:
        zip_code = zip_match.group(1)

    # -----------------------------
    # Phone normalization
    # -----------------------------
    phone_normalized = normalize_phone(raw_phone)

    # -----------------------------
    # Write normalized fields
    # -----------------------------
    record["mailing_address"] = raw_addr or None
    record["mailing_street"] = street
    record["mailing_city"] = city
    record["mailing_state"] = state
    record["mailing_zip"] = zip_code

    if phone_normalized:
        record["raw_phone"] = phone_normalized

    return record
