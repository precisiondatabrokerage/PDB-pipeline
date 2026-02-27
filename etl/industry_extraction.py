def classify_industry(entity: dict) -> tuple[str, str | None]:
    """
    Assigns industry_type and sub_industry based on company name and attributes.
    """
    name = (entity.get("canonical_name") or "").lower()

    if "hoa" in name:
        return ("property_management", "hoa_management")

    if "property" in name or "pm " in name or "pm, " in name:
        return ("property_management", None)

    return ("unknown", None)
