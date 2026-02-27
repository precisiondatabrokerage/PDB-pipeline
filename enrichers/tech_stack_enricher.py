import os
import requests

API_KEY = os.getenv("WAPPALYZER_API_KEY")

def enrich_tech_stack(url: str | None) -> dict:
    """
    Uses Wappalyzer (if API key exists) to identify technologies used on a website.
    """
    if not url or not API_KEY:
        return {"website_tech_stack": None}

    endpoint = "https://api.wappalyzer.com/v2/lookup/"

    try:
        resp = requests.get(endpoint, params={"url": url}, headers={"x-api-key": API_KEY})
        tech = resp.json()
        return {"website_tech_stack": tech}
    except:
        return {"website_tech_stack": None}
