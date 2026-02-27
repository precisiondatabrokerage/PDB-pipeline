import requests, os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

url = "https://maps.googleapis.com/maps/api/place/textsearch/json"

params = {
    "query": "property management in Knoxville TN",
    "key": API_KEY
}

r = requests.get(url, params=params)
print("STATUS:", r.status_code)
print("RESPONSE JSON:")
print(r.json())
