
from db.mongo_client import get_mongo

mongo = get_mongo()

coll = None

if hasattr(mongo, "raw_company_entity_expansion"):
    coll = mongo.raw_company_entity_expansion
else:
    try:
        coll = mongo.get_collection("raw_company_entity_expansion")
    except Exception:
        pass

if coll is None:
    try:
        coll = mongo["raw_company_entity_expansion"]
    except Exception as e:
        raise RuntimeError(f"Could not access raw_company_entity_expansion collection: {e}")

docs = list(coll.find({"parent_run_id": "3d098372-a582-4bb6-a1c3-2bad750274c"}).limit(5))

print(f"doc_count={len(docs)}")

for i, d in enumerate(docs, 1):
    d.pop("_id", None)
    print(f"\n--- DOC {i} ---")
    print(d)
