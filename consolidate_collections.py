import os
import json
import shutil
import argparse
from os.path import expanduser
from tiktoktools import ROOT_DIR
from tiktoktools.metadata import analyze_collection

parser = argparse.ArgumentParser()
parser.add_argument("collection", type=str, help="collection to analyze")
args = parser.parse_args()

collection = args.collection  # "random_tiktok_20240706_190713_720518"  # wells
collection_address = str(os.path.join(ROOT_DIR, "collections", collection))
unified_collection_address = str(os.path.join(expanduser("~"), "tiktok-random"))

sampled_seconds = analyze_collection(collection)
print("timestamp, hits, estimated_uploads_per_second")
for sampled_second in sampled_seconds:
    for hit in [hit for hit in sampled_second["hits"]
                if f"{hit}.json" not in os.listdir(os.path.join(unified_collection_address, "metadata"))]:
        shutil.copy(os.path.join(collection_address, "metadata", f"{hit}.json"),
                    os.path.join(unified_collection_address, "metadata", f"{hit}.json"))
    with open(os.path.join(unified_collection_address, "queries", f"{sampled_second['timestamp']}.json"), "w") as f:
        json.dump(sampled_second, f, indent=4)
    print(f"{sampled_second['timestamp']}, {len(sampled_second['hits'])}, {sampled_second['estimated_uploads_per_second']}")
