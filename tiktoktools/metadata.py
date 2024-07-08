import json


def extract_metadata(page_source) -> tuple[dict, str, str]:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(page_source, "html.parser")
    rehydration_script_elements = soup.select('script#__UNIVERSAL_DATA_FOR_REHYDRATION__')
    if len(rehydration_script_elements) == 1:
        rehydration_dict = json.loads(rehydration_script_elements[0].text)
        # extract status code / status message
        if "__DEFAULT_SCOPE__" in rehydration_dict and "webapp.video-detail" in rehydration_dict["__DEFAULT_SCOPE__"]:
            return (rehydration_dict["__DEFAULT_SCOPE__"]["webapp.video-detail"],
                    str(rehydration_dict["__DEFAULT_SCOPE__"]["webapp.video-detail"]["statusCode"]),
                    rehydration_dict["__DEFAULT_SCOPE__"]["webapp.video-detail"]["statusMsg"])
        else:
            return {}, "ERROR", "__DEFAULT_SCOPE__ or webapp.video-detail not in data for rehydration"
    else:
        return {}, "ERROR", "script#__UNIVERSAL_DATA_FOR_REHYDRATION__"


def analyze_collection(collection: str) -> list:
    import os
    from tiktoktools import ROOT_DIR
    from datetime import datetime
    collection_data = []
    for hit_json in [hit_json for hit_json in os.listdir(
            os.path.join(ROOT_DIR, "collections", collection, "queries")) if hit_json.endswith("_hits.json")]:
        with open(os.path.join(ROOT_DIR, "collections", collection, "queries", hit_json)) as f:
            responses = json.load(f)
        timestamp = int(hit_json.split("_")[0])
        datetime.utcfromtimestamp(timestamp)
        hits = [response["id"] for response in responses if response["statusCode"] == "0"]
        other_status_msgs = [{response["id"]: {"statusCode": response["statusCode"],
                                               "statusMsg": response["statusMsg"]}} for response in responses if
                             response["statusCode"] != "0" and response["statusMsg"] != "item doesn't exist"]
        summary = {
            "timestamp": timestamp,
            "utc_datetime": datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S"),
            "queries": len(responses),
            "hits": hits,
            "estimated_uploads_per_second": int((2**22)/(len(responses)/len(hits))),
            "other_messages": other_status_msgs
        }
        collection_data.append(summary)
    return collection_data
