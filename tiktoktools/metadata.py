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
    increment_limit = 64
    if "_i_" in collection:
        increment_limit = int(collection.split("_i_")[1].split("_")[0])
    for hit_json in [hit_json for hit_json in os.listdir(
            os.path.join(ROOT_DIR, "collections", collection, "queries")) if hit_json.endswith("_hits.json")]:
        with open(os.path.join(ROOT_DIR, "collections", collection, "queries", hit_json)) as f:
            responses = json.load(f)
        timestamp = int(hit_json.split("_")[0])
        datetime.utcfromtimestamp(timestamp)
        hits = [response["id"] for response in responses if response["statusCode"] == "0"]
        other_status_msgs = [{
            "id": response["id"],
            "statusCode": response["statusCode"],
            "statusMsg": response["statusMsg"]
        } for response in responses if response["statusCode"] not in ["0", "ERROR", "10101"] and
                                       response["statusMsg"] not in ["item doesn't exist", "", None]]
        error_msgs = [{
            "id": response["id"],
            "statusCode": response["statusCode"],
            "statusMsg": response["statusMsg"]
        } for response in responses if response["statusCode"] in ["ERROR", "10101"]]

        count_hits = len(hits)
        count_responses = int(len(responses) * 64 / increment_limit)
        count_private_deleted = len(other_status_msgs)

        estimated_uploads_per_second = 0
        if count_hits > 0:
            estimated_uploads_per_second = int((2 ** 22) / (count_responses / count_hits))
        estimated_uploads_per_second_deleted = 0
        if count_private_deleted + count_hits > 0:
            estimated_uploads_per_second_deleted = int(
                (2 ** 22) / (count_responses / (count_private_deleted + count_hits)))
        summary = {
            "timestamp": timestamp,
            "utc_datetime": datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S"),
            "queries": len(responses),
            "effective_queries": count_responses,
            "increment_limit": increment_limit,
            "hits": hits,
            "other_messages": other_status_msgs,
            "error_messages": error_msgs,
            "estimated_uploads_per_second": estimated_uploads_per_second,
            "estimated_uploads_all": estimated_uploads_per_second_deleted
        }
        collection_data.append(summary)
    return collection_data
