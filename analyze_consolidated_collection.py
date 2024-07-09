import os
import csv
import json
from os.path import expanduser


def analyze_query_stats(query_stats: dict) -> dict:
    other_hits = analyze_other_statusmsgs(query_stats["other_messages"])
    summary = {
        "unix_timestamp": query_stats["timestamp"],
        "excel_timestamp": round(query_stats["timestamp"] / 86400 + 25569, 5),
        "extant_hits": len(query_stats["hits"]),
        "extant_hit_rate": query_stats["estimated_uploads_per_second"],
    }
    for other_hit in other_hits.keys():
        summary[other_hit] = other_hits[other_hit]
    summary["total_hits"] = sum(other_hits.values()) + summary["extant_hits"]
    summary["total_hit_rate"] = 0
    if summary["total_hits"] > 0:
        summary["total_hit_rate"] = int((2**22) / (query_stats["queries"] / summary["total_hits"]))
    return summary


def analyze_other_statusmsgs(other_messages: list[dict]) -> dict:
    breakdown = {
        "geoblocked_hits": 0,
        "moderated_hits": 0,
        "storypost_hits": 0,
        "private_hits": 0,
        "deleted_hits": 0,
        "other_hits": 0
    }
    for other_message in other_messages:
        if "cross_border_violation" in other_message["statusMsg"]:
            breakdown["geoblocked_hits"] += 1

        elif "content_classification" in other_message["statusMsg"]:
            breakdown["moderated_hits"] += 1
        elif "status_reviewing" in other_message["statusMsg"]:
            breakdown["moderated_hits"] += 1
        elif "status_audit_not_pass" in other_message["statusMsg"]:
            breakdown["moderated_hits"] += 1

        elif "item is storypost" in other_message["statusMsg"]:
            breakdown["storypost_hits"] += 1

        elif "status_self_see" in other_message["statusMsg"]:
            breakdown["private_hits"] += 1
        elif "status_friend_see" in other_message["statusMsg"]:
            breakdown["private_hits"] += 1
        elif "author_secret" in other_message["statusMsg"]:
            breakdown["private_hits"] += 1

        elif "status_deleted" in other_message["statusMsg"]:
            breakdown["deleted_hits"] += 1
        elif "vigo" in other_message["statusMsg"]:
            breakdown["deleted_hits"] += 1

        elif "author_status" in other_message["statusMsg"]:
            breakdown["moderated_hits"] += 1

        else:
            print(f"other message: {other_message['statusMsg']}")
            breakdown["other_hits"] += 1

    return breakdown


def process_metadata(metadata: dict, query_timestamp: int) -> dict:
    selected_metadata = {}
    selected_metadata["query_timestamp"] = query_timestamp
    return selected_metadata


def main():
    unified_collection_address = str(os.path.join(expanduser("~"), "tiktok-random"))
    queries_stats = []
    extant_hits, extant_hits_metadata = [], []
    for query_stats_json in os.listdir(os.path.join(unified_collection_address, "queries")):
        with open(os.path.join(unified_collection_address, "queries", query_stats_json), "r") as f:
            query_stats = json.load(f)
        queries_stats.append(analyze_query_stats(query_stats))
        extant_hits += query_stats["hits"]
    for extant_hit_id in extant_hits:
        extant_hits_metadata += [extant_hit_id]

    with open(os.path.join(unified_collection_address, "queries.csv"), "w") as f:
        w = csv.DictWriter(f, queries_stats[0].keys())
        w.writeheader()
        w.writerows(queries_stats)
    # with open(os.path.join(unified_collection_address, "metadata.csv"), "w") as f:
    #     w = csv.DictWriter(f)
    #     w.writerows(extant_hits_metadata)


if __name__ == "__main__":
    main()
