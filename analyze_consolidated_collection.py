import os
import csv
import json
from os.path import expanduser


unified_collection_address = str(os.path.join(expanduser("~"), "tiktok-random"))
metadata_fields = {
    "id": "video_id",
    "desc": "description",
    "createTime": "create_timestamp",
    "scheduleTime": "schedule_timestamp",
    "video": {
        "height": "video_height",
        "width": "video_width",
        "duration": "video_duration",
        "bitrate": "video_bitrate",
        "videoQuality": "video_quality",
        "subtitleInfos": "video_subtitle_info",
        "volumeInfo": "video_volume_info",
        "VQScore": "video_quality_score",
    },
    "author": {
        "id": "author_id",
        "shortId": "author_short_id",
        "uniqueId": "author_unique_id",
        "secUid": "author_sec_uid",
        "nickname": "author_nickname",
        "signature": "author_bio",
        "createTime": "author_create_timestamp",
        "verified": "author_verified",
        "ftc": "author_ftc",
        "openFavorite": "author_open_favorite",
        "commentSetting": "author_comment_setting",
        "duetSetting": "author_duet_setting",
        "stitchSetting": "author_stitch_setting",
        "privateAccount": "author_private_account",
        "secret": "author_secret",
        "isADVirtual": "author_is_ad_virtual",
        "ttSeller": "author_tt_seller",
        "downloadSetting": "author_download_setting",
        "isEmbedBanned": "author_is_embed_banned",
        "canExpPlaylist": "author_can_exp_playlist",
        "suggestAccountBind": "author_suggest_account_bind",
        "recommendReason": "author_recommend_reason",
        "roomId": "author_room_id",
        "relation": "author_relation",
        "nowInvitationCardUrl": "author_now_invitation_card_url",
    },
    "challenges": "challenges",
    "stats": {
        "diggCount": "stats_like_count",
        "shareCount": "stats_share_count",
        "commentCount": "stats_comment_count",
        "playCount": "stats_view_count",
        "collectCount": "stats_save_count"
    },
    "statsV2": {
        "diggCount": "statsv2_like_count",
        "shareCount": "statsv2_share_count",
        "commentCount": "statsv2_comment_count",
        "playCount": "statsv2_view_count",
        "collectCount": "statsv2_save_count",
        "repostCount": "statsv2_repost_count"
    },
    "warnInfo": "warn_info",
    "originalItem": "original_item",
    "officalItem": "official_item",
    "textExtra": "text_extra",
    "secret": "video_secret",
    "forFriend": "video_for_friend",
    "itemCommentStatus": "item_comment_status",
    "takeDown": "take_down",
    "effectStickers": "effect_stickers",
    "privateItem": "private_item",

    "stitchEnabled": "video_stitch_enabled",
    "stickersOnItem": "stickers_on_item",
    "shareEnabled": "video_share_enabled",
    "comments": "comments",
    "duetEnabled": "video_duet_enabled",
    "duetDisplay": "duet_display",
    "duetInfo": "duet_info",
    "stitchDisplay": "stitch_display",
    "indexEnabled": "index_enabled",
    "locationCreated": "location_created",
    "suggestedWords": "suggested_words",
    "contents": "contents",
    "channelTags": "channel_tags",
    "item_control": {
        "can_repost": "video_can_repost"
    },
    "IsAigc": "video_is_ai_gc",
    "AIGCDescription": "ai_gc_description",
    "aigcLabelType": "ai_gc_label_type",
    "isAd": "video_is_ad",
    "adLabelVersion": "ad_label_version",
    "adAuthorization": "ad_authorization",
    "brandOrganicType": "brand_organic_type",
    "contentLocation": "content_location",
    "poi": "point_of_interest",
    "music": "music",
    "diversificationId": "diversification_id",
    "diversificationLabels": "diversification_labels",
    "BAInfo": "BAInfo",
    "isECVideo": "video_is_ec",
    "itemMute": "item_mute",
    "anchors": "anchors",
    "maskType": "mask_type",
    "playlistId": "playlist_id",
    "imagePost": "image_post"
}


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


def process_metadata(metadata: dict, query_id: int, query_timestamp: int, fields: dict = None) -> dict:
    selected_metadata = {}
    selected_metadata["query_timestamp"] = query_timestamp
    selected_metadata["query_id"] = str(query_id)
    for field_0 in metadata_fields.keys():
        if isinstance(metadata_fields[field_0], str):
            if field_0 in metadata.keys():
                if isinstance(metadata[field_0], dict) or isinstance(metadata[field_0], list):
                    selected_metadata[metadata_fields[field_0]] = json.dumps(metadata[field_0])
                elif (isinstance(metadata[field_0], int) or isinstance(metadata[field_0], float)) and metadata[field_0] > 2**53:
                    selected_metadata[metadata_fields[field_0]] = str(metadata[field_0])
                else:
                    selected_metadata[metadata_fields[field_0]] = metadata[field_0]
            else:
                selected_metadata[metadata_fields[field_0]] = None
        elif isinstance(metadata_fields[field_0], dict):
            for field_1 in metadata_fields[field_0].keys():
                if field_1 in metadata[field_0].keys():
                    if isinstance(metadata[field_0][field_1], dict) or isinstance(metadata[field_0][field_1], list):
                        selected_metadata[metadata_fields[field_0][field_1]] = json.dumps(metadata[field_0][field_1])
                    elif isinstance(metadata[field_0][field_1], int) and metadata[field_0][field_1] > 2**53:
                        selected_metadata[metadata_fields[field_0][field_1]] = str(metadata[field_0][field_1])
                    else:
                        selected_metadata[metadata_fields[field_0][field_1]] = metadata[field_0][field_1]
                else:
                    selected_metadata[metadata_fields[field_0][field_1]] = None
    return selected_metadata


def get_unique_metadata_fields(metadata: dict) -> set[tuple]:
    metadata_fields = set()
    for field_0 in metadata.keys():
        if isinstance(metadata[field_0], dict):
            for field_1 in metadata[field_0].keys():
                if isinstance(metadata[field_0][field_1], dict):
                    for field_2 in metadata[field_0][field_1].keys():
                        if isinstance(metadata[field_0][field_1][field_2], dict):
                            for field_3 in metadata[field_0][field_1][field_2].keys():
                                metadata_fields.add((field_0, field_1, field_2, field_3))
                        else:
                            metadata_fields.add((field_0, field_1, field_2))
                else:
                    metadata_fields.add((field_0, field_1))
        else:
            metadata_fields.add((field_0,))
    return metadata_fields


def main():

    queries_stats = []
    extant_hits, extant_hits_metadata = [], []
    extant_unique_metadata_fields: set[tuple] = set()
    for query_stats_json in os.listdir(os.path.join(unified_collection_address, "queries")):
        with open(os.path.join(unified_collection_address, "queries", query_stats_json), "r") as f:
            query_stats = json.load(f)
        queries_stats.append(analyze_query_stats(query_stats))
        for extant_hit in query_stats["hits"]:
            with open(os.path.join(unified_collection_address, "metadata", f"{extant_hit}.json"), "r") as f:
                extant_hit_metadata = json.load(f)
            try:
                processed_metadata = process_metadata(extant_hit_metadata["itemInfo"]["itemStruct"],
                                                      int(extant_hit),
                                                      int(query_stats_json.split(".")[0]))
                processed_metadata["status_code"] = extant_hit_metadata["statusCode"]
                processed_metadata["status_message"] = extant_hit_metadata["statusMsg"]
                extant_hits_metadata.append(processed_metadata)

                unique_metadata_fields = get_unique_metadata_fields(extant_hit_metadata["itemInfo"]["itemStruct"])
                extant_unique_metadata_fields.update(unique_metadata_fields)
            except Exception as e:
                print(f"{extant_hit} {str(e)}")

    with open(os.path.join(unified_collection_address, "queries.csv"), "w") as f:
        w = csv.DictWriter(f, queries_stats[0].keys())
        w.writeheader()
        w.writerows(queries_stats)
    with open(os.path.join(unified_collection_address, "metadata.csv"), "w") as f:
        w = csv.DictWriter(f, extant_hits_metadata[0].keys())
        w.writeheader()
        w.writerows(extant_hits_metadata)
    for field in sorted(extant_unique_metadata_fields):
        if len(field) == 1:
            if field[0] not in metadata_fields.keys():
                print(field)
        if len(field) >= 2:
            if field[0] not in metadata_fields.keys() or (isinstance(metadata_fields[field[0]], dict) and field[1] not in metadata_fields[field[0]].keys()):
                print(field)

    # with open(os.path.join(unified_collection_address, "metadata.csv"), "w") as f:
    #     w = csv.DictWriter(f)
    #     w.writerows(extant_hits_metadata)


if __name__ == "__main__":
    main()
