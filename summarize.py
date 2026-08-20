"""
tiktok_summarize.py — Summarize TikTok random-sample collection data into a dashboard-ready JSON.

=== USAGE ===
  python tiktok_summarize.py /path/to/collection_folder [--output /path/to/output.json]

  Where collection_folder contains:
    - queries/       (directory of per-second query JSON files)
    - metadata.csv   (flattened per-video metadata)
"""

import os
import json
import math
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timezone

COUNTRY_NAMES = {
    "AF": "Afghanistan", "AL": "Albania", "DZ": "Algeria", "AO": "Angola",
    "AR": "Argentina", "AM": "Armenia", "AU": "Australia", "AT": "Austria",
    "AZ": "Azerbaijan", "BD": "Bangladesh", "BY": "Belarus", "BE": "Belgium",
    "BJ": "Benin", "BO": "Bolivia", "BA": "Bosnia and Herzegovina",
    "BR": "Brazil", "BG": "Bulgaria", "BF": "Burkina Faso", "KH": "Cambodia",
    "CM": "Cameroon", "CA": "Canada", "TD": "Chad", "CL": "Chile",
    "CN": "China", "CO": "Colombia", "CD": "DR Congo", "CR": "Costa Rica",
    "CI": "Côte d'Ivoire", "HR": "Croatia", "CU": "Cuba", "CZ": "Czech Republic",
    "DK": "Denmark", "DO": "Dominican Republic", "EC": "Ecuador", "EG": "Egypt",
    "SV": "El Salvador", "ET": "Ethiopia", "FI": "Finland", "FR": "France",
    "GA": "Gabon", "GE": "Georgia", "DE": "Germany", "GH": "Ghana",
    "GR": "Greece", "GT": "Guatemala", "GN": "Guinea", "HT": "Haiti",
    "HN": "Honduras", "HK": "Hong Kong", "HU": "Hungary", "IN": "India",
    "ID": "Indonesia", "IQ": "Iraq", "IE": "Ireland", "IL": "Israel",
    "IT": "Italy", "JM": "Jamaica", "JP": "Japan", "JO": "Jordan",
    "KZ": "Kazakhstan", "KE": "Kenya", "KR": "South Korea", "KW": "Kuwait",
    "KG": "Kyrgyzstan", "LA": "Laos", "LB": "Lebanon", "LY": "Libya",
    "LT": "Lithuania", "MG": "Madagascar", "MW": "Malawi", "MY": "Malaysia",
    "ML": "Mali", "MX": "Mexico", "MN": "Mongolia", "MA": "Morocco",
    "MZ": "Mozambique", "MM": "Myanmar", "NA": "Namibia", "NP": "Nepal",
    "NL": "Netherlands", "NZ": "New Zealand", "NI": "Nicaragua", "NE": "Niger",
    "NG": "Nigeria", "NO": "Norway", "OM": "Oman", "PK": "Pakistan",
    "PS": "Palestine", "PA": "Panama", "PY": "Paraguay", "PE": "Peru",
    "PH": "Philippines", "PL": "Poland", "PT": "Portugal", "QA": "Qatar",
    "RO": "Romania", "RU": "Russia", "RW": "Rwanda", "SA": "Saudi Arabia",
    "SN": "Senegal", "RS": "Serbia", "SG": "Singapore", "SK": "Slovakia",
    "SI": "Slovenia", "SO": "Somalia", "ZA": "South Africa", "ES": "Spain",
    "LK": "Sri Lanka", "SD": "Sudan", "SE": "Sweden", "CH": "Switzerland",
    "SY": "Syria", "TW": "Taiwan", "TJ": "Tajikistan", "TZ": "Tanzania",
    "TH": "Thailand", "TG": "Togo", "TN": "Tunisia", "TR": "Turkey",
    "TM": "Turkmenistan", "UG": "Uganda", "UA": "Ukraine", "AE": "United Arab Emirates",
    "GB": "United Kingdom", "US": "United States", "UY": "Uruguay",
    "UZ": "Uzbekistan", "VE": "Venezuela", "VN": "Vietnam", "YE": "Yemen",
    "ZM": "Zambia", "ZW": "Zimbabwe",
    "FAKE-AD": "Fake/Ad Location",
}

SEARCH_SPACE_PER_SECOND = 2**22  # 4,194,304


class TikTokSummarizer:

    def __init__(self, collection_path: str, collection_date: str = None):
        self.collection_path = collection_path
        self.collection_date = collection_date

        self._load_query_jsons()

        self.metadata_df = pd.read_csv(os.path.join(collection_path, "metadata.csv"))
        print(f"Loaded metadata.csv: {len(self.metadata_df)} extant video hits")

        if self.collection_date is None:
            max_ts = max(self.timestamps)
            self.collection_date = datetime.fromtimestamp(max_ts, tz=timezone.utc).strftime("%B %d %Y")

        self._compute_size_estimates()
        self._preprocess_metadata()

        self.stats_data = {}
        self.stats_quantiles = {}
        self.stats_fields = []

    def _load_query_jsons(self):
        queries_dir = os.path.join(self.collection_path, "queries")
        if not os.path.isdir(queries_dir):
            raise FileNotFoundError(
                f"queries/ directory not found in {self.collection_path}. "
                "This script requires the individual query JSON files."
            )

        self.timestamps = []
        self.per_second_raw_queries = []
        self.per_second_effective_queries = []
        self.per_second_extant_hits = []
        self.per_second_total_hits = []
        self.increment_limit = None

        for fname in sorted(os.listdir(queries_dir)):
            if not fname.endswith(".json"):
                continue
            with open(os.path.join(queries_dir, fname), "r") as f:
                data = json.load(f)

            self.timestamps.append(data["timestamp"])
            self.per_second_raw_queries.append(data["queries"])
            self.per_second_effective_queries.append(data["effective_queries"])

            extant = len(data["hits"])
            other = len(data["other_messages"])
            self.per_second_extant_hits.append(extant)
            self.per_second_total_hits.append(extant + other)

            if self.increment_limit is None:
                self.increment_limit = data.get("increment_limit", 64)

        self.n_queries = len(self.timestamps)
        self.total_raw_queries = sum(self.per_second_raw_queries)
        self.total_effective_queries = sum(self.per_second_effective_queries)
        self.total_extant_hits = sum(self.per_second_extant_hits)
        self.total_all_hits = sum(self.per_second_total_hits)
        self.total_seconds_in_range = max(self.timestamps) - min(self.timestamps)

        print(f"Loaded {self.n_queries} query JSONs from queries/")
        print(f"  Raw queries: {self.total_raw_queries:,}")
        print(f"  Effective queries: {self.total_effective_queries:,}")
        print(f"  Increment limit: {self.increment_limit}")
        print(f"  Extant hits: {self.total_extant_hits:,}")
        print(f"  Total hits: {self.total_all_hits:,}")
        print(f"  Timestamp range: {self.total_seconds_in_range:,} seconds")

    def _compute_size_estimates(self):
        # Global hit rate (single rate across entire sample)
        self.extant_hit_rate = self.total_extant_hits / self.total_effective_queries
        self.total_hit_rate = self.total_all_hits / self.total_effective_queries

        # Size = hit_rate × 2^22 × total_seconds
        self.estimated_extant_size = int(
            self.extant_hit_rate * SEARCH_SPACE_PER_SECOND * self.total_seconds_in_range
        )
        self.estimated_total_size = int(
            self.total_hit_rate * SEARCH_SPACE_PER_SECOND * self.total_seconds_in_range
        )

        # Confidence intervals
        mean_eff_per_second = self.total_effective_queries / self.n_queries
        scale_factor = (SEARCH_SPACE_PER_SECOND / mean_eff_per_second) * self.total_seconds_in_range

        arr_extant = np.array(self.per_second_extant_hits, dtype=float)
        sd_extant = arr_extant.std(ddof=1)
        se_extant = sd_extant / np.sqrt(self.n_queries)
        self.extant_margin = se_extant * scale_factor * 1.96

        arr_total = np.array(self.per_second_total_hits, dtype=float)
        sd_total = arr_total.std(ddof=1)
        se_total = sd_total / np.sqrt(self.n_queries)
        self.total_margin = se_total * scale_factor * 1.96

        print(f"\nSize estimates:")
        print(f"  Extant: {self.estimated_extant_size:,} (± {int(self.extant_margin):,})")
        print(f"  Total:  {self.estimated_total_size:,} (± {int(self.total_margin):,})")

    def _preprocess_metadata(self):
        df = self.metadata_df

        df["upload_year"] = df["create_timestamp"].apply(
            lambda t: datetime.fromtimestamp(t, tz=timezone.utc).year if pd.notna(t) else None
        )

        df["location_code"] = df["location_created"].apply(
            lambda x: str(x).strip('"').strip() if pd.notna(x) else None
        )
        df["location_name"] = df["location_code"].apply(
            lambda x: COUNTRY_NAMES.get(x, x) if pd.notna(x) else "Unknown"
        )

        def extract_category(val):
            if pd.isna(val):
                return "Uncategorized"
            try:
                parsed = json.loads(val)
                if isinstance(parsed, list) and len(parsed) > 0:
                    return parsed[-1]
            except (json.JSONDecodeError, TypeError):
                pass
            return "Uncategorized"

        df["category"] = df["diversification_labels"].apply(extract_category)
        df["has_music"] = df["music"].notna()

        numeric_cols = [
            "statsv2_view_count", "statsv2_like_count", "statsv2_comment_count",
            "statsv2_save_count", "statsv2_share_count", "video_duration"
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        self.metadata_df = df

    def calculate(self):
        self._add_numerical_stat("views", "statsv2_view_count", "views")
        self._add_numerical_stat("likes", "statsv2_like_count", "likes")
        self._add_numerical_stat("duration", "video_duration", "seconds")
        self._add_numerical_stat("comments", "statsv2_comment_count", "comments")
        self._add_numerical_stat("saves", "statsv2_save_count", "saves")
        self._add_numerical_stat("shares", "statsv2_share_count", "shares")

        self._add_location_stat()
        self._add_category_stat("category", "category", "category")
        self._add_category_stat("music", "has_music", "has music")
        self._add_category_stat("video_is_ai_gc", "video_is_ai_gc", "AI generated content")
        self._add_category_stat("video_is_ad", "video_is_ad", "advertisement")

        self._add_upload_year_stats()

    def _add_numerical_stat(self, stat_name, column, unit):
        series = self.metadata_df[column]
        max_val = series.max()

        if max_val <= 0:
            self.stats_fields.append(stat_name)
            self.stats_data[stat_name] = {
                "unit": unit, "labels": ["0"], "values": [1.0],
                "median": float(series.median()), "mean": float(series.mean()),
            }
            self.stats_quantiles[stat_name] = self._quantiles(column)
            return

        log_base = 2
        stop = int(math.log(max(max_val, 1), log_base)) + 1
        bins = [-0.01, 0] + list(np.logspace(0, stop, stop + 1, base=log_base, dtype="int"))
        distribution = series.value_counts(normalize=True, sort=False, bins=bins)

        labels = []
        for i in range(1, len(bins)):
            if bins[i] - bins[i - 1] <= 1:
                labels.append(f"{bins[i]:,}")
            else:
                labels.append(f"{(bins[i - 1] + 1):,}-{bins[i]:,}")

        self.stats_fields.append(stat_name)
        self.stats_data[stat_name] = {
            "unit": unit, "labels": labels, "values": distribution.tolist(),
            "median": float(series.median()), "mean": float(series.mean()),
        }
        self.stats_quantiles[stat_name] = self._quantiles(column)

    def _add_location_stat(self, top_n=20):
        valid = self.metadata_df[
            self.metadata_df["location_name"].notna() &
            (self.metadata_df["location_name"] != "Unknown") &
            (self.metadata_df["location_name"] != "None")
        ]
        if len(valid) == 0:
            return
        counts = valid["location_name"].value_counts()
        proportions = counts / counts.sum()
        top = proportions.head(top_n)
        other_proportion = proportions.iloc[top_n:].sum()

        self.stats_fields.append("location_created")
        self.stats_data["location_created"] = {
            "unit": "country",
            "labels": top.index.tolist() + ["other"],
            "values": top.tolist() + [other_proportion],
        }

    def _add_category_stat(self, stat_name, column, unit):
        counts = self.metadata_df[column].value_counts(dropna=False).sort_values(ascending=False)
        proportions = counts / counts.sum()
        self.stats_fields.append(stat_name)
        self.stats_data[stat_name] = {
            "unit": unit,
            "labels": [str(l) for l in counts.index.tolist()],
            "values": proportions.tolist(),
        }

    def _add_upload_year_stats(self):
        df = self.metadata_df
        years = df.groupby("upload_year", as_index=False).agg(count=("upload_year", "count"))
        years["proportion"] = years["count"] / years["count"].sum()
        years["estimated_uploads"] = (years["proportion"] * self.estimated_total_size).astype(int)
        years["cumulative_uploads"] = years["estimated_uploads"].cumsum().astype(int)
        year_labels = years["upload_year"].astype(int).tolist()

        self.stats_fields.append("upload_year")
        self.stats_data["upload_year"] = {
            "unit": "year", "labels": year_labels,
            "values": years["proportion"].tolist(),
            "median": int(df["upload_year"].median()),
        }
        self.stats_quantiles["upload_year"] = self._quantiles("upload_year")

        self.stats_fields.append("annual_uploads")
        self.stats_data["annual_uploads"] = {
            "unit": "year", "labels": year_labels,
            "values": years["estimated_uploads"].tolist(),
        }
        self.stats_fields.append("cumulative_uploads")
        self.stats_data["cumulative_uploads"] = {
            "unit": "year", "labels": year_labels,
            "values": years["cumulative_uploads"].tolist(),
        }

    def _quantiles(self, column):
        q = [i / 100 for i in range(1, 100)]
        return self.metadata_df[column].quantile(q=q, interpolation="higher").tolist()

    def export_json(self, output_path=None):
        result = {
            "sample": {
                "collection_date": self.collection_date,
                "size": self.estimated_total_size,
                "size_margin_of_error_95": int(self.total_margin),
                "extant_size": self.estimated_extant_size,
                "extant_size_margin_of_error_95": int(self.extant_margin),
                "total_raw_queries": self.total_raw_queries,
                "total_effective_queries": self.total_effective_queries,
                "increment_limit": self.increment_limit,
                "verified_hits": self.total_extant_hits,
                "total_hits": self.total_all_hits,
                "sampled_seconds": self.n_queries,
                "total_seconds_in_range": self.total_seconds_in_range,
            },
            "stats": {
                "fields": self.stats_fields,
                "data": self.stats_data,
                "quantiles": self.stats_quantiles,
            },
        }
        if output_path:
            with open(output_path, "w") as f:
                json.dump(result, f, indent=4, default=str)
            print(f"\nSaved summary to {output_path}")
        return result


def main():
    parser = argparse.ArgumentParser(
        description="Summarize TikTok random-sample collection into dashboard-ready JSON."
    )
    parser.add_argument("collection_path", type=str)
    parser.add_argument("-o", "--output", type=str, default=None)
    parser.add_argument("-d", "--date", type=str, default=None)
    args = parser.parse_args()

    output_path = args.output or os.path.join(args.collection_path, "summary.json")
    summarizer = TikTokSummarizer(args.collection_path, collection_date=args.date)
    summarizer.calculate()
    result = summarizer.export_json(output_path)

    s = result["sample"]
    print(f"\n{'='*70}")
    print(f"Summary: {s['collection_date']}")
    print(f"  Total size:   {s['size']:>20,} (± {s['size_margin_of_error_95']:,})")
    print(f"  Extant size:  {s['extant_size']:>20,} (± {s['extant_size_margin_of_error_95']:,})")
    print(f"  Raw queries:  {s['total_raw_queries']:>20,}")
    print(f"  Eff. queries: {s['total_effective_queries']:>20,}")
    print(f"  Extant hits:  {s['verified_hits']:>20,}")
    print(f"  Total hits:   {s['total_hits']:>20,}")
    print(f"  Sampled secs: {s['sampled_seconds']:>20,}")
    print(f"  Fields: {', '.join(result['stats']['fields'])}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
