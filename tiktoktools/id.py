"""
functions for generating / manipulating TikTok IDs
"""

import random
from datetime import datetime, timezone
from tiktoktools.time import random_date, generate_random_timestamp
from tiktoktools.bits import convert_binary_to_decimal_id, convert_hex_to_binary, generate_random_binary_bits


def generate_random_resource_binary_str(resource_type: str = "D", limit_incrementer_randomness: int = 0) -> str:
    """
    Generate a random, 32-bit binary string of specified resource type.
    :param resource_type: 1 hex char (D=video)
    :param limit_incrementer_randomness: reduce randomness of 6-bit increment segment
    :return: a random, 32-bit binary string of specified resource type.
    """
    if 0 < limit_incrementer_randomness < 64:
        return f"{generate_random_binary_bits(10)}00{'{:b}'.format(random.randrange(0, limit_incrementer_randomness)).zfill(6)}00{convert_hex_to_binary(resource_type)}00{generate_random_binary_bits(6)}"
    return f"{generate_random_binary_bits(10)}00{generate_random_binary_bits(6)}00{convert_hex_to_binary(resource_type)}00{generate_random_binary_bits(6)}"


def generate_binary_id(timestamp: int = None, resource_type: str = "D") -> str:
    """
    Generate a random 64-bit binary string of specified resource type.
    :param timestamp: integer timestamp to generate random binary ID for
    :param resource_type: type of resource (video, profile, etc.) to generate random binary ID for
    :return:
    """
    if resource_type not in ["6", "B", "0", "4", "D"]:
        raise ValueError(
            f"{resource_type} is not a valid resource type. Must be 6 (devices), B (live sessions), 0 or 4 (accounts), D (videos)")
    if timestamp is None:
        timestamp = generate_random_timestamp()
    return "{:b}".format(timestamp).zfill(32) + generate_random_resource_binary_str(resource_type)


def generate_ids_from_timestamp(timestamp: int = None, n: int = 50000, resource_type: str = "D", limit_incrementer_randomness: int = 0) -> list[int]:
    """
    Generate random IDs from a specified timestamp.
    :param timestamp:
    :param n:
    :param resource_type:
    :param incrementer_shortcut:
    :return:
    """
    if timestamp is None:
        timestamp = generate_random_timestamp()
    timestamp_binary = "{:b}".format(timestamp).zfill(32)
    unique_ids = set()
    while len(unique_ids) < n:
        unique_ids.add(generate_random_resource_binary_str(resource_type, limit_incrementer_randomness))
    unique_ids = list(unique_ids)
    unique_ids = [int(f"{timestamp_binary}{unique_id}", 2) for unique_id in unique_ids]
    return unique_ids


def generate_ids_from_date(n_ids: int, year: int, month: int, day: int) -> list[int]:
    """
    Generate random IDs from random seconds on a specified date
    :param n_ids:
    :param year:
    :param month:
    :param day:
    :return: list of IDs
    """
    ids = []
    if year < 2018:
        raise ValueError("year must be 2018 or later")
    for i in range(n_ids):
        dt = datetime(year, month, day, random.randint(0, 23), random.randint(0, 59), random.randint(0, 59), tzinfo=timezone.utc)
        ids.append(convert_binary_to_decimal_id(generate_binary_id(int(datetime.timestamp(dt)))))
    return ids


def generate_ids_from_month(n_days: int, year: int, month: int, ids_per_day: int = 100000) -> list[int]:
    """
    Generate random IDs from one random second per day, for a specified number of days, in a specified month
    :param n_days:
    :param year:
    :param month:
    :param ids_per_day:
    :return: list of randomly generated IDs
    """
    ids = []
    for i in range(n_days):
        day = random_date(year, month)
        ids += generate_ids_from_date(ids_per_day, year, month, day)
    return ids
