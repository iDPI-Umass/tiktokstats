"""
functions for generating / manipulating TikTok IDs
"""

import random
import calendar
from datetime import datetime, timezone
from tiktoktools import JAN_1_2018, TIME_NOW


def generate_random_binary_bits(n_bits: int) -> str:
    """
    Generate a random binary string of length n_bits.
    :param n_bits:
    :return:
    """
    return str("{:b}".format(random.getrandbits(n_bits))).zfill(n_bits)


def convert_hex_to_binary(hex_string: str) -> str:
    """
    Convert 1-char hex string to 4-bit binary string.
    :param hex_string:
    :return:
    """
    if len(hex_string) != 1:
        raise ValueError("Hex string must be 1-char hex string.")
    return str("{:b}".format(int(hex_string, 16))).zfill(4)


def generate_random_resource_binary_str(resource_type: str = "D", limit_incrementer_randomness: bool = False) -> str:
    """
    Generate a random, 32-bit binary string of specified resource type.
    :param resource_type: 1 hex char (D=video)
    :param limit_incrementer_randomness: reduce randomness of incrementing segment from 6-bits to 4-bits (when true,
                                        still covers ~95% of used addresses)
    :return: a random, 32-bit binary string of specified resource type.
    """
    if limit_incrementer_randomness:
        return f"{generate_random_binary_bits(10)}0000{generate_random_binary_bits(4)}00{convert_hex_to_binary(resource_type)}00{generate_random_binary_bits(6)}"
    return f"{generate_random_binary_bits(10)}00{generate_random_binary_bits(6)}00{convert_hex_to_binary(resource_type)}00{generate_random_binary_bits(6)}"


def generate_random_timestamp(start_timestamp: int = None, end_timestamp: int = None) -> int:
    """
    Generate a random timestamp within a specified range.
    :param start_timestamp: unix integer timestamp (default is jan 1 2018 00:00:00)
    :param end_timestamp: unix integer timestamp (default is datetime.utcnow())
    :return: random integer timestamp
    """
    if start_timestamp is None or start_timestamp < JAN_1_2018 or start_timestamp > TIME_NOW:
        start_timestamp = JAN_1_2018
    if end_timestamp is None or end_timestamp > TIME_NOW or end_timestamp < JAN_1_2018:
        end_timestamp = TIME_NOW
    return random.randint(start_timestamp, end_timestamp)


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


def convert_binary_to_decimal_id(binary_id: str) -> int:
    """
    Convert binary ID to decimal
    :param binary_id:
    :return:
    """
    decimal_id = int(binary_id, 2)
    if decimal_id < 6505865277131980800:
        raise ValueError("timestamp is before Jan 1 2018 00:00:00")
    return decimal_id


def extract_datetime_from_id(id_int: int) -> datetime:
    """
    Extract datetime from 64-bit binary ID.
    :param id_int:
    :return:
    """
    id_binary = "{:b}".format(id_int).zfill(64)
    timestamp_binary = id_binary[0:32]
    timestamp_decimal = int(timestamp_binary, 2)
    if timestamp_decimal < JAN_1_2018:
        raise ValueError("timestamp is before Jan 1 2018 00:00:00")
    if timestamp_decimal > TIME_NOW:
        raise ValueError("timestamp is after current time")
    return datetime.utcfromtimestamp(timestamp_decimal)


def extract_resource_binary_from_id(id_int: int) -> str:
    """
    gets last 32 bits from 64-bit integer ID
    :param id_int: integer ID
    :return:
    """
    if id_int < 6505865277131980800:
        raise ValueError("timestamp is before Jan 1 2018 00:00:00")
    id_binary = "{:b}".format(id_int).zfill(64)
    return id_binary[32:]


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


def random_date(year: int, month: int) -> int:
    """
    Generate a random date in a specified year and month.
    :param year:
    :param month:
    :return:
    """
    dates = calendar.Calendar().itermonthdates(year, month)
    return random.choice([date for date in dates if date.month == month]).day


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


def generate_bitswap_ids(id_int: int) -> list[int]:
    """
    Generate an integer list of bitswapped resource binary IDs from a known ID, to test if bitswapped IDs are valid
    (they are not)
    :param id_int:
    :return:
    """
    swap = {"0": "1", "1": "0"}
    id_binary = "{:b}".format(id_int).zfill(64)
    timestamp_binary = id_binary[0:32]
    unique_binary = id_binary[32:64]
    bitswapped_ids = [unique_binary] + [unique_binary[0:i]+swap[unique_binary[i]]+unique_binary[i+1:] for i in range(32)]
    bitswapped_ids = [convert_binary_to_decimal_id(timestamp_binary + bitswapped_id) for bitswapped_id in bitswapped_ids]
    return bitswapped_ids


def generate_ids_from_timestamp(timestamp: int = None, n: int = 50000, resource_type: str = "D", incrementer_shortcut=False) -> list[int]:
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
        unique_ids.add(generate_random_resource_binary_str(resource_type, incrementer_shortcut))
    unique_ids = list(unique_ids)
    unique_ids = [int(f"{timestamp_binary}{unique_id}", 2) for unique_id in unique_ids]
    return unique_ids


# def get_first_10_chars():
#     return pd.read_csv("first10.csv", dtype=str, header=None)[0].to_list()
#
#
# def get_last_6_chars():
#     return pd.read_csv("last6.csv", dtype=str, header=None)[0].to_list()
#
#
# def generate_all_same_second_ids(id_int: int):
#     id_binary = "{:b}".format(id_int).zfill(64)
#     timestamp_binary = id_binary[0:32]
#     all_ids = []
#     for segment_1 in get_first_10_chars():
#         for segment_2 in ["".join(seq) for seq in itertools.product("01", repeat=5)]:
#             for segment_3 in get_last_6_chars():
#                 all_ids.append(int(f"{timestamp_binary}{segment_1}000{segment_2}00110100{segment_3}", 2))
#     return all_ids
#
#
# def generate_same_second_ids_by_day(year, month, day):
#     random_time_id = generate_ids_from_date(1, year, month, day)
#     all_ids = generate_all_same_second_ids(random_time_id[0])
#     print(all_ids[5000])
#     print(all_ids[2345])
#     return all_ids
