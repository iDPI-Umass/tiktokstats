# import os
# import sys
# import json
# import asyncio
# import aiohttp
import random
import calendar
import itertools
from datetime import datetime, timezone

import pandas as pd

JAN_1_2018 = 1514764800
TIME_NOW = int(datetime.utcnow().timestamp())


def random_bits(bits: int) -> str:
    return str("{:b}".format(random.getrandbits(bits))).zfill(bits)


def hex_to_binary(hex_string: str) -> str:
    return str("{:b}".format(int(hex_string, 16))).zfill(4)
    # return bin(int(hex_string, 16)).zfill(4)


def generate_resource_binary_str(resource_type: str = "D", incrementer_shortcut=False) -> str:
    if incrementer_shortcut:
        return f"{random_bits(10)}0000{random_bits(4)}00{hex_to_binary(resource_type)}00{random_bits(6)}"
    return f"{random_bits(10)}00{random_bits(6)}00{hex_to_binary(resource_type)}00{random_bits(6)}"


def get_random_timestamp(start_timestamp: int = None, end_timestamp: int = None) -> int:
    if start_timestamp is None:
        start_timestamp = JAN_1_2018
    if end_timestamp is None:
        end_timestamp = TIME_NOW
    return random.randint(start_timestamp, end_timestamp)

def generate_binary_id(timestamp: int = None, resource: str = "D"):
    if resource not in ["6", "B", "0", "4", "D"]:
        raise ValueError(
            f"{resource} is not a valid resource type. Must be 6 (devices), B (live sessions), 0 or 4 (accounts), D (videos)")
    if timestamp is None:
        timestamp = get_random_timestamp()
    return "{:b}".format(timestamp).zfill(32) + generate_resource_binary_str(resource)


def binary_to_decimal_id(binary_id):
    return int(binary_id, 2)

def extract_datetime_from_id(id_int: int):
    id_binary = "{:b}".format(id_int).zfill(64)
    timestamp_binary = id_binary[0:32]
    return datetime.utcfromtimestamp(int(timestamp_binary, 2))


def extract_unique_binary_from_id(id_int: int):
    id_binary = "{:b}".format(id_int).zfill(64)
    return id_binary[32:]


def generate_ids_from_date(n_ids:int, year:int, month:int, day:int) -> list[int]:
    ids = []
    for i in range(n_ids):
        dt = datetime(year, month, day, random.randint(0, 23), random.randint(0, 59), random.randint(0, 59), tzinfo=timezone.utc)
        ids.append(binary_to_decimal_id(generate_binary_id(int(datetime.timestamp(dt)))))
    return ids


def random_date(year:int, month:int) -> int:
    dates = calendar.Calendar().itermonthdates(year, month)
    return random.choice([date for date in dates if date.month == month]).day


def generate_ids_from_month(n_days:int, year:int, month:int, ids_per_day = 100000):
    ids = []
    for i in range(n_days):
        day = random_date(year, month)
        ids += generate_ids_from_date(ids_per_day, year, month, day)
    return ids


def generate_bitswap_ids(id_int: int):
    swap = {"0": "1", "1": "0"}
    id_binary = "{:b}".format(id_int).zfill(64)
    timestamp_binary = id_binary[0:32]
    unique_binary = id_binary[32:64]
    bitswapped_ids = [unique_binary] + [unique_binary[0:i]+swap[unique_binary[i]]+unique_binary[i+1:] for i in range(32)]
    bitswapped_ids = [binary_to_decimal_id(timestamp_binary+bitswapped_id) for bitswapped_id in bitswapped_ids]
    print(bitswapped_ids)
    for bitswapped_id in bitswapped_ids:
        print(bitswapped_id)
    # print(timestamp_binary)
    # print(unique_binary)


def get_first_10_chars():
    return pd.read_csv("first10.csv", dtype=str, header=None)[0].to_list()


def get_last_6_chars():
    return pd.read_csv("last6.csv", dtype=str, header=None)[0].to_list()


def generate_all_same_second_ids(id_int: int):
    id_binary = "{:b}".format(id_int).zfill(64)
    timestamp_binary = id_binary[0:32]
    all_ids = []
    for segment_1 in get_first_10_chars():
        for segment_2 in ["".join(seq) for seq in itertools.product("01", repeat=5)]:
            for segment_3 in get_last_6_chars():
                all_ids.append(int(f"{timestamp_binary}{segment_1}000{segment_2}00110100{segment_3}", 2))
    return all_ids


def generate_ids_from_timestamp(timestamp: int = None, n: int = 50000, resource_type: str = "D", incrementer_shortcut=False) -> list[int]:
    if timestamp is None:
        timestamp = get_random_timestamp()
    timestamp_binary = "{:b}".format(timestamp).zfill(32)
    unique_ids = set()
    while len(unique_ids) < n:
        unique_ids.add(generate_resource_binary_str(resource_type, incrementer_shortcut))
    unique_ids = list(unique_ids)
    unique_ids = [int(f"{timestamp_binary}{unique_id}", 2) for unique_id in unique_ids]
    return unique_ids


def generate_same_second_ids_by_day(year, month, day):
    random_time_id = generate_ids_from_date(1, year, month, day)
    all_ids = generate_all_same_second_ids(random_time_id[0])
    print(all_ids[5000])
    print(all_ids[2345])
    return all_ids
