import os
import sys
import json
import asyncio
import aiohttp
import random
import calendar
from datetime import datetime, timezone

JAN_1_2018 = 1514764800
TIME_NOW = int(datetime.utcnow().timestamp())


def random_bits(bits: int) -> str:
    return str("{:b}".format(random.getrandbits(bits))).zfill(bits)


def hex_to_binary(hex_string: str) -> str:
    return str("{:b}".format(int(hex_string, 16))).zfill(4)
    # return bin(int(hex_string, 16)).zfill(4)


def generate_resource_binary_str(resource: str = "D") -> str:
    return f"{random_bits(10)}000{random_bits(5)}00{hex_to_binary(resource)}00{random_bits(6)}"


def generate_binary_id(timestamp: int = None, resource: str = "D"):
    if resource not in ["6", "B", "0", "4", "D"]:
        raise ValueError(
            f"{resource} is not a valid resource type. Must be 6 (devices), B (live sessions), 0 or 4 (accounts), D (videos)")
    if timestamp is None:
        timestamp = random.randint(JAN_1_2018, TIME_NOW)
        # print(datetime.utcfromtimestamp(timestamp))
    return "{:b}".format(timestamp).zfill(32) + generate_resource_binary_str(resource)


def binary_to_decimal_id(binary_id):
    return int(binary_id, 2)

def extract_datetime_from_id(id_int: int):
    id_binary = "{:b}".format(id_int).zfill(64)
    timestamp_binary = id_binary[0:32]
    return datetime.utcfromtimestamp(int(timestamp_binary, 2))


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


def main():

    conn = aiohttp.TCPConnector(limit_per_host=100, limit=0, ttl_dns_cache=300)
    PARALLEL_REQUESTS = 100
    results = []

    urls = ['https://www.tiktok.com/api/comment/list/?aweme_id=7350639310271532293&count=1&cursor=0',
            'https://www.tiktok.com/api/comment/list/?aweme_id=7345721731560049966&count=1&cursor=0',
            'https://www.tiktok.com/api/comment/list/?aweme_id=7348110594538540321&count=1&cursor=0',
            'https://www.tiktok.com/api/comment/list/?aweme_id=7345733301618904322&count=1&cursor=0'
            'https://www.tiktok.com/api/comment/list/?aweme_id=7345200301618904322&count=1&cursor=0']


    async def gather_with_conncurrency(n):
        semaphore = asyncio.Semaphore(n)
        session = aiohttp.ClientSession(connector=conn)
        async def get(url):
            async with semaphore:
                async with session.get(url, ssl=False) as response:
                    obj = json.loads(await response.read())
                    results.append(obj)
        await asyncio.gather(*(get(url) for url in urls))
        await session.close()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(gather_with_conncurrency(PARALLEL_REQUESTS))
    conn.close()


    print(json.dumps(results))
# id = 7339313555189009710
# # id = 6690266970199409670
# id = 7320340725672919813
# id_binary = "{:b}".format(id)
# print(len(id_binary))
# id_timestamp = int(id_binary[:31], 2)
# print(id_timestamp)
#
# dt = datetime.fromtimestamp(id_timestamp)
# print(dt)
#
# for i in range(50):
#     binary_id = generate_id(resource="D")
#     print(binary_id)
#     print(int(binary_id, 2))
#     # print(extract_datetime_from_id(int(binary_id, 2)))
#

# generate_ids_from_date(100, 2022, 12, 1)
# print(type(random_date(2005, 12)))


# ids = generate_ids_from_month(10, 2020, 5)
# for id in ids:
#     print(extract_datetime_from_id(id))


