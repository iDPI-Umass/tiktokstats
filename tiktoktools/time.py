import calendar
import random
from datetime import timedelta, datetime
from tiktoktools import JAN_1_2018, TIME_NOW


def date_range(start_date, end_date):
    for n in range(int((end_date-start_date).days)):
        yield start_date + timedelta(n)


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


def random_date(year: int, month: int) -> int:
    """
    Generate a random date in a specified year and month.
    :param year:
    :param month:
    :return:
    """
    dates = calendar.Calendar().itermonthdates(year, month)
    return random.choice([date for date in dates if date.month == month]).day
