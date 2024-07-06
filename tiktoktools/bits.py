import random


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


def convert_hex_to_binary(hex_string: str) -> str:
    """
    Convert 1-char hex string to 4-bit binary string.
    :param hex_string:
    :return:
    """
    if len(hex_string) != 1:
        raise ValueError("Hex string must be 1-char hex string.")
    return str("{:b}".format(int(hex_string, 16))).zfill(4)


def generate_random_binary_bits(n_bits: int) -> str:
    """
    Generate a random binary string of length n_bits.
    :param n_bits:
    :return:
    """
    return str("{:b}".format(random.getrandbits(n_bits))).zfill(n_bits)
