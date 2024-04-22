# from TikTokApi import TikTokApi
# import asyncio
# import os
import json
from collections import Counter

with open('test_ids.json', 'r') as f:
    data = json.load(f)

binary_ids = [video["asBinary"] for video in data]
for i in range(len(binary_ids)):
    if len(binary_ids[i]) == 63:
        binary_ids[i] = f"0{binary_ids[i]}"

print(len(binary_ids[0]))
a = [binary_id[60:64] for binary_id in binary_ids]
b = [binary_id[56:60] for binary_id in binary_ids]
c = [binary_id[52:56] for binary_id in binary_ids]
d = [binary_id[48:52] for binary_id in binary_ids]
e = [binary_id[44:48] for binary_id in binary_ids]
f = [binary_id[40:44] for binary_id in binary_ids]
g = [binary_id[36:40] for binary_id in binary_ids]
h = [binary_id[32:36] for binary_id in binary_ids]

print(Counter(a).most_common())
print(list(set(b)))
print(list(set(c)))
print(list(set(d)))
print(list(set(e)))
print(list(set(f)))
print(list(set(g)))
print(list(set(h)))

interval = 2
for i in range(int(32/interval)):
    substrings = [binary_id[32 + i*interval:32 + i*interval + interval] for binary_id in binary_ids]
    print(f"{32 + i*interval}-{32 + i*interval + interval - 1}: {list(set(substrings))}")
    print(json.dumps(Counter(substrings).most_common(), indent=4))

print("---")
interval = 4
for i in range(int(32/interval)):
    substrings = [binary_id[32 + i*interval:32 + i*interval + interval] for binary_id in binary_ids]
    print(f"{32 + i*interval}-{32 + i*interval + interval - 1}: {list(set(substrings))}")

print("---")
interval = 4
for i in range(int(64 / interval)):
    substrings = [binary_id[i * interval:i * interval + interval] for binary_id in binary_ids]
    print(
        f"{i * interval}-{i * interval + interval - 1}: {list(set(substrings))}")
