"""UUIDv7 generator (RFC 9562) — time-ordered UUIDs, no external dependency.

Layout (128 bits):
    48  unix timestamp in ms
     4  version (= 7)
    12  rand_a
     2  variant (= 0b10)
    62  rand_b

Because the high bits are a millisecond timestamp, these sort by creation time,
which keeps btree index inserts append-mostly on the hot tables.
"""

from __future__ import annotations

import secrets
import time
import uuid


def uuid7() -> uuid.UUID:
    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = (
        (ms << 80)
        | (0x7 << 76)
        | (rand_a << 64)
        | (0b10 << 62)
        | rand_b
    )
    return uuid.UUID(int=value)