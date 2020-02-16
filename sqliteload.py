import sys
import json
import logging
import sqlite3
import os

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

con = sqlite3.connect(sys.argv[1])
c = con.cursor()

c.execute("PRAGMA synchronous=OFF;")
c.execute("PRAGMA temp_store_directory='%s';" % os.path.dirname(sys.argv[1]))
c.execute("CREATE TABLE event_logs (address varchar(20), topic0 varchar(32), topic1 varchar(32), topic2 varchar(32), topic3 varchar(32), topic4 varchar(32), data blob, blockNumber BIGINT, transactionHash varchar(32), transactionIndex MEDIUMINT, blockHash varchar(32), logIndex MEDIUMINT, PRIMARY KEY (blockHash, logIndex));")
con.commit()
c = con.cursor()


def getByIndex(l, i, default=""):
    try:
        return l[i]
    except IndexError:
        return default

def decode(value):
    if not value:
        return value
    v = value.decode("hex").lstrip("\x00")
    if not v:
        return "\x00"
    return v

try:
    fd = open(sys.argv[2])
except IndexError:
    fd = sys.stdin
old_keys = set()
new_keys = set()
entries = []
for i, line in enumerate(fd):
    if i % 25000 == 0:
        c.executemany(
            "INSERT OR IGNORE INTO event_logs(address, topic0, topic1, topic2, topic3, topic4, data, blockNumber, transactionHash, transactionIndex, blockHash, logIndex) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            entries
        )
        entries = []
        con.commit()
        c = con.cursor()
        old_keys = new_keys
        new_keys = set()
    try:
        item = json.loads(line)
    except ValueError:
        pass
    prim_key = (item["blockHash"], item["logIndex"])
    if prim_key in new_keys or prim_key in old_keys:
        continue
    new_keys.add(prim_key)
    entries.append((
        sqlite3.Binary(decode(item["address"][2:])),
        sqlite3.Binary(decode(getByIndex(item["topics"], 0)[2:])),
        sqlite3.Binary(decode(getByIndex(item["topics"], 1)[2:])),
        sqlite3.Binary(decode(getByIndex(item["topics"], 2)[2:])),
        sqlite3.Binary(decode(getByIndex(item["topics"], 3)[2:])),
        sqlite3.Binary(decode(getByIndex(item["topics"], 4)[2:])),
        sqlite3.Binary(item["data"][2:].decode("hex")),
        int(item["blockNumber"], 16),
        sqlite3.Binary(decode(item["transactionHash"][2:])),
        int(item["transactionIndex"], 16),
        sqlite3.Binary(decode(item["blockHash"][2:])),
        int(item["logIndex"], 16),
    ))
fd.close()
if entries:
    c.executemany(
        "INSERT OR IGNORE INTO event_logs(address, topic0, topic1, topic2, topic3, topic4, data, blockNumber, transactionHash, transactionIndex, blockHash, logIndex) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
        entries
    )
con.commit()
c = con.cursor()
c.execute("CREATE INDEX address ON event_logs(address);")
c.execute("CREATE INDEX topic0 ON event_logs(topic0);")
c.execute("CREATE INDEX topic1 ON event_logs(topic1);")
c.execute("CREATE INDEX topic2 ON event_logs(topic2);")
c.execute("CREATE INDEX topic3 ON event_logs(topic3);")
c.execute("CREATE INDEX topic4 ON event_logs(topic4);")
c.execute("CREATE INDEX blockNumber ON event_logs(blockNumber);")
con.commit()
