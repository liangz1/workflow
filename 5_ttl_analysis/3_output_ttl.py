from pymongo import MongoClient
import logging
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from config import DB_NAME


client = MongoClient()
db = client[DB_NAME]
BATCH_SIZE = 3000
coll = db.ttl_real
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

cnt = 0
lines = []
for doc in coll.find():

    min_ttl = 2147483647
    min_size = 1000000
    for key, val in doc["ttl"].iteritems():
        if val[0] < min_size:
            min_size = val[0]
            min_ttl = int(key)
        elif val[0] == min_size:
            if int(key) < min_ttl:
                min_ttl = int(key)
        if min_ttl == 2147483647:
            print doc
        record = [str(doc["_id"]), doc["qname"], str(min_ttl), str(min_size), str(doc["ttl"])]
    # print doc
    lines.append("\t".join(record)+"\n")
    cnt += 1
    if cnt % BATCH_SIZE == 0:
        logging.info(cnt)

logging.info("writing file...")

with open("ttl_min_size_results.txt", "w") as f:
    f.writelines(lines)
