from pymongo import MongoClient, InsertOne
import logging
from collections import defaultdict
from multiprocessing.dummy import Pool
import struct

client = MongoClient()
db = client.aws0324
BATCH_SIZE = 300
coll_path = db.cname_real_ip
new_coll = db.ttl_real
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

cnt = 0
requests = []
finished = set()
for doc in new_coll.find({}, {}):
    finished.add(doc["_id"])
logging.info("Finished %d" % len(finished))

for doc in coll_path.find():
    if doc["_id"] in finished:
        continue
    ttl = defaultdict(set)
    new_doc = dict()
    for rec in doc["A"]:
        # ip = rec[1][0][2][0]
        for line in rec[1]:
            ttl[str(rec[2])] |= set(line[2])
    for rec in doc["CNAMES"]:
        for line in rec[1]:
            ttl[str(rec[2])] |= set(line[2])
    for ttl_time in ttl:
        # find the min time with this domain in it
        ip_set = ttl[ttl_time]
        min_set = 1000000
        min_ip = None
        for ip in ip_set:
            tmp_doc = db.ip_domain.find_one({"_id": ip})
            tmp_an_set = set(tmp_doc["domain_set"])
            if doc["_id"] in tmp_an_set:
                tmp_size = tmp_doc["sz"]
                if tmp_size < min_set:
                    min_set = tmp_size
                    min_ip = ip
        if min_set < 1000000:
            ttl[ttl_time] = [min_set, min_ip]
        else:
            ttl[ttl_time] = "NA"
    new_doc["_id"] = doc["_id"]
    new_doc["qname"] = doc["qname"]
    new_doc["ttl"] = ttl
    cnt += 1
    requests.append(InsertOne(new_doc))
    if cnt % BATCH_SIZE == 0:
        new_coll.bulk_write(requests)
        requests = []
        logging.info(cnt)
new_coll.bulk_write(requests)