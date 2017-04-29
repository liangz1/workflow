from pymongo import MongoClient, InsertOne
import logging
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from config import TTL_BATCH_SIZE, DB_NAME

client = MongoClient()
db = client[DB_NAME]
coll_path = db.cname_path
new_coll = db.cname_real_ip

cnt = 0
requests = []
finished = set()
for doc in new_coll.find({}, {}):
    finished.add(doc["_id"])
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
logging.info("Finished %d" % len(finished))
for doc in coll_path.find():
    if doc["_id"] in finished:
        continue
    # logging.debug(type(doc["A"]))
    if type(doc["A"]) is unicode:
        continue
    cnt += 1
    t = dict()
    # build t
    raw_doc = db.rawall.find_one({"_id": doc["_id"]})
    for zonedoc in raw_doc["zones"]:
        zonename = zonedoc["name"]
        t[zonename] = dict()
        for resolverdoc in zonedoc["resolvers"]:
            t[zonename][resolverdoc["name"]] = resolverdoc["ip"]

    for k, record in enumerate(doc["A"]):
        # logging.debug(doc["A"])  find the IP's in rawall, creating aux dict.
        for i, ns in enumerate(record[1]):
            zone = ns[0]
            nameserver = ns[1]

            if zone in t:
                zone_t = t[zone]
                if nameserver in zone_t:
                    ip = zone_t[nameserver]
                else:
                    logging.error("no ip recorded for %s!" % nameserver)
                    ip = "NO IP"
            else:
                logging.error("no ip recorded for %s!" % zone)
                ip = "NO IP"

            # ip = db.ns_ip.find_one({"_id": ns[1]})["ip"]

            doc["A"][k][1][i].append(ip)

    for k, record in enumerate(doc["CNAMES"]):
        for i, ns in enumerate(record[1]):
            zone = ns[0]
            nameserver = ns[1]

            if zone in t:
                zone_t = t[zone]
                if nameserver in zone_t:
                    ip = zone_t[nameserver]
                else:
                    logging.error("no ip recorded for %s!" % nameserver)
                    ip = "NO IP"
            else:
                logging.error("no ip recorded for %s!" % zone)
                ip = "NO IP"
            # ip = db.ns_ip.find_one({"_id": ns[1]})["ip"]

            doc["CNAMES"][k][1][i].append(ip)

    requests.append(InsertOne(doc))
    if cnt % TTL_BATCH_SIZE == 0:
        new_coll.bulk_write(requests)
        requests = []
        logging.info(cnt)
new_coll.bulk_write(requests)




"""
# this is used to generate ns_ip mapping
coll_ns.aggregate(
    [
        {"$unwind": "$ns_set"},
        {"$group": {
            "_id": "$ns_set",
            "ip": {"$addToSet": "$_id"}
        }},
        {"$project": {
            "ip": 1,
            "sz": {"$size": "$ip" }
        }},
        {"$out": "ns_ip"}
    ], allowDiskUse=True
)
"""
