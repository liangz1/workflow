import time
from pymongo import MongoClient
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from config import DB_NAME, COLL_NAME

client = MongoClient()
db = client[DB_NAME]


def pre_coll(coll_name):
    coll = db[coll_name]

    # generate ip to domain_ns_zone

    cursor = coll.aggregate(
        [
            {"$unwind": "$zones"},
            {"$project": {
                "qname": 1,
                "zones": {
                    "name": {"$toLower": "$zones.name"},
                    "resolvers": 1
                }
            }},
            {"$match": {"zones.name": { "$ne": "com."} } },
            {"$match": {"zones.name": { "$ne": "net."} } },
            {"$match": {"zones.name": { "$ne": "org."} } },
            {"$match": {"zones.name": { "$ne": "gtld-servers.net."} } },

            {"$unwind": "$zones.resolvers"},
            {"$project": {
                "zones": {
                    "resolvers": {
                        "up": 0,
                        "ttl": 0
                    }
                }
            }},
            {"$unwind": "$zones.resolvers.ip"},
            {"$match": {"zones.resolvers.ip": { "$ne": "SERVFAIL"} } },
            {"$match": {"zones.resolvers.ip": { "$ne": "NODATA"} } },
            {"$match": {"zones.resolvers.ip": { "$ne": "TIMEOUT"} } },
            {"$match": {"zones.resolvers.ip": { "$ne": "NXDOMAIN"} } },
            {"$match": {"zones.resolvers.ip": { "$ne": "VALUE_ERROR"} } },
            {"$project": {
                "_id": 0,
                "ip": "$zones.resolvers.ip",
                "domain": "$qname",
                "rank": "$_id",
                "ns": "$zones.resolvers.name",
                "zone": "$zones.name"
            }},
            {"$out": "ip_all"+coll_name}
        ], allowDiskUse=True
    )

start = time.time()
pre_coll(COLL_NAME) # Using 401.850880 seconds.
duration = time.time() - start
print "Using %f seconds." % duration
