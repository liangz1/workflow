import time
from pymongo import MongoClient
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from config import DB_NAME

client = MongoClient()
db = client[DB_NAME]


def group_ip(group_type):
    if group_type == "domain":
        set_name = "domain_set"
        add_to_set = "$rank"
        out_name = "ip_domain"
    elif group_type == "ns":
        set_name = "ns_set"
        add_to_set = "$ns"
        out_name = "ip_ns"
    else:
        set_name = "zone_set"
        add_to_set = "$zone"
        out_name = "ip_zone"

    coll = db["ip_allrawall"]

    cursor = coll.aggregate(
        [
            {"$group": {
                "_id": "$ip",
                set_name: { "$addToSet": add_to_set }
            }},
            {"$project": {
                set_name: 1,
                "sz": {"$size": "$"+set_name }}},
            {"$out": out_name}
        ], allowDiskUse=True
    )

stt = time.time()
print "Grouping domain..."
group_ip("domain")  # Used 23 seconds
t1 = time.time()
print "Used %f seconds" % (t1 - stt)
print "Grouping ns..."
group_ip("ns")  # Used 16 seconds
t2 = time.time()
print "Used %f seconds" % (t2 - t1)
print "Grouping zone..."
group_ip("zone")  # Used 19 seconds
t3 = time.time()
print "Used %f seconds" % (t3 - t2)
