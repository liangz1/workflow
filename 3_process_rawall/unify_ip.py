import socket
import struct
import time
from pymongo import MongoClient, UpdateMany

client = MongoClient()
db = client["aws0324"]
coll = db["ip_allrawall"]


def to_ip(string_of_int):
    return socket.inet_ntoa(struct.pack("!L", long(string_of_int)))


def is_ip(addr):
    try:
        socket.inet_aton(addr)
        return True
    except socket.error:
        return False

# requests = []
records = []
for doc in coll.find({"ip": {"$regex": "^[0-9]+$"}}):
    records.append([doc["_id"], to_ip(doc["ip"])])
    # requests.append(UpdateMany({"ip": doc["ip"]}, {"$set": {"ip": to_ip(doc["ip"])}}))
print "updating..."

length = len(records)
# length = len(requests)

start = time.time()

# batch = []
for i, item in enumerate(records):
# for i, req in enumerate(requests):
    coll.update_one({"_id": item[0]}, {"$set": {"ip": item[1]}})
    # batch.append(req)
    if (i+1) % 100 == 0:

        # coll.bulk_write(batch)
        # batch = []
        tmptime = (time.time()-start)/(i+1)
        print "%d / %d, avg time: %f, time left: %f" % ((i+1), length, tmptime, (length-(i+1))*tmptime)

if length % 100 != 0:
# if len(batch) > 0:
#     coll.bulk_write(batch)
    tmptime = (time.time() - start) / length
    print "%d / %d, avg time: %f, time left: %f" % (length, length, tmptime, (length - length) * tmptime)
duration = time.time() - start
print "Using %f seconds." % duration  # Using 2056.106464 seconds
