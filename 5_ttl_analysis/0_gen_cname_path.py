import socket
import itertools
from pymongo import MongoClient, InsertOne
import logging
from multiprocessing.dummy import Pool
import struct
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__),'../'))
from config import TTL_BATCH_SIZE, DB_NAME, COLL_NAME, DOT

client = MongoClient()
db = client[DB_NAME]
coll = db[COLL_NAME]

new_coll = db.cname_path
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

with open("../top-1m.txt") as f:
    lines = f.read().splitlines()
look = ["placeholder"]
for line in lines:
    look.append(line.split(",")[1])


def gen_doc(c1=coll, c2=new_coll):
    done = set([doc["_id"] for doc in c2.find({}, {})])
    logging.info("finished %d" % len(done))
    for doc in c1.find({"_id": {"$gt": len(done)}}):
        doc["qname"] = look[doc["_id"]].replace(".", DOT)+DOT
        yield doc


def is_int_ip(addr):
    try:
        socket.inet_ntoa(struct.pack("!L", long(addr)))
        return True
    except socket.error:
        return False


def to_ip(addr):
    return socket.inet_ntoa(struct.pack("!L", long(addr)))


def process_doc(doc):
    new_doc = dict()
    new_doc["_id"] = doc["_id"]
    new_doc["qname"] = doc["qname"]

    """ find the CNAME chain and A record"""
    cnames = []
    A_record = []
    # first, cast the list to a dict
    new_names = dict()
    names = doc["names"]
    for name_d in names:
        new_names[name_d["name"].replace(".", "#")] = (name_d["addresses"], name_d["ttl"])

    # then search records in the dict new_names
    get_addr = False
    next_name = None
    find_a_cname = False
    see_error = None

    if doc["qname"] not in new_names:
        # logging.error("%s not in new_names! %s" % (doc["qname"], str(new_names)))
        see_error = "INCOMPLETE DATA"
        new_doc["A"] = see_error
        return InsertOne(new_doc)

    record = new_names[doc["qname"]]
    addr_dict = record[0]

    for addr in addr_dict.keys():
        if addr in {"SERVFAIL", "NXDOMAIN", "NODATA", "TIMEOUT", "VALUE_ERROR"}:
            # no a useful record
            see_error = addr
            continue
        if addr[-1] == DOT:
            # find a CNAME

            if find_a_cname:
                logging.error("qname: %s, find a cname %s when processing %s" % (doc["qname"], next_name, addr))
                see_error = "TOO MANY CNAMEs"
                new_doc["A"] = see_error
                return InsertOne(new_doc)
            find_a_cname = True
            next_name = addr
            cnames.append((addr, addr_dict[addr], record[1]))
            # if got addr, that's ok
        else:
            if not is_int_ip(addr):
                logging.error("addr format error: %s" % addr)
            assert is_int_ip(addr)

            # find an A record
            get_addr = True
            A_record.append([to_ip(addr), addr_dict[addr], record[1]])
            # if find a cname, that's ok

    if not get_addr and next_name is None:
        new_doc["A"] = see_error
        return InsertOne(new_doc)

    while not get_addr and len(cnames) < 10: # prevent infinite loop
        logging.debug(next_name)
        logging.debug(new_names)

        if next_name not in new_names:
            # logging.error("qname:%s, %s not in new_names! %s" % (doc["qname"], next_name, str(new_names)))
            see_error = "INCOMPLETE DATA"
            new_doc["A"] = see_error
            return InsertOne(new_doc)
        record = new_names[next_name]
        addr_dict = record[0]
        find_a_cname = False
        for addr in addr_dict.keys():
            if addr in {"SERVFAIL", "NXDOMAIN", "NODATA", "TIMEOUT", "VALUE_ERROR"}:
                # no a useful record
                see_error = addr
                continue
            if addr[-1] == DOT:
                # find a CNAME

                if find_a_cname:
                    logging.error("qname: %s, find a cname %s when processing %s" % (doc["qname"], next_name, addr))
                    see_error = "TOO MANY CNAMEs"
                    new_doc["A"] = see_error
                    return InsertOne(new_doc)
                next_name = addr
                cnames.append((addr, addr_dict[addr], record[1]))
                find_a_cname = True
            else:
                if not is_int_ip(addr):
                    logging.error("addr format error: %s" % addr)
                assert is_int_ip(addr)

                # find an A record
                get_addr = True
                A_record.append([to_ip(addr), addr_dict[addr], record[1]])
        if not (get_addr or find_a_cname):
            new_doc["A"] = see_error if see_error else "UNEXPECTED EMPTY RECORD"   # no such case
            return InsertOne(new_doc)

    if get_addr:
        new_doc["CNAMES"] = cnames
        new_doc["A"] = A_record
    else:
        new_doc["A"] = "TOO LONG CNAME CHAIN"
    return InsertOne(new_doc)

p = Pool()
docs = gen_doc()
cnt = 0
while True:
    ret = p.map(process_doc, itertools.islice(docs, TTL_BATCH_SIZE))
    cnt += 1
    if not ret:
        break
    else:
        new_coll.bulk_write(ret)
    # if cnt % PRINT_SIZE == 0:
    logging.info("finished %d" % (TTL_BATCH_SIZE*cnt))
