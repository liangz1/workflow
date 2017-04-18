from pymongo import MongoClient


def infer(db_name, coll_name):
    with open("special_ips.txt") as f:
        lines = f.read().splitlines()
    specials = dict()
    for line in lines:
        key, val = line.split()
        specials[val] = key

    with open("top-1m.txt") as f:
        lines = f.read().splitlines()
    qname = dict()
    for line in lines:
        key, val = line.split(",")
        qname[int(key)] = val

    db = MongoClient()[db_name]
    coll = db[coll_name]
    domain_lookup = db["ip_domain"]
    ns_lookup = db["ip_ns"]
    zone_lookup = db["ip_zone"]
    result = []
    for doc in coll.find():
        tmp = None
        tmpu = set()
        ns_list = []
        for ip in doc["ipset"]:
            if ip in specials:
                ns_list.append([specials[ip]])
                continue
            ns_ans = ns_lookup.find_one({"_id": ip})
            if ns_ans is None:
                print "ns set is none: "+ip
                ns_list.append([ip])
                continue
            ns_list.append(ns_ans["ns_set"])
            ans = domain_lookup.find_one({"_id": ip})
            if ans is None:
                print "domain set is none: " + qname[doc["rank"]]
                continue
            domain_set = set([qname[dict_item] for dict_item in ans["domain_set"]])

            if tmp is None:
                tmp = set() | domain_set
            else:
                tmp &= domain_set
            if len(domain_set) < 10000:
                tmpu |= domain_set
        if (tmp is None or (qname[doc["rank"]] not in tmp)) and len(tmpu) > 0:
            tmp = tmpu
        tmp_result = []
        tmp_result.append(str(doc["rank"]))
        tmp_result.append(qname[doc["rank"]])
        tmp_result.append(str(len(tmp)) if tmp is not None else "INF")
        tmp_result.append(str(ns_list))
        # tmp_result.append(", ".join(doc["ipset"]))
        result.append("\t".join(tmp_result)+"\n")
    with open(coll_name+"_result.txt", "w") as f:
        f.writelines(result)

infer("aws0324", "capture3k")
