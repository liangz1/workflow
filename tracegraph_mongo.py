import logging

import dns.resolver
import socket
import sys
from multiprocessing import Queue, JoinableQueue, Process
import time
import struct
from pymongo import MongoClient, InsertOne
from config import BATCH_SIZE, WORKER_NUM, TOTAL_NUM, DB_NAME, COLL_NAME


#BATCH_SIZE = 30
#WORKER_NUM = 1300
#TOTAL_NUM = 1000000
client = MongoClient()
db = client[DB_NAME]
coll = db[COLL_NAME]

def to_int(str_ip):
    try:
        ret = str(int(socket.ntohl(struct.unpack("I", socket.inet_aton(str_ip))[0])))
        return ret
    except Exception, e:
        raise e


# Try creating a dummy socket to see if ipv6 is available
have_ipv6 = False
# s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
# try:
#     s.connect(('ipv6.google.com', 0))
# except:
#     have_ipv6 = False

if have_ipv6:
    rdtypes_for_nameservers = [dns.rdatatype.A, dns.rdatatype.AAAA]
else:
    rdtypes_for_nameservers = [dns.rdatatype.A]

dns_errors = {
    dns.resolver.NXDOMAIN: 'NXDOMAIN',
    dns.resolver.NoNameservers: 'SERVFAIL',
    dns.resolver.Timeout: 'TIMEOUT',
    'NODATA': 'NODATA',
}

class Zone(object):
    def __init__(self, name, parent=None):
        self.name = name
        self.resolvers = {}
        self.ttl = -1
        self.root = parent or self
        self.trace_missing_glue = parent and parent.trace_missing_glue or False
        self.even_trace_m_gtld_servers_net = parent and parent.even_trace_m_gtld_servers_net or False

        if name == '.':
            self.subzones = {}
            self.names = {}

    def trace(self, name, depth, logger, rdtype=dns.rdatatype.A):
        if isinstance(rdtype,basestring):
            rdtype = dns.rdatatype.from_text(rdtype)
        if self.name == '.' and not self.resolvers:
            # if this is a root and doesn't have resolvers, find root resolvers using default resolver
            self.find_root_resolvers()
        if not name.endswith('.'):
            name += '.'
        # dfs on all resolvers here
        for resolver in sorted(self.resolvers.values(), key=lambda x: x.name):
            resolver.resolve(name, depth, logger, rdtype=rdtype)

    def resolve(self, name, rdtype=dns.rdatatype.A):
        if self.name == '.' and not self.resolvers:
            self.find_root_resolvers()
        if name in self.root.names:
            return self.root.names[name].ip
        if name in self.resolvers:
            # Misconfiguration a la otenet.gr, ns1.otenet.gr isn't glued anywhere. www.cosmote.gr A lookup triggered it
            pass
        for resolver in self.resolvers.values():
            if resolver.ip:
                return resolver.resolve(name, rdtype=rdtype, register=False)
        else:
            # No glue at all
            return self.resolvers.values()[0].resolve(name, rdtype=rdtype, register=False)

    def find_root_resolvers(self):
        for root in 'abcdefghijklm':
            root += '.root-servers.net.'
            self.resolvers[root] = Resolver(self, root)
            root_answer = dns.resolver.query(root,rdtype=dns.rdatatype.A)
            self.resolvers[root].ip = [x.address for x in root_answer.response.answer[0]]
            self.resolvers[root].ttl = 36000000
            self.resolvers[root].up = []


    def serialize(self):
        ret = {
            'name': self.name,
            'resolvers': [x.serialize() for x in self.resolvers.values()],
            'ttl': self.ttl,
            'zones': [],
            'names': [],
        }
        if self.name == '.':
            done = ['.']
            # Order them in such a way that we don't need to jump through hoops when deserializing
            def add_zone(zone, depth):
                if depth > 20:
                    return
                if zone.name in done:
                    return
                for resolver in zone.resolvers.values():
                    for up in resolver.up:
                        if up.zone.name not in done:
                            add_zone(up.zone, depth+1)
                ret['zones'].append(zone.serialize())
                done.append(zone.name)

            for zone in self.subzones.values():
                add_zone(zone, 0)
            for name in self.names.values():
                ret['names'].append(name.serialize())
        return ret

    @classmethod
    def deserialize(klass, data, root=None):
        inst = klass(data['name'], root)
        inst.ttl = data['ttl']
        for resolver in data['resolvers']:
            resolver = Resolver.deserialize(resolver, inst)
            inst.resolvers[resolver.name.lower()] = resolver
        if root:
            root.subzones[inst.name] = inst
        if not root:
            inst.subzones['.'] = inst
            for zone in data['zones']:
                Zone.deserialize(zone, inst)
            inst.subzones.pop('.')
            for name in data['names']:
                name = Name.deserialize(name, inst)
                inst.names[name.name] = name
        return inst

class Name(object):
    def __init__(self, name):
        self.name = name
        self.addresses = {}
        self.ttl = -1

    def serialize(self):
        return {
            'name': str(self.name),
            'addresses': dict([(addr, [[res.zone.name, res.name] for res in self.addresses[addr]]) for addr in self.addresses]),
            'ttl': self.ttl
        }

    @classmethod
    def deserialize(klass, data, root):
        inst = klass(data['name'])
        inst.ttl = data['ttl']
        for addr in data['addresses']:
            inst.addresses[addr] = []
            for zone,resolver in data['addresses'][addr]:
                if zone == '.':
                    inst.addresses[addr].append(root.resolvers[resolver.lower()])
                else:
                    inst.addresses[addr].append(root.subzones[zone].resolvers[resolver.lower()])
        return inst

class Resolver(object):
    def __init__(self, zone, name):
        self.zone = zone
        self.name = name
        self.root = self.zone.root
        self.ip = []
        self.ttl = -1
        self.up = []

    def resolve(self, name, depth, logger, rdtype=dns.rdatatype.A, register=True):
        if not self.ip:
            # log("Did not receive glue record for %s" % self.name)
            if name == self.name:
                return ["No glue"]
            if self.name in self.root.names and self.root.names[self.name].addresses:
                self.ip = self.root.names[self.name].addresses.keys()
            elif self.zone.trace_missing_glue and (self.name != 'm.gtld-servers.net.' or self.zone.even_trace_m_gtld_servers_net):

                self.root.trace(self.name, 0, logger, dns.rdatatype.A)
                if self.name in self.root.names and self.root.names[self.name].addresses:
                    self.ip = self.root.names[self.name].addresses.keys()

            else:
                self.ip = self.root.resolve(self.name, dns.rdatatype.A)
        if not self.ip or self.ip == ['NODATA']:
            if register:
                msg = 'NODATA'
                if name not in self.root.names:
                    self.root.names[name] = Name(name)
                name = self.root.names[name]
                if msg not in name.addresses:
                    name.addresses[msg] = []
                name.addresses[msg].append(self)
            return ["Resolver has no IP"]
        res = dns.resolver.Resolver(configure=False)
        res.timeout = 3.0
        for ip in self.ip[:1]:
            res.nameservers = self.ip[:1]
            if self.ip[0] == "SERVFAIL":
                continue
            # log("Trying to resolve %s (%s) on %s (%s) (R:%s)" % (name, dns.rdatatype.to_text(rdtype), self.name, self.ip[0], register))
            try:
                ans = res.query(name, rdtype=rdtype, raise_on_no_answer=False)
            except (dns.resolver.NXDOMAIN, dns.resolver.NoNameservers, dns.resolver.Timeout, ValueError):
                # Insert a bogus name node for NXDOMAIN/SERVFAIL
                if sys.exc_type not in dns_errors:
                    if sys.exc_type == ValueError:
                        msg = "VALUE_ERROR"
                    else:
                        msg = "ERROR_exc_tpye"+str(sys.exc_type)
                else:
                    msg = dns_errors[sys.exc_type]
                if not register:
                    return
                if name not in self.root.names:
                    self.root.names[name] = Name(name)
                name = self.root.names[name]
                if msg not in name.addresses:
                    name.addresses[msg] = []
                name.addresses[msg].append(self)
                return

            if not ans.response.answer:
                return self.process_auth(name, rdtype, ans, register, depth+1, logger)
            return self.process_answer(name, rdtype, ans, register, depth+1, logger)

    def process_auth(self, name, rdtype, ans, register, depth, logger):
        # OK, we're being sent a level lower
        # print "Process auth: %s" % "\n".join([record.name.to_text() for record in ans.response.authority])
        logger.info(str(depth))
        zone = None
        for record in ans.response.authority:
            zonename = record.name.to_text()
            if zonename in self.root.subzones and zonename != self.zone.name and self.zone.name.endswith(zonename):
                # They're trying to send us back up, nasty!
                # Let's cut that off right now
                if register:
                    msg = 'NXDOMAIN'
                    if name not in self.root.names:
                        self.root.names[name] = Name(name)
                    name = self.root.names[name]
                    if msg not in name.addresses:
                        name.addresses[msg] = []
                    name.addresses[msg].append(self)
                return
            if zonename == self.zone.name:
                # Weird... no answer for our own zone?
                if register:
                    msg = 'NXDOMAIN'
                    if name not in self.root.names:
                        self.root.names[name] = Name(name)
                    name = self.root.names[name]
                    if msg not in name.addresses:
                        name.addresses[msg] = []
                    name.addresses[msg].append(self)
                return
            if record.rdtype == dns.rdatatype.NS:
                if not register:
                    zone = Zone(zonename, self.root)
                else:
                    if zonename not in self.root.subzones:
                        self.root.subzones[zonename] = Zone(zonename, self.root)
                    zone = self.root.subzones[zonename]

                for item in record.items:
                    ns = item.target.to_text().lower()
                    if ns not in zone.resolvers:
                        zone.resolvers[ns] = Resolver(zone, ns)
                    if self not in zone.resolvers[ns].up:
                        zone.resolvers[ns].up.append(self)
                    zone.resolvers[ns].ttl = record.ttl
                zone.ttl = record.ttl

        if not zone:
            # Seen with eg akamai's a0a.akamaiedge.net: resolvers return
            # NOERROR but only an SOA record when requesting A records (a0a
            # only has an ipv6 address)
            if register:
                msg = 'NODATA'
                if name not in self.root.names:
                    self.root.names[name] = Name(name)
                name = self.root.names[name]
                if msg not in name.addresses:
                    name.addresses[msg] = []
                name.addresses[msg].append(self)
            return

        # Process glue records
        for record in ans.response.additional:
            if record.rdtype in rdtypes_for_nameservers:
                zone.resolvers[record.name.to_text().lower()].ip = [x.address for x in record.items]
                zone.resolvers[record.name.to_text().lower()].ttl = record.ttl

        # Simple resolution?
        if not register:
            return zone.resolve(name, rdtype)

        # We're doing a depth-first search, so by now the name may actually be resolved already
        if name not in self.root.names:
            return zone.trace(name, depth, logger, rdtype)

    def process_answer(self, name, rdtype, ans, register, depth, logger):
        # print "Process answer: %s" % "\n".join([record.name.to_text().lower() for record in ans.response.answer])
        logger.info(str(depth))

        # Real answer
        names = {}
        resolve = []
        orig_name = name.lower()

        for record in ans.response.answer:
            name = record.name.to_text().lower()
            if name not in names:
                if name in self.root.names:
                    names[name] = self.root.names[name]
                else:
                    names[name] = Name(name)
            name = names[name]
            name.ttl = record.ttl

            if record.rdtype == dns.rdatatype.A:
                for x in record.items:
                    try:
                        addr = to_int(x.address)
                    except Exception, e:
                        logger.error(x.address+"\t"+str(e))
                    if addr not in name.addresses:
                        name.addresses[addr] = []
                    name.addresses[addr].append(self)

            elif record.rdtype == dns.rdatatype.MX:
                for x in record.items:
                    addr = x.exchange.to_text().lower()
                    resolve.append((addr, 'A'))
                    if addr not in name.addresses:
                        name.addresses[addr] = []
                    name.addresses[addr].append(self)

            elif record.rdtype == dns.rdatatype.CNAME:
                for x in record.items:
                    cname = x.target.to_text().lower()
                    resolve.append((cname, rdtype))
                    cname_key = cname.replace(".", "-")
                    if cname_key not in name.addresses:
                        name.addresses[cname_key] = []
                    name.addresses[cname_key].append(self)

            # elif record.rdtype == dns.rdatatype.SRV:
            #     for x in record.items:
            #         cname = x.target.to_text().lower()
            #         resolve.append((cname, 'A'))
            #         if cname not in name.addresses:
            #             name.addresses[cname] = []
            #         name.addresses[cname].append(self)
            #
            # elif record.rdtype in (dns.rdatatype.TXT, dns.rdatatype.SOA, dns.rdatatype.PTR):
            #     for x in record.items:
            #         addr = x.to_text()
            #         if addr not in name.addresses:
            #             name.addresses[addr] = []
            #         name.addresses[addr].append(self)

            else:
                raise RuntimeError("Unknown record:" + str(record))

        if not register:
            return names[orig_name].addresses.keys()

        self.root.names.update(names)
        for name, newrdtype in resolve:
            if name not in self.root.names:
                self.root.trace(name, 0, logger, newrdtype)

    def serialize(self):
        return {
            'name': self.name,
            'ip': self.ip,
            'ttl': self.ttl,
            'up': [[res.zone.name, res.name] for res in self.up],
        }

    @classmethod
    def deserialize(klass, data, zone):
        inst = klass(zone, data['name'])
        inst.ip = data['ip']
        inst.ttl = data['ttl']
        for zone, resolver in data['up']:
            inst.up.append(inst.root.subzones[zone].resolvers[resolver.lower()])
        return inst


def gen_root():
    rt = Zone('.')
    rt.ttl = 36000000
    return rt


def run_dnsgraph(id_name):
    logger = logging.getLogger(id_name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(threadName)s %(name)s %(funcName)s %(message)s')
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    fh = logging.FileHandler('log_trace.log')
    fh.setLevel(logging.ERROR)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    _id, name = id_name.split(",")
    root = gen_root()  # generate a root zone and trace from it to the target address
    root.trace_missing_glue = True
    root.even_trace_m_gtld_servers_net = True
    try:
        root.trace(name, 0, logger, rdtype='A')
    except RuntimeError, e:
        logger.error(str(e))
    data = root.serialize()
    data["_id"] = int(_id)
    return data


def work(id, jobs, result):
    while True:
        task = jobs.get()
        if task is None:
            break
        request = run_dnsgraph(task)
        result.put(request)


with open("top-1m.txt") as f:
    top1m_lines = f.read().splitlines()

finished_domains = [False for i in xrange(TOTAL_NUM)]
finished_cnt = 0
for doc in coll.find({}, {}):
    finished_domains[doc["_id"] - 1] = True
    finished_cnt += 1

print "Finished domains: %d" % finished_cnt
print "Count: %d" % coll.count()
jobs = Queue()
for i, line in enumerate(top1m_lines):
    if not finished_domains[i]:
        jobs.put(line)  # Initiate jobs

result = JoinableQueue()
[Process(target=work, args=(i, jobs, result)).start() for i in xrange(WORKER_NUM)]
print 'starting workers'

requests = []
batch_doc = []
start = time.time()
count = 0
err_cnt = 0
logger = logging.getLogger("main")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(name)s %(funcName)s %(message)s')

fh = logging.FileHandler('log_trace.log')
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)
for i in xrange(1, TOTAL_NUM + 1 - finished_cnt):
    count = i
    doc = result.get()
    batch_doc.append(doc)
    req = InsertOne(doc)
    requests.append(req)
    result.task_done()
    if i % BATCH_SIZE == 0:
        try:
            coll.bulk_write(requests)
        except Exception, e:
            logger.error(str(e)+"\nDoc: "+str(batch_doc[0]))
        requests = []
        logger.info("processed %d/%d" % (i + finished_cnt, TOTAL_NUM))
        duration = time.time() - start
        logger.info("Using time: %f, avg time: %f" % (duration, duration / i))
        # sys.stdout.flush()
if len(requests) > 0:
    coll.bulk_write(requests)
    logger.info("processed %d/%d" % (count + finished_cnt, TOTAL_NUM))
    duration = time.time() - start
    logger.info("Using time: %f, avg time: %f" % (duration, duration / count))
    # sys.stdout.flush()

for w in xrange(WORKER_NUM):
    jobs.put(None)

result.join()
