import subprocess

import time

flush_cache = "sudo unbound-control reload".split()


def collect(line):
    rank, domain = line.split(",")
    dig = ["dig", "+short", domain, "@localhost"]

    subprocess.call(flush_cache)
    subprocess.call(dig)


def collect_sampled_domains(file_name, sample_size):
    with open(file_name) as f:
        lines = f.read().splitlines()
    start = time.time()
    [collect(line) for line in lines]
    print "avg time: %f" % ((time.time() - start)/sample_size)


collect_sampled_domains("top-1m_sample1000.txt", 1000)
