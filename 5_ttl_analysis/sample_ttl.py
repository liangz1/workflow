import random


def gen_sample(src_file, sample_size):
    with open(src_file) as f:
        data = f.read().splitlines()

    spl = random.sample(data, sample_size)
    line = ",".join(spl)

    out_file = src_file[:-4]+"_sample"+str(sample_size)+".txt"
    with open(out_file, "w") as f:
        f.write(line)

gen_sample("ttl_data.txt", 50000)