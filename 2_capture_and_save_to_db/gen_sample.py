import random


def gen_sample(src_file, sample_size):
    with open(src_file) as f:
        data = f.read().splitlines()

    spl = random.sample(data, sample_size)
    splt_spl = [line.split(",") for line in spl]
    splt_spl = [[int(t[0]), t[1]] for t in splt_spl]
    splt_spl.sort(key=lambda tup: tup[0])
    spl = [str(a)+","+b+"\n" for a, b in splt_spl]

    out_file = src_file[:-4]+"_sample"+str(sample_size)+".txt"
    with open(out_file, "w") as f:
        f.writelines(spl)

gen_sample("4th_range.txt", 1000)