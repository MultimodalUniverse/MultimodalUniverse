from datasets import load_dataset
from tqdm import trange
from time import time

benchmark_iters = 2048

# test local speed
galaxies = iter(load_dataset("Smith42/Galaxies", split="train", cache_dir="/raid/data/cache"))

t0 = time()
for _ in trange(benchmark_iters):
    B = next(galaxies)
throughput_local = benchmark_iters/(time() - t0)

# test streaming speed
galaxies = iter(load_dataset("Smith42/Galaxies", split="train", cache_dir="/raid/data/cache", streaming=True))

t0 = time()
for _ in trange(benchmark_iters):
    B = next(galaxies)
throughput_remote = benchmark_iters/(time() - t0)

print("gal/s local", throughput_local)
print("gal/s remote", throughput_remote)
print("ratio", throughput_remote/throughput_local)
