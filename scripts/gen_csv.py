#!/usr/bin/env python3
import argparse
import os
import random
import sys
import time

HEADER = ["id","name","age","active","city","score"]
CITIES = ["seoul","busan","incheon","daegu","daejeon"]


def row(i):
    name = f"user{i}"
    age = random.randint(18, 65)
    active = "true" if random.random() > 0.3 else "false"
    city = random.choice(CITIES)
    score = f"{random.uniform(50, 100):.2f}"
    return f"{i},{name},{age},{active},{city},{score}\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size-gb", type=float, default=1.0)
    ap.add_argument("--rows-per-chunk", type=int, default=100000)
    ap.add_argument("--output", default="testdata/bench.csv")
    args = ap.parse_args()

    target_bytes = int(args.size_gb * 1024 * 1024 * 1024)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    random.seed(1)
    written = 0
    start = time.time()

    with open(args.output, "w", encoding="utf-8") as f:
        header = ",".join(HEADER) + "\n"
        f.write(header)
        written += len(header)

        i = 1
        while written < target_bytes:
            chunk = []
            for _ in range(args.rows_per_chunk):
                line = row(i)
                chunk.append(line)
                written += len(line)
                i += 1
                if written >= target_bytes:
                    break
            f.writelines(chunk)

    elapsed = time.time() - start
    size_mb = written / (1024 * 1024)
    print(f"wrote {size_mb:.1f} MB to {args.output} in {elapsed:.2f}s")


if __name__ == "__main__":
    main()
