import os
import sys
import time
import zlib
import subprocess

import pandas as pd
from s3_utils import (get_file_size, read_s3_split, put_text, get_text, list_keys, delete_prefix)
from mr_lib import prepare_lines

HERE = os.path.dirname(os.path.abspath(__file__))
SPLIT_SIZE = int(os.environ.get("SPLIT_SIZE", 64 * 1024))
REDUCERS = int(os.environ.get("REDUCERS", 2))
TOTAL_TASKS = int(os.environ.get("TOTAL_TASKS", 1))
HOLD_SECONDS = int(os.environ.get("HOLD_SECONDS", 0))
INDEX = int(os.environ.get("JOB_COMPLETION_INDEX", 0))
SHUFFLE, OUTPUT = "shuffle/", "output/"


def key_of(line):
    return line.split("\t", 1)[0]


def cmd_clean():
    delete_prefix(SHUFFLE)
    delete_prefix(OUTPUT)
    print("shuffle/ и output/ очищены")


def cmd_map(k):
    size = get_file_size()
    starts = range(0, size, SPLIT_SIZE)
    mine = [(i, s) for i, s in enumerate(starts) if i % TOTAL_TASKS == INDEX]
    print(f"[map {INDEX}] частей всего {len(starts)}, моих {len(mine)}", flush=True)

    buckets = [[] for _ in range(REDUCERS)]
    emitted = 0
    for i, s in mine:
        raw = read_s3_split(s, min(s + SPLIT_SIZE, size))
        if not raw:
            continue
        lines = prepare_lines(raw)
        if not lines:
            continue
        res = subprocess.run([sys.executable, os.path.join(HERE, "mapper.py"), str(k)],
                             input="\n".join(lines), text=True, capture_output=True)
        if res.returncode != 0:
            print(f"[map {INDEX}] ошибка mapper: {res.stderr}", flush=True)
            sys.exit(1)
        for line in res.stdout.splitlines():
            if "\t" not in line:
                continue
            buckets[zlib.crc32(key_of(line).encode()) % REDUCERS].append(line)
            emitted += 1

    for r, b in enumerate(buckets):
        if b:
            b.sort(key=key_of)
            put_text(f"{SHUFFLE}map{INDEX:04d}-r{r:02d}.txt", "\n".join(b) + "\n")
    print(f"[map {INDEX}] записей: {emitted}", flush=True)
    if HOLD_SECONDS:
        print(f"[map {INDEX}] удерживаю под {HOLD_SECONDS} с (для наблюдения)", flush=True)
        time.sleep(HOLD_SECONDS)


def cmd_reduce():
    r = INDEX
    keys = [k for k in list_keys(SHUFFLE) if k.endswith(f"-r{r:02d}.txt")]
    lines = []
    for key in keys:
        lines.extend(l for l in get_text(key).splitlines() if l)
    lines.sort(key=key_of)
    print(f"[reduce {r}] файлов {len(keys)}, строк {len(lines)}", flush=True)
    res = subprocess.run([sys.executable, os.path.join(HERE, "reducer.py")],
                         input="\n".join(lines) + "\n", text=True, capture_output=True)
    if res.returncode != 0:
        print(res.stderr, flush=True)
        sys.exit(1)
    put_text(f"{OUTPUT}part-{r:05d}.txt", res.stdout)


def cmd_collect(k):
    rows = []
    for key in list_keys(OUTPUT + "part-"):
        for line in get_text(key).splitlines():
            p = line.split("\t")
            if len(p) == 3:
                rows.append((p[0], int(p[1]), float(p[2])))
    rows.sort(key=lambda x: x[2], reverse=True)
    df = pd.DataFrame(rows, columns=["stack", "cnt", "median_salary"])
    print(f"--- MapReduce: зарплаты (k={k}), групп: {len(df)} ---")
    print("ТОП-5 повышающих:\n" + df.head(5).to_string(index=False))
    print("ТОП-5 понижающих:\n" + df.tail(5).to_string(index=False))
    put_text(f"{OUTPUT}result-k{k}.csv", df.to_csv(index=False))


if __name__ == "__main__":
    stage = sys.argv[1]
    k = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    {"clean": cmd_clean, "map": lambda: cmd_map(k),
     "reduce": cmd_reduce, "collect": lambda: cmd_collect(k)}[stage]()