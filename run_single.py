import sys
import itertools
import statistics
from collections import defaultdict

import pandas as pd
from profiler import Watch
from s3_utils import get_file_size, read_s3_split, put_text
from mr_lib import prepare_lines


def main():
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    watch = Watch()

    size = get_file_size()
    print(f"Файл: {size / 1024 / 1024:.1f} МБ, k={k}", flush=True)
    raw = read_s3_split(0, size)
    print("Файл прочитан", flush=True)
    lines = prepare_lines(raw)
    del raw
    print(f"Подходящих вакансий: {len(lines)}", flush=True)

    groups = defaultdict(list)
    for line in lines:
        skills_raw, salary_str = line.split("|", 1)
        salary = float(salary_str)
        skills = sorted({s.strip() for s in skills_raw.split(";") if s.strip()})
        if len(skills) < k:
            continue
        for combo in itertools.combinations(skills, k):
            groups["|".join(combo)].append(salary)
    print(f"Групп: {len(groups)}", flush=True)

    rows = [(key, len(v), statistics.median(v)) for key, v in groups.items() if len(v) >= 20]
    rows.sort(key=lambda r: r[2], reverse=True)
    df = pd.DataFrame(rows, columns=["stack", "cnt", "median_salary"])
    print("ТОП-5 повышающих:\n" + df.head(5).to_string(index=False))
    print("ТОП-5 понижающих:\n" + df.tail(5).to_string(index=False))
    put_text(f"output/single-k{k}.csv", df.to_csv(index=False))

    watch.stop()
    print(f"Время выполнения: {watch.seconds:.2f} сек")
    print(f"Пик памяти (RSS процесса): {watch.peak_mb:.2f} МБ")
    if watch.cgroup_peak_mb is not None:
        print(f"Пик памяти (cgroup контейнера): {watch.cgroup_peak_mb:.2f} МБ")


if __name__ == "__main__":
    main()