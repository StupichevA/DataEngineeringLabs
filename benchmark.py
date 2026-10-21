import subprocess
import sys
import re
import pandas as pd
import time
import psutil
import threading
import os


class Watch:
    def __init__(self):
        self.me = psutil.Process(os.getpid())
        self.peak = 0
        self.running = True
        self.start_time = time.perf_counter()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self):
        while self.running:
            try:
                total = self.me.memory_info().rss
                for c in self.me.children(recursive=True):
                    try:
                        total += c.memory_info().rss
                    except psutil.NoSuchProcess:
                        pass
                if total > self.peak:
                    self.peak = total
            except psutil.NoSuchProcess:
                break
            time.sleep(0.05)

    def stop(self):
        self.running = False
        self.thread.join()
        self.seconds = time.perf_counter() - self.start_time
        self.peak_mb = self.peak / (1024 * 1024)
        return self


def run_script(script, k):
    result = subprocess.run(
        [sys.executable, script, str(k)],
        capture_output=True, text=True
    )
    time_val = mem_val = 0.0
    for line in result.stdout.splitlines():
        if "Время выполнения:" in line:
            time_val = float(re.search(r"([\d.]+)", line).group(1))
        if "Пик памяти:" in line:
            mem_val = float(re.search(r"([\d.]+)", line).group(1))
    return time_val, mem_val, result.returncode

from steps import run_mapreduce_salary, run_duckdb_salary

def collect_data(k):
    df_mr = run_mapreduce_salary(k, verbose=False)
    df_dd = run_duckdb_salary(k, verbose=False)
    return df_mr, df_dd


def measure(script, k, attempts=3):
    times, mems = [], []
    for _ in range(attempts):
        t, m, rc = run_script(script, k)
        if rc != 0:
            continue
        times.append(t)
        mems.append(m)
    if not times:
        return 0.0, 0.0
    return sum(times) / len(times), sum(mems) / len(mems)


def merge_for_print(df_mr, df_dd):
    if df_mr.empty and df_dd.empty:
        return pd.DataFrame()

    mr = df_mr.rename(columns={"cnt": "count_mr", "median_salary": "median_mr"})
    dd = df_dd.rename(columns={"cnt": "count_dd", "median_salary": "median_dd"})

    merged = pd.merge(mr, dd, on="stack", how="outer")
    merged = merged.fillna(0)
    return merged


def print_merged_table(merged_df, title, sort_by, ascending=False, top_n=10):
    if merged_df.empty:
        print(f"\n{title}: нет данных\n")
        return

    df_sorted = merged_df.sort_values(sort_by, ascending=ascending).head(top_n).copy()

    df_sorted["median_mr"] = df_sorted["median_mr"].round(0).astype(int)
    df_sorted["median_dd"] = df_sorted["median_dd"].round(0).astype(int)

    max_len = max(len(str(s)) for s in df_sorted["stack"])
    width = max(30, min(max_len, 55))

    print(f"\n{title}:")
    header = (f"{'stack':<{width}} | {'cnt_mr':>7} {'cnt_dd':>7} | "
              f"{'med_mr':>9} {'med_dd':>9}")
    print(header)
    print("-" * len(header))
    for _, row in df_sorted.iterrows():
        stack = str(row['stack'])[:width - 1]
        print(f"{stack:<{width}} | {int(row['count_mr']):>7} {int(row['count_dd']):>7} | "
              f"{int(row['median_mr']):>9} {int(row['median_dd']):>9}")
    print()


def print_all_for_k(df_mr, df_dd, k):
    merged = merge_for_print(df_mr, df_dd)

    total_vacancies = int(merged["count_mr"].sum()) if not merged.empty else 0
    total_stacks = len(merged)

    print(f"РЕЗУЛЬТАТЫ ДЛЯ k = {k}  (вакансий: {total_vacancies}, стеков: {total_stacks})")

    print_merged_table(merged, f"ТОП-10 по ЧАСТОТЕ (k={k})", sort_by="count_mr", ascending=False)
    print_merged_table(merged, f"ТОП-10 ПОВЫШАЮЩИХ ЗАРПЛАТУ (k={k})", sort_by="median_mr", ascending=False)
    print_merged_table(merged, f"ТОП-10 ПОНИЖАЮЩИХ ЗАРПЛАТУ (k={k})", sort_by="median_mr", ascending=True)


def build_summary(k, df_mr, df_dd, time_mr, mem_mr, time_dd, mem_dd):
    vac_mr = int(df_mr["cnt"].sum()) if not df_mr.empty else 0
    vac_dd = int(df_dd["cnt"].sum()) if not df_dd.empty else 0
    stacks_mr = len(df_mr)
    stacks_dd = len(df_dd)
    sum_med_mr = float(df_mr["median_salary"].sum()) if not df_mr.empty else 0
    sum_med_dd = float(df_dd["median_salary"].sum()) if not df_dd.empty else 0

    return {
        "k": k,
        "stacks_mr": stacks_mr,
        "stacks_dd": stacks_dd,
        "vac_mr": vac_mr,
        "vac_dd": vac_dd,
        "sum_median_mr": round(sum_med_mr, 0),
        "sum_median_dd": round(sum_med_dd, 0),
        "time_mr_s": round(time_mr, 2),
        "time_dd_s": round(time_dd, 2),
        "mem_mr_mb": round(mem_mr, 2),
        "mem_dd_mb": round(mem_dd, 2),
    }


if __name__ == "__main__":
    print("Сбор данных для k=1,2,3...\n")

    all_summaries = []
    for k in [1, 2, 3]:
        df_mr, df_dd = collect_data(k)
        print_all_for_k(df_mr, df_dd, k)

        print(f"Замер времени и памяти для k={k} (3 попытки)...")
        time_mr, mem_mr = measure("step_mapreduce.py", k)
        time_dd, mem_dd = measure("step_duckdb.py", k)
        print(f"MapReduce (среднее): {time_mr:.2f}с, {mem_mr:.2f}МБ")
        print(f"DuckDB (среднее): {time_dd:.2f}с, {mem_dd:.2f}МБ\n")

        all_summaries.append(build_summary(k, df_mr, df_dd, time_mr, mem_mr, time_dd, mem_dd))

    print("СВОДНАЯ ТАБЛИЦА (все k)")

    header = (f"{'k':<3} | {'stacks_mr':<10} {'stacks_dd':<10} | {'vac_mr':<10} {'vac_dd':<10} | "
              f"{'sum_med_mr':<12} {'sum_med_dd':<12} | {'t_mr':<6} {'t_dd':<6} | {'m_mr':<6} {'m_dd':<6}")
    print(header)
    print("-" * len(header))
    for s in all_summaries:
        print(f"{s['k']:<3} | {s['stacks_mr']:<10} {s['stacks_dd']:<10} | {s['vac_mr']:<10} {s['vac_dd']:<10} | "
              f"{s['sum_median_mr']:<12} {s['sum_median_dd']:<12} | {s['time_mr_s']:<6} {s['time_dd_s']:<6} | "
              f"{s['mem_mr_mb']:<6} {s['mem_dd_mb']:<6}")

    print("\nОбозначения: stacks — кол-во уникальных комбинаций; vac — суммарное число вхождений;")
    print("sum_med — сумма медиан; t — среднее время (с); m — средняя память (МБ).")

    pd.DataFrame(all_summaries).to_csv("benchmark_summary.csv", index=False)
    print("\nСохранено в benchmark_summary.csv")