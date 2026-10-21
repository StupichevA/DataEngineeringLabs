import sys
from profiler import Watch
from steps import run_duckdb_salary

if __name__ == "__main__":
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    watch = Watch()
    run_duckdb_salary(k)
    watch.stop()
    print(f"Время выполнения: {watch.seconds:.2f} сек")
    print(f"Пик памяти: {watch.peak_mb:.2f} МБ")