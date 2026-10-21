from profiler import Watch
from steps import run_duckdb_regions

if __name__ == "__main__":
    watch = Watch()
    run_duckdb_regions(top_n=3)
    watch.stop()
    print(f"\nВремя выполнения: {watch.seconds:.2f} сек")
    print(f"Пик памяти: {watch.peak_mb:.2f} МБ")