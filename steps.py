import os
import sys
import time
import subprocess
import multiprocessing
import glob
import shutil
import zlib
import io
import pandas as pd
import duckdb
from s3_utils import read_s3_split, get_file_size, BUCKET_NAME, OBJECT_NAME, ENDPOINT_URL, ACCESS_KEY, SECRET_KEY

MAPPER = [sys.executable, os.path.join(os.path.dirname(__file__), "mapper.py")]
REDUCER = [sys.executable, os.path.join(os.path.dirname(__file__), "reducer.py")]
WORK_DIR = os.path.join(os.path.dirname(__file__), "mr_work_salary")
SPLIT_SIZE = 64 * 1024
WORKERS = 4
REDUCERS = 2
MAP_DELAY = float(os.getenv("MAP_DELAY", "0"))  # только для демонстрации


def _atomic_write(path, text):
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def _done_marker(index):
    return os.path.join(WORK_DIR, f"map{index:04d}.done")


def _process_split(index, start, end, k):
    raw_data = read_s3_split(start, end)
    if not raw_data:
        return 0

    prepared_lines = []
    try:
        df = pd.read_csv(io.BytesIO(raw_data), header=None)
        for _, row in df.iterrows():
            if len(row) < 8:
                continue
            skills_str = str(row.iloc[-1])
            if pd.isna(skills_str) or skills_str == 'nan':
                continue
            skills_str = skills_str.replace('[', '').replace(']', '').replace("'", "").replace('"', '')
            skills_clean = skills_str.replace(',', ';')
            try:
                salary_from = float(row.iloc[3]) if pd.notna(row.iloc[3]) else 0
                currency = str(row.iloc[5]) if pd.notna(row.iloc[5]) else 'RUR'
                gross = str(row.iloc[6]).lower() if pd.notna(row.iloc[6]) else 'false'
                if currency != 'RUR' or salary_from == 0:
                    continue
                if gross == 'true':
                    salary_from = salary_from * 0.87
                prepared_lines.append(f"{skills_clean}|{salary_from}")
            except Exception:
                continue
    except Exception as e:
        print(f"[Map {index}] ОШИБКА ЧТЕНИЯ CSV: {e}")
        return 0

    if not prepared_lines:
        return 0

    result = subprocess.run(MAPPER + [str(k)], input="\n".join(prepared_lines), text=True, capture_output=True)
    if result.returncode != 0:
        print(f"[Map {index}] ОШИБКА: {result.stderr}")
        return 0

    lines_out = result.stdout.splitlines()
    buckets = [[] for _ in range(REDUCERS)]
    for line in lines_out:
        if '\t' not in line:
            continue
        key = line.split('\t')[0]
        r = zlib.crc32(key.encode()) % REDUCERS
        buckets[r].append(line)

    for r, bucket in enumerate(buckets):
        if not bucket:
            continue
        bucket.sort(key=lambda x: x.split('\t')[0])
        filename = os.path.join(WORK_DIR, f"map{index:04d}-r{r:02d}.txt")
        # "\n" в конце: без него последняя строка файла склеивается со следующей в reduce_task
        _atomic_write(filename, "\n".join(bucket) + "\n")
    return len(lines_out)


def map_task(task):
    index, start, end, k = task
    if os.path.exists(_done_marker(index)):
        return index, "skip"          # чекпоинт: сплит уже обработан
    n = _process_split(index, start, end, k)
    if MAP_DELAY:
        time.sleep(MAP_DELAY)
    _atomic_write(_done_marker(index), str(n))   # маркер пишется последним
    return index, "done"


def reduce_task(r):
    pattern = os.path.join(WORK_DIR, f"map*-r{r:02d}.txt")
    files = glob.glob(pattern)
    if not files:
        return

    final = os.path.join(WORK_DIR, f"part-{r:05d}.txt")
    tmp = f"{final}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as out:
        reducer = subprocess.Popen(REDUCER, stdin=subprocess.PIPE, stdout=out, text=True)
        all_lines = []
        for f_path in files:
            with open(f_path, 'r', encoding='utf-8') as f:
                all_lines.extend(f.readlines())
        all_lines.sort(key=lambda x: x.split('\t')[0])
        for line in all_lines:
            reducer.stdin.write(line)
        reducer.stdin.close()
        reducer.wait()
    os.replace(tmp, final)


def run_mapreduce_salary(k, verbose=False, resume=False, log=print):
    if not resume and os.path.exists(WORK_DIR):
        shutil.rmtree(WORK_DIR)
    os.makedirs(WORK_DIR, exist_ok=True)

    file_size = get_file_size()
    tasks = [(i, s, min(s + SPLIT_SIZE, file_size), k) for i, s in enumerate(range(0, file_size, SPLIT_SIZE))]
    total = len(tasks)
    ready = sum(os.path.exists(_done_marker(t[0])) for t in tasks)
    log(f"map: сплитов всего {total}, готово по чекпоинту {ready}")

    done = skipped = 0
    with multiprocessing.Pool(WORKERS) as pool:
        for _, status in pool.imap_unordered(map_task, tasks):
            if status == "skip":
                skipped += 1
            else:
                done += 1
            if (done + skipped) % 10 == 0:
                log(f"map: завершено {done + skipped}/{total} (обработал сам {done}, пропустил {skipped})")
    log(f"map завершён: обработал сам {done}, пропустил по чекпоинту {skipped}")

    with multiprocessing.Pool(REDUCERS) as pool:
        pool.map(reduce_task, range(REDUCERS))

    all_results = []
    for f_path in glob.glob(os.path.join(WORK_DIR, "part-*.txt")):
        with open(f_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) == 3:
                    key, count, median = parts
                    all_results.append((key, int(count), float(median)))

    all_results.sort(key=lambda x: (-x[2], x[0]))   # детерминированный порядок при равных медианах
    df = pd.DataFrame(all_results, columns=["stack", "cnt", "median_salary"])

    if verbose:
        print(f"\n--- MapReduce: зарплаты (k={k}) ---")
        print("  ТОП-5 повышающих:")
        print(df.head(5).to_string(index=False))
        print("  ТОП-5 понижающих:")
        print(df.tail(5).to_string(index=False))

    return df


def control_numbers(df):
    top = df.iloc[0] if len(df) else None
    return {
        "rows": int(len(df)),
        "sum_cnt": int(df["cnt"].sum()),
        "sum_median": round(float(df["median_salary"].sum()), 2),
        "top1": f"{top['stack']} ({top['median_salary']})" if top is not None else None,
    }


def setup_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{ENDPOINT_URL.replace('http://', '')}';
        SET s3_access_key_id='{ACCESS_KEY}';
        SET s3_secret_access_key='{SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con


def duckdb_base_cte():
    return f"""
    WITH raw_data AS (
        SELECT 
            list_distinct(string_split(
                replace(replace(replace(skills_list, '[', ''), ']', ''), '''', ''), ', '
            )) AS skills_array,
            CASE 
                WHEN currency = 'RUR' AND CAST(gross AS VARCHAR) = 'true' THEN salary_from * 0.87
                WHEN currency = 'RUR' THEN salary_from
                ELSE NULL 
            END AS salary
        FROM read_csv_auto('s3://{BUCKET_NAME}/{OBJECT_NAME}')
        WHERE skills_list IS NOT NULL AND salary_from IS NOT NULL
    )
    """


def duckdb_query(k):
    base = duckdb_base_cte()
    if k == 1:
        return base + """
        , exploded AS (SELECT trim(unnest(skills_array)) AS stack, salary FROM raw_data)
        SELECT stack, count(*) AS cnt, round(median(salary)) AS median_salary
        FROM exploded WHERE stack != '' AND salary IS NOT NULL
        GROUP BY stack HAVING count(*) >= 20
        ORDER BY median_salary DESC;
        """
    elif k == 2:
        return base + """
        , pairs AS (
            SELECT CASE WHEN trim(skills_array[i]) < trim(skills_array[j])
                   THEN trim(skills_array[i]) || '|' || trim(skills_array[j])
                   ELSE trim(skills_array[j]) || '|' || trim(skills_array[i]) END AS stack, salary
            FROM raw_data,
            LATERAL generate_series(1, len(skills_array)) AS t(i),
            LATERAL generate_series(1, len(skills_array)) AS u(j)
            WHERE i < j AND salary IS NOT NULL
        )
        SELECT stack, count(*) AS cnt, round(median(salary)) AS median_salary
        FROM pairs GROUP BY stack HAVING count(*) >= 20
        ORDER BY median_salary DESC;
        """
    else:
        return base + """
        , triples AS (
            SELECT list_aggregate(list_sort(
                [trim(skills_array[i]), trim(skills_array[j]), trim(skills_array[l])]
            ), 'string_agg', '|') AS stack, salary
            FROM raw_data,
            LATERAL generate_series(1, len(skills_array)) AS t(i),
            LATERAL generate_series(1, len(skills_array)) AS u(j),
            LATERAL generate_series(1, len(skills_array)) AS v(l)
            WHERE i < j AND j < l AND salary IS NOT NULL
        )
        SELECT stack, count(*) AS cnt, round(median(salary)) AS median_salary
        FROM triples WHERE stack != '' GROUP BY stack HAVING count(*) >= 20
        ORDER BY median_salary DESC;
        """


def run_duckdb_salary(k, verbose=False):
    con = setup_duckdb()
    try:
        df = con.execute(duckdb_query(k)).df()
        df.columns = ["stack", "cnt", "median_salary"]

        if verbose:
            print(f"\n--- DuckDB: зарплаты (k={k}) ---")
            print("  ТОП-5 повышающих:")
            print(df.head(5).to_string(index=False))
            print("  ТОП-5 понижающих:")
            print(df.tail(5).to_string(index=False))

        return df
    except Exception as e:
        print(f"Ошибка DuckDB k={k}: {e}")
        return pd.DataFrame(columns=["stack", "cnt", "median_salary"])


def run_duckdb_regions(top_n=3):
    con = setup_duckdb()
    query = f"""
    WITH base AS (
        SELECT area, skills_list,
               CASE 
                   WHEN currency = 'RUR' AND CAST(gross AS VARCHAR) = 'true' THEN salary_from * 0.87
                   WHEN currency = 'RUR' THEN salary_from
                   ELSE NULL 
               END AS salary
        FROM read_csv_auto('s3://{BUCKET_NAME}/{OBJECT_NAME}')
        WHERE area IS NOT NULL
    ),
    top_regions AS (
        SELECT area, COUNT(*) as total, COUNT(salary) as with_salary,
               MEDIAN(salary) as median_salary
        FROM base
        GROUP BY area
        ORDER BY total DESC
        LIMIT {top_n}
    ),
    skills_by_region AS (
        SELECT b.area, trim(unnest(
            list_distinct(string_split(
                replace(replace(replace(b.skills_list, '[', ''), ']', ''), '''', ''), ', '
            ))
        )) AS skill
        FROM base b
        JOIN top_regions t ON b.area = t.area
        WHERE b.skills_list IS NOT NULL
    ),
    skill_stats AS (
        SELECT area, skill, COUNT(*) as cnt
        FROM skills_by_region
        WHERE skill != ''
        GROUP BY area, skill
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY area ORDER BY cnt DESC) as rn
        FROM skill_stats
    )
    SELECT t.area, t.total, t.with_salary, round(t.median_salary) as median_salary,
           list_aggregate(list_sort(list(s.skill) FILTER (WHERE s.rn <= 5)), 'string_agg', ', ') as top5_skills
    FROM top_regions t
    LEFT JOIN ranked s ON t.area = s.area
    GROUP BY t.area, t.total, t.with_salary, t.median_salary
    ORDER BY t.total DESC;
    """
    try:
        df = con.execute(query).df()
        print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"Ошибка регионального анализа: {e}")
        return pd.DataFrame()