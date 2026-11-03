import sys
from steps import run_mapreduce_salary, control_numbers

if __name__ == "__main__":
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    df = run_mapreduce_salary(k)
    print("КОНТРОЛЬНЫЕ ЧИСЛА (эталон):", control_numbers(df))

