import sys
import statistics


def main():
    current_key = None
    salaries = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            key, salary_str = line.split('\t', 1)
            salary = float(salary_str)
        except ValueError:
            continue
        if current_key == key:
            salaries.append(salary)
        else:
            if current_key and len(salaries) >= 20:
                print(f"{current_key}\t{len(salaries)}\t{statistics.median(salaries)}")
            current_key = key
            salaries = [salary]
    if current_key and len(salaries) >= 20:
        print(f"{current_key}\t{len(salaries)}\t{statistics.median(salaries)}")


if __name__ == "__main__":
    main()