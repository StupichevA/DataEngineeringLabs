import sys
import itertools


def get_combinations(skills, k):
    if len(skills) < k:
        return []
    skills = sorted(list(set(skills)))
    return itertools.combinations(skills, k)


def main():
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('|')
        if len(parts) < 2:
            continue

        skills_raw, salary_str = parts[0], parts[1]

        try:
            salary = float(salary_str)
        except ValueError:
            continue

        skills = [s.strip() for s in skills_raw.split(';') if s.strip()]

        for combo in get_combinations(skills, k):
            key = "|".join(combo)
            print(f"{key}\t{salary}")


if __name__ == "__main__":
    main()