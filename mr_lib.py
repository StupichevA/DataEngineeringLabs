import io
import pandas as pd


def prepare_lines(raw_data: bytes):
    out = []
    try:
        df = pd.read_csv(io.BytesIO(raw_data), header=None, on_bad_lines="skip")
    except Exception as e:
        print(f"ОШИБКА ЧТЕНИЯ CSV: {e}", flush=True)
        return out
    for _, row in df.iterrows():
        if len(row) < 8:
            continue
        if pd.isna(row.iloc[-1]):
            continue
        skills = str(row.iloc[-1])
        skills = skills.replace("[", "").replace("]", "").replace("'", "").replace('"', "")
        skills = skills.replace(",", ";")
        try:
            salary = float(row.iloc[3]) if pd.notna(row.iloc[3]) else 0
            currency = str(row.iloc[5]) if pd.notna(row.iloc[5]) else "RUR"
            gross = str(row.iloc[6]).lower() if pd.notna(row.iloc[6]) else "false"
            if currency != "RUR" or salary == 0:
                continue
            if gross == "true":
                salary *= 0.87
            out.append(f"{skills}|{salary}")
        except Exception:
            continue
    return out