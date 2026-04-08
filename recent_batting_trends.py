from datetime import date, timedelta
import os
import pandas as pd
from pybaseball import batting_stats_range
import warnings
import gspread

warnings.filterwarnings("ignore")

# =========================
# GOOGLE SHEETS INTEGRATION
# =========================
def upload_to_sheets(df):
    """Uploads the processed DataFrame to Google Sheets using a mounted secret file."""
    try:
        SHEET_ID = "1PHrPbnG7oB6RFPtOilw0DskShhiD7XL4IP0AAJQLj4k"
        json_file = os.environ.get("GOOGLE_CREDS_PATH", "/etc/secrets/google-credentials.json")

        gc = gspread.service_account(filename=json_file)
        sh = gc.open_by_key(SHEET_ID)
        worksheet = sh.worksheet("Recent Hot Hitters")

        worksheet.clear()
        data = [df.columns.values.tolist()] + df.astype(str).values.tolist()
        worksheet.update("A1", data)

        print(f"\n✅ Google Sheet updated successfully via ID: {SHEET_ID}")

    except Exception as e:
        print(f"\n❌ Failed to update Google Sheets: {e}")


# =========================
# HELPERS
# =========================
def safe_div(numerator, denominator):
    if denominator is None or denominator == 0:
        return 0
    return numerator / denominator


def fix_name_encoding(name):
    if not isinstance(name, str):
        return name
    try:
        return name.encode("latin1").decode("utf-8")
    except Exception:
        return name


def ensure_columns(df, columns):
    for col in columns:
        if col not in df.columns:
            df[col] = 0
    return df


def build_rate_stats(df):
    needed = ["H", "2B", "3B", "HR", "BB", "SO", "AB", "PA", "SF", "HBP", "RBI"]
    df = ensure_columns(df, needed)
    df["1B"] = df["H"] - df["2B"] - df["3B"] - df["HR"]
    df["TB"] = df["1B"] + (2 * df["2B"]) + (3 * df["3B"]) + (4 * df["HR"])
    df["AVG"] = df.apply(lambda r: safe_div(r["H"], r["AB"]), axis=1)
    df["OBP"] = df.apply(
        lambda r: safe_div(r["H"] + r["BB"] + r["HBP"], r["AB"] + r["BB"] + r["HBP"] + r["SF"]),
        axis=1,
    )
    df["SLG"] = df.apply(lambda r: safe_div(r["TB"], r["AB"]), axis=1)
    df["OPS"] = df["OBP"] + df["SLG"]
    return df


def choose_name_column(df):
    if "Name" in df.columns:
        return df
    possible_name_cols = [c for c in df.columns if "name" in c.lower()]
    if possible_name_cols:
        df["Name"] = df[possible_name_cols[0]]
        return df
    raise ValueError("Could not find a player name column.")


def choose_team_column(df):
    if "Team" in df.columns:
        return df
    possible_team_cols = [c for c in df.columns if c.lower() in ["team", "tm"]]
    df["Team"] = df[possible_team_cols[0]] if possible_team_cols else ""
    return df


def format_rates(df):
    df = df.copy()
    for col in ["AVG", "OBP", "SLG", "OPS"]:
        if col in df.columns:
            df[col] = df[col].round(3)
    return df


def build_hot_cold_score(df):
    df = df.copy()
    df["BB_rate"] = df.apply(lambda r: safe_div(r["BB"], r["PA"]), axis=1)
    df["K_rate"] = df.apply(lambda r: safe_div(r["SO"], r["PA"]), axis=1)
    df["HR_rate"] = df.apply(lambda r: safe_div(r["HR"], r["PA"]), axis=1)
    df["H_rate"] = df.apply(lambda r: safe_div(r["H"], r["PA"]), axis=1)
    df["TrendScore"] = (
        (df["OPS"] * 100)
        + (df["HR_rate"] * 50)
        + (df["H_rate"] * 20)
        + (df["BB_rate"] * 10)
        - (df["K_rate"] * 15)
    ).round(2)
    return df


def save_csv(df, filepath):
    df.to_csv(filepath, index=False, encoding="utf-8-sig")


# =========================
# SETTINGS
# =========================
DAYS_BACK = 10
MIN_PA = 10
TOP_N = 50
SORT_BY = "H"
OUTPUT_DIR = "output"


# =========================
# MAIN
# =========================
def main():
    today = date.today()
    start_date = today - timedelta(days=DAYS_BACK)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\nPulling MLB batting stats from {start_date} to {today}...\n")

    def safe_batting_pull(start, end):
        try:
            return batting_stats_range(str(start), str(end))
        except Exception as e:
            print(f"Primary pull failed: {e}")
            return None

    df = safe_batting_pull(start_date, today)

    if df is None or df.empty:
        print("No data found for requested range. Check if games were played.")
        return

    df = choose_name_column(df)
    df = choose_team_column(df)
    df["Name"] = df["Name"].apply(fix_name_encoding)
    df = build_rate_stats(df)
    df = build_hot_cold_score(df)
    df = df[df["PA"] >= MIN_PA].copy()

    if df.empty:
        print(f"No hitters found with at least {MIN_PA} PA.")
        return

    hot = df.sort_values(SORT_BY, ascending=False).head(TOP_N).copy()
    show_cols = [
        "Name",
        "Team",
        "PA",
        "AB",
        "H",
        "HR",
        "RBI",
        "BB",
        "SO",
        "AVG",
        "OBP",
        "SLG",
        "OPS",
        "TrendScore",
    ]
    show_cols = [c for c in show_cols if c in df.columns]
    hot_out = format_rates(hot[show_cols].copy())

    print("=" * 110)
    print(f"TOP {TOP_N} HOTTEST HITTERS | Sorted by {SORT_BY}")
    print("=" * 110)
    print(hot_out.to_string(index=False))

    upload_to_sheets(hot_out)


if __name__ == "__main__":
    main()
