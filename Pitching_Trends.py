from datetime import date, timedelta
import os
import pandas as pd
from pybaseball import pitching_stats_range
import warnings
warnings.filterwarnings("ignore")

# =========================
# GOOGLE SHEETS INTEGRATION
# =========================
def upload_to_sheets(df):
    """Uploads the processed DataFrame to Google Sheets using the unique ID."""
    try:
        SHEET_ID = "1PHrPbnG7oB6RFPtOilw0DskShhiD7XL4IP0AAJQLj4k"  # <-- update if using a different sheet

        import gspread
        json_file = "logical-contact-467719-v2-eea0bc240cc3.json"
        gc = gspread.service_account(filename=json_file)

        sh = gc.open_by_key(SHEET_ID)
        worksheet = sh.worksheet("Recent Hot Pitchers")  # must match the tab name exactly

        worksheet.clear()
        data = [df.columns.values.tolist()] + df.astype(str).values.tolist()
        worksheet.update('A1', data)

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

def build_pitching_rates(df):
    """
    Calculates ERA, WHIP, K/9, BB/9, K/BB, H/9, HR/9, and a FIP-lite.
    pybaseball pitching_stats_range columns (common): IP, H, ER, BB, SO, HR, HBP, BF (batters faced)
    """
    needed = ["IP", "H", "ER", "BB", "SO", "HR", "HBP", "BF", "R"]
    df = ensure_columns(df, needed)

    # Innings pitched as a float (e.g. 6.2 -> 6.667)
    def ip_to_float(ip):
        try:
            whole = int(ip)
            partial = round(ip % 1 * 10)  # .1 -> 1 out, .2 -> 2 outs
            return whole + partial / 3
        except Exception:
            return 0

    df["IP_float"] = df["IP"].apply(ip_to_float)

    df["ERA"]   = df.apply(lambda r: round(safe_div(r["ER"] * 9, r["IP_float"]), 2), axis=1)
    df["WHIP"]  = df.apply(lambda r: round(safe_div(r["H"] + r["BB"], r["IP_float"]), 3), axis=1)
    df["K9"]    = df.apply(lambda r: round(safe_div(r["SO"] * 9, r["IP_float"]), 2), axis=1)
    df["BB9"]   = df.apply(lambda r: round(safe_div(r["BB"] * 9, r["IP_float"]), 2), axis=1)
    df["HR9"]   = df.apply(lambda r: round(safe_div(r["HR"] * 9, r["IP_float"]), 2), axis=1)
    df["H9"]    = df.apply(lambda r: round(safe_div(r["H"] * 9, r["IP_float"]), 2), axis=1)
    df["KBB"]   = df.apply(lambda r: round(safe_div(r["SO"], r["BB"]) if r["BB"] > 0 else r["SO"], 2), axis=1)

    # FIP-lite (no cFIP constant, so just the numerator — useful for relative comparison)
    # FIP = (13*HR + 3*BB - 2*SO) / IP  [constant omitted for ranking purposes]
    df["FIP_lite"] = df.apply(
        lambda r: round(safe_div((13 * r["HR"]) + (3 * r["BB"]) - (2 * r["SO"]), r["IP_float"]), 2),
        axis=1
    )

    return df

def build_pitcher_trend_score(df):
    """
    Higher is better:
      + K/9 rewarded heavily
      + K/BB ratio rewarded
      - ERA penalized
      - WHIP penalized
      - HR/9 penalized
    """
    df = df.copy()
    df["TrendScore"] = (
        (df["K9"]      *  5.0)
      + (df["KBB"]     *  8.0)
      - (df["ERA"]     *  6.0)
      - (df["WHIP"]    * 15.0)
      - (df["HR9"]     * 10.0)
    ).round(2)
    return df

def save_csv(df, filepath):
    df.to_csv(filepath, index=False, encoding="utf-8-sig")


# =========================
# SETTINGS
# =========================
DAYS_BACK  = 10
MIN_IP     = 3        # minimum innings pitched to qualify
TOP_N      = 50
SORT_BY    = "SO"     # change to "ERA", "TrendScore", "K9", etc.
OUTPUT_DIR = "output"


# =========================
# MAIN
# =========================
def main():
    today      = date.today()
    start_date = today - timedelta(days=DAYS_BACK)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\nPulling MLB pitching stats from {start_date} to {today}...\n")

    def safe_pitching_pull(start, end):
        try:
            return pitching_stats_range(str(start), str(end))
        except Exception as e:
            print(f"Primary pull failed: {e}")
            return None

    df = safe_pitching_pull(start_date, today)

    if df is None or df.empty:
        print("No data found for requested range. Check if games were played.")
        return

    # Normalization
    df = choose_name_column(df)
    df = choose_team_column(df)
    df["Name"] = df["Name"].apply(fix_name_encoding)

    # Build metrics
    df = build_pitching_rates(df)
    df = build_pitcher_trend_score(df)

    # Filter by minimum IP
    df = df[df["IP_float"] >= MIN_IP].copy()

    if df.empty:
        print(f"No pitchers found with at least {MIN_IP} IP.")
        return

    # Sort and slice
    hot = df.sort_values(SORT_BY, ascending=(SORT_BY in ["ERA", "WHIP", "FIP_lite", "BB9", "HR9"])).head(TOP_N).copy()

    show_cols = [
        "Name", "Team", "IP", "H", "ER", "BB", "SO", "HR",
        "ERA", "WHIP", "K9", "BB9", "HR9", "KBB", "FIP_lite", "TrendScore"
    ]
    show_cols = [c for c in show_cols if c in hot.columns]
    hot_out = hot[show_cols].copy()

    # Terminal Output
    print("=" * 130)
    print(f"TOP {TOP_N} PITCHERS (last {DAYS_BACK} days) | Sorted by {SORT_BY}")
    print("=" * 130)
    print(hot_out.to_string(index=False))

    # Upload
    upload_to_sheets(hot_out)


if __name__ == "__main__":
    main()