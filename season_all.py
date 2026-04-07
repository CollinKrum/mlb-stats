from datetime import date
import os
import pandas as pd
from pybaseball import batting_stats_range
import warnings
warnings.filterwarnings("ignore")
import gspread
import re
# =========================
# GOOGLE SHEETS INTEGRATION
# =========================
def upload_to_sheets(df):
    """Uploads the processed DataFrame to Google Sheets."""
    try:
        SHEET_ID = "1PHrPbnG7oB6RFPtOilw0DskShhiD7XL4IP0AAJQLj4k"
        SHEET_NAME = "Full Season Batting"  # Tab name — change if needed

        json_file = "logical-contact-467719-v2-eea0bc240cc3.json"

        gc = gspread.service_account(filename=json_file)
        sh = gc.open_by_key(SHEET_ID)

        # Try to find the tab by name, create it if it doesn't exist
        try:
            worksheet = sh.worksheet(SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=SHEET_NAME, rows="2000", cols="60")

        worksheet.clear()
        data = [df.columns.values.tolist()] + df.astype(str).values.tolist()
        worksheet.update('A1', data)

        print(f"\n✅ Google Sheet tab '{SHEET_NAME}' updated successfully.")

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
    if '\\x' not in name:
        return name
    try:
        # Replace literal \xNN sequences with actual bytes, then decode as utf-8
        byte_string = re.sub(
            r'\\x([0-9a-fA-F]{2})',
            lambda m: bytes.fromhex(m.group(1)).decode('latin-1'),
            name
        )
        return byte_string.encode('latin-1').decode('utf-8')
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
    possible = [c for c in df.columns if "name" in c.lower()]
    if possible:
        df["Name"] = df[possible[0]]
        return df
    raise ValueError("Could not find a player name column.")

def choose_team_column(df):
    if "Team" in df.columns:
        return df
    possible = [c for c in df.columns if c.lower() in ["team", "tm"]]
    df["Team"] = df[possible[0]] if possible else ""
    return df

def build_derived_stats(df):
    """Compute all derived/advanced stats on top of raw pybaseball columns."""
    needed = ["H", "2B", "3B", "HR", "BB", "SO", "AB", "PA", "SF", "HBP", "RBI", "R", "SB", "CS", "G"]
    df = ensure_columns(df, needed)

    # Singles and total bases
    df["1B"] = df["H"] - df["2B"] - df["3B"] - df["HR"]
    df["TB"] = df["1B"] + (2 * df["2B"]) + (3 * df["3B"]) + (4 * df["HR"])
    df["XBH"] = df["2B"] + df["3B"] + df["HR"]

    # Core rate stats
    df["AVG"]  = df.apply(lambda r: safe_div(r["H"],  r["AB"]), axis=1)
    df["OBP"]  = df.apply(lambda r: safe_div(r["H"] + r["BB"] + r["HBP"], r["AB"] + r["BB"] + r["HBP"] + r["SF"]), axis=1)
    df["SLG"]  = df.apply(lambda r: safe_div(r["TB"], r["AB"]), axis=1)
    df["OPS"]  = df["OBP"] + df["SLG"]
    df["ISO"]  = df["SLG"] - df["AVG"]   # Isolated power

    # BABIP: (H - HR) / (AB - SO - HR + SF)
    df["BABIP"] = df.apply(
        lambda r: safe_div(r["H"] - r["HR"], r["AB"] - r["SO"] - r["HR"] + r["SF"]), axis=1
    )

    # Plate discipline rates
    df["BB%"]  = df.apply(lambda r: safe_div(r["BB"], r["PA"]), axis=1)
    df["K%"]   = df.apply(lambda r: safe_div(r["SO"], r["PA"]), axis=1)
    df["BB/K"] = df.apply(lambda r: safe_div(r["BB"], r["SO"]), axis=1)

    # Power / contact rates
    df["HR/PA"]  = df.apply(lambda r: safe_div(r["HR"], r["PA"]), axis=1)
    df["XBH%"]   = df.apply(lambda r: safe_div(r["XBH"], r["AB"]), axis=1)
    df["H/G"]    = df.apply(lambda r: safe_div(r["H"],  r["G"]),  axis=1)
    df["HR/G"]   = df.apply(lambda r: safe_div(r["HR"], r["G"]),  axis=1)
    df["RBI/G"]  = df.apply(lambda r: safe_div(r["RBI"], r["G"]), axis=1)

    # Baserunning
    df["SB_pct"] = df.apply(
        lambda r: safe_div(r["SB"], r["SB"] + r["CS"]), axis=1
    )

    # Trend score (composite hot/cold signal)
    df["TrendScore"] = (
        (df["OPS"]   * 100) +
        (df["HR/PA"] *  50) +
        (df["ISO"]   *  30) +
        (df["BB%"]   *  10) -
        (df["K%"]    *  15)
    ).round(2)

    return df

def format_rates(df):
    """Round rate stats to readable decimals."""
    df = df.copy()
    round3 = ["AVG", "OBP", "SLG", "OPS", "ISO", "BABIP", "BB%", "K%", "BB/K",
              "HR/PA", "XBH%", "H/G", "HR/G", "RBI/G", "SB_pct"]
    for col in round3:
        if col in df.columns:
            df[col] = df[col].round(3)
    return df

def save_csv(df, filepath):
    df.to_csv(filepath, index=False, encoding="utf-8-sig")


# =========================
# SETTINGS
# =========================
SEASON_START = "2026-03-25"   # Update each year when Opening Day changes
MIN_AB       = 1              # Include anyone with at least 1 AB
SORT_BY      = "H"          # Column to sort output by
OUTPUT_DIR   = "output"

# Ordered display columns — everything available will also be saved to CSV/Sheets
DISPLAY_COLS = [
    "Name", "Team", "G", "PA", "AB", "H", "1B", "2B", "3B", "HR",
    "R", "RBI", "BB", "SO", "HBP", "SF", "SB", "CS", "TB", "XBH",
    "AVG", "OBP", "SLG", "OPS", "ISO", "BABIP",
    "BB%", "K%", "BB/K", "HR/PA", "XBH%",
    "H/G", "HR/G", "RBI/G", "SB_pct", "TrendScore"
]


# =========================
# MAIN
# =========================
def main():
    today       = date.today()
    start_date  = SEASON_START
    end_date    = str(today)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"\nPulling full season batting stats: {start_date} → {end_date}\n")

    # --- Pull data ---
    try:
        df = batting_stats_range(start_date, end_date)
    except Exception as e:
        print(f"❌ Data pull failed: {e}")
        return

    if df is None or df.empty:
        print("No data returned. Games may not have started yet for this date range.")
        return

    # --- Normalize ---
    df = choose_name_column(df)
    df = choose_team_column(df)
    df["Name"] = df["Name"].apply(fix_name_encoding)

    # --- Filter to players with at least 1 AB ---
    df = ensure_columns(df, ["AB"])
    df = df[df["AB"] >= MIN_AB].copy()

    if df.empty:
        print(f"No players found with {MIN_AB}+ AB.")
        return

    # --- Compute all derived stats ---
    df = build_derived_stats(df)
    df = format_rates(df)

    # --- Sort ---
    if SORT_BY in df.columns:
        df = df.sort_values(SORT_BY, ascending=False)

    # --- Build output DataFrame (keep all available cols + derived) ---
    out_cols = [c for c in DISPLAY_COLS if c in df.columns]
    # Also append any raw pybaseball columns not already in our list
    extra_raw = [c for c in df.columns if c not in out_cols]
    full_out  = df[out_cols + extra_raw].copy()

    # --- Save CSV ---
    csv_path = os.path.join(OUTPUT_DIR, f"full_season_batting_{end_date}.csv")
    save_csv(full_out, csv_path)
    print(f"💾 CSV saved → {csv_path}")

    # --- Terminal preview (top 30) ---
    preview_cols = ["Name", "Team", "G", "PA", "AB", "H", "HR", "RBI",
                    "AVG", "OBP", "SLG", "OPS", "ISO", "BABIP", "BB%", "K%", "TrendScore"]
    preview_cols = [c for c in preview_cols if c in full_out.columns]
    print("\n" + "=" * 130)
    print(f"FULL SEASON BATTING — TOP 30 by {SORT_BY} (preview) | as of {end_date}")
    print("=" * 130)
    print(full_out[preview_cols].head(30).to_string(index=False))
    print(f"\nTotal players with {MIN_AB}+ AB: {len(full_out)}")

    # --- Upload to Sheets ---
    upload_to_sheets(full_out)


if __name__ == "__main__":
    main()