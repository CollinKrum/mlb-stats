from datetime import date
import os
import pandas as pd
from pybaseball import pitching_stats_range
import warnings
warnings.filterwarnings("ignore")
import gspread

# =========================
# SETTINGS
# =========================
SEASON_START = "2026-03-25"   # Update each year when Opening Day changes
MIN_IP       = 0.1            # Include anyone who has thrown a pitch (1/3 inning)
SORT_BY      = "ERA"          # Column to sort output by
SORT_ASC     = True           # True = low is best (ERA, WHIP, BB9), False = high is best (K9, FIP)
OUTPUT_DIR   = "output"

# Starter = started every appearance | Reliever = never started | Swingman = mixed
STARTER_GS_PCT  = 0.8   # >= 80% of appearances are starts → starter
RELIEVER_GS_PCT = 0.0   # 0% starts → reliever (anything in between = swingman, goes in all + relievers)

# =========================
# GOOGLE SHEETS INTEGRATION
# =========================
SHEET_ID = "1PHrPbnG7oB6RFPtOilw0DskShhiD7XL4IP0AAJQLj4k"
JSON_FILE = os.environ.get("GOOGLE_CREDS_PATH", "/etc/secrets/google-credentials.json")

TAB_ALL       = "All Pitchers"
TAB_STARTERS  = "Starters"
TAB_RELIEVERS = "Relievers"

def clean_for_sheets(df):
    """Replace inf/-inf/NaN with 0 so JSON serialization doesn't choke."""
    import numpy as np
    df = df.copy()
    df.replace([np.inf, -np.inf], 0, inplace=True)
    df.fillna(0, inplace=True)
    return df

def upload_tab(sh, df, tab_name):
    """Upload a DataFrame to a specific tab, creating it if needed."""
    try:
        try:
            worksheet = sh.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=tab_name, rows="2000", cols="80")

        worksheet.clear()
        df = clean_for_sheets(df)
        data = [df.columns.values.tolist()] + df.astype(str).values.tolist()
        worksheet.update('A1', data)
        print(f"  ✅ Tab '{tab_name}' updated ({len(df)} pitchers)")
    except Exception as e:
        print(f"  ❌ Failed to update tab '{tab_name}': {e}")

def upload_to_sheets(all_df, sp_df, rp_df):
    """Upload all three splits to separate tabs in the same Google Sheet."""
    try:
        gc = gspread.service_account(filename=JSON_FILE)
        sh = gc.open_by_key(SHEET_ID)
        print("\nUploading to Google Sheets...")
        upload_tab(sh, all_df, TAB_ALL)
        upload_tab(sh, sp_df,  TAB_STARTERS)
        upload_tab(sh, rp_df,  TAB_RELIEVERS)
    except Exception as e:
        print(f"\n❌ Google Sheets connection failed: {e}")

# =========================
# HELPERS
# =========================
def safe_div(a, b):
    return 0 if (b is None or b == 0) else a / b

def fix_name_encoding(name):
    if not isinstance(name, str):
        return name
    try:
        return name.encode("latin1").decode("utf-8")
    except Exception:
        return name

def ensure_columns(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = 0
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

def ip_to_float(ip):
    """
    Convert baseball innings notation to a real float:
    5.0 -> 5.0
    5.1 -> 5 + 1/3
    5.2 -> 5 + 2/3
    """
    try:
        ip = float(ip)
        whole = int(ip)
        frac = round((ip - whole) * 10)
        if frac == 1:
            return whole + (1 / 3)
        if frac == 2:
            return whole + (2 / 3)
        return float(whole)
    except Exception:
        return 0.0

def build_derived_stats(df):
    """Compute all derived/advanced pitching stats."""
    needed = ["IP", "H", "ER", "R", "BB", "SO", "HR", "HBP", "BF",
              "G", "GS", "W", "L", "SV", "HLD", "BS", "CG", "SHO"]
    df = ensure_columns(df, needed)

    # Convert innings to usable float for calculations
    df["IP_calc"] = df["IP"].apply(ip_to_float)

    # Core rate stats
    df["ERA"]   = df.apply(lambda r: round(safe_div(r["ER"] * 9, r["IP_calc"]), 2), axis=1)
    df["WHIP"]  = df.apply(lambda r: round(safe_div(r["H"] + r["BB"], r["IP_calc"]), 3), axis=1)
    df["K9"]    = df.apply(lambda r: round(safe_div(r["SO"] * 9, r["IP_calc"]), 2), axis=1)
    df["BB9"]   = df.apply(lambda r: round(safe_div(r["BB"] * 9, r["IP_calc"]), 2), axis=1)
    df["HR9"]   = df.apply(lambda r: round(safe_div(r["HR"] * 9, r["IP_calc"]), 2), axis=1)
    df["H9"]    = df.apply(lambda r: round(safe_div(r["H"]  * 9, r["IP_calc"]), 2), axis=1)

    # Plate discipline / ratios
    df["K/BB"]  = df.apply(lambda r: round(safe_div(r["SO"], r["BB"]), 2), axis=1)
    df["K%"]    = df.apply(lambda r: round(safe_div(r["SO"], r["BF"]), 3), axis=1)
    df["BB%"]   = df.apply(lambda r: round(safe_div(r["BB"], r["BF"]), 3), axis=1)
    df["HR/BF"] = df.apply(lambda r: round(safe_div(r["HR"], r["BF"]), 3), axis=1)

    # FIP-lite (constant omitted for relative comparison)
    df["FIP_raw"] = df.apply(
        lambda r: round(safe_div((13 * r["HR"]) + (3 * r["BB"]) - (2 * r["SO"]), r["IP_calc"]), 2),
        axis=1
    )

    # Simplified BABIP for pitchers
    df["BABIP"] = df.apply(
        lambda r: round(safe_div(r["H"] - r["HR"], r["BF"] - r["BB"] - r["SO"] - r["HR"]), 3), axis=1
    )

    # LOB% proxy
    df["LOB%"] = df.apply(
        lambda r: round(
            safe_div(r["H"] + r["BB"] - r["R"], r["H"] + r["BB"] - (1.4 * r["HR"])), 3
        ), axis=1
    )

    # Role classification
    df["GS_pct"] = df.apply(lambda r: safe_div(r["GS"], r["G"]), axis=1)
    df["Role"] = df["GS_pct"].apply(
        lambda x: "SP" if x >= STARTER_GS_PCT else ("RP" if x == RELIEVER_GS_PCT else "SW")
    )

    # IP per appearance
    df["IP/G"] = df.apply(lambda r: round(safe_div(r["IP_calc"], r["G"]), 2), axis=1)

    # Composite TrendScore
    df["TrendScore"] = (
        (df["K9"]    *  5.0) +
        (df["K/BB"]  * 10.0) +
        (df["K%"]    * 30.0) -
        (df["BB%"]   * 20.0) -
        (df["HR9"]   * 10.0) -
        (df["WHIP"]  * 10.0)
    ).round(2)

    return df

def format_df(df):
    df = df.copy()
    int_cols = ["G", "GS", "W", "L", "SV", "HLD", "BS", "CG", "SHO",
                "H", "ER", "R", "BB", "SO", "HR", "HBP", "BF"]
    for c in int_cols:
        if c in df.columns:
            try:
                df[c] = df[c].astype(float).astype(int)
            except Exception:
                pass
    return df

def save_csv(df, path):
    df.to_csv(path, index=False, encoding="utf-8-sig")

# =========================
# DISPLAY COLUMN ORDER
# =========================
DISPLAY_COLS = [
    "Name", "Team", "Role", "G", "GS", "W", "L", "SV", "HLD", "BS", "CG", "SHO",
    "IP", "IP/G", "BF", "H", "R", "ER", "HR", "BB", "HBP", "SO",
    "ERA", "WHIP", "K9", "BB9", "HR9", "H9",
    "K/BB", "K%", "BB%", "HR/BF",
    "FIP_raw", "BABIP", "LOB%",
    "TrendScore"
]

# =========================
# MAIN
# =========================
def main():
    today     = date.today()
    end_str   = str(today)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\nPulling full season pitching stats: {SEASON_START} → {end_str}\n")

    try:
        df = pitching_stats_range(SEASON_START, end_str)
    except Exception as e:
        print(f"❌ Data pull failed: {e}")
        return

    if df is None or df.empty:
        print("No pitching data returned.")
        return

    # --- Normalize ---
    df = choose_name_column(df)
    df = choose_team_column(df)
    df["Name"] = df["Name"].apply(fix_name_encoding)

    # --- Filter to MIN_IP ---
    df = ensure_columns(df, ["IP"])
    df["IP_calc"] = df["IP"].apply(ip_to_float)
    df = df[df["IP_calc"] >= MIN_IP].copy()

    if df.empty:
        print("No pitchers found after IP filter.")
        return

    # --- Compute derived stats + role ---
    df = build_derived_stats(df)
    df = format_df(df)

    # --- Build output column order ---
    out_cols  = [c for c in DISPLAY_COLS if c in df.columns]
    extra_raw = [c for c in df.columns if c not in out_cols and c not in ["GS_pct", "IP_calc"]]
    full_out  = df[out_cols + extra_raw].copy()

    # --- Sort ---
    if SORT_BY in full_out.columns:
        full_out = full_out.sort_values(SORT_BY, ascending=SORT_ASC)

    # --- Split by role ---
    sp_out = full_out[full_out["Role"] == "SP"].copy()
    rp_out = full_out[full_out["Role"].isin(["RP", "SW"])].copy()

    # --- Save CSVs ---
    all_path = os.path.join(OUTPUT_DIR, f"pitching_all_{end_str}.csv")
    sp_path  = os.path.join(OUTPUT_DIR, f"pitching_sp_{end_str}.csv")
    rp_path  = os.path.join(OUTPUT_DIR, f"pitching_rp_{end_str}.csv")
    save_csv(full_out, all_path)
    save_csv(sp_out,   sp_path)
    save_csv(rp_out,   rp_path)
    print(f"💾 CSVs saved → {OUTPUT_DIR}/")

    # --- Terminal preview ---
    preview_cols = ["Name", "Team", "Role", "G", "GS", "IP", "W", "L", "SV",
                    "ERA", "WHIP", "K9", "BB9", "K/BB", "FIP_raw", "TrendScore"]
    preview_cols = [c for c in preview_cols if c in full_out.columns]

    print("\n" + "=" * 120)
    print(f"ALL PITCHERS — TOP 30 by {SORT_BY} | Full Season as of {end_str}")
    print("=" * 120)
    print(full_out[preview_cols].head(30).to_string(index=False))

    print(f"\n{'='*60}")
    print(f"  Total pitchers: {len(full_out)}  |  SP: {len(sp_out)}  |  RP/SW: {len(rp_out)}")
    print(f"{'='*60}")

    # --- Upload to Sheets ---
    upload_to_sheets(full_out, sp_out, rp_out)

if __name__ == "__main__":
    main()
