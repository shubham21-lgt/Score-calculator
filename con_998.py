# =============================================================================
# Consumer Scorecard V4  —  FINAL END-TO-END PIPELINE
# VERSION: con_14  (all product_mapping + bank_mapping fixes applied)
# =============================================================================
print("=" * 60)
print("  RUNNING con_v3.py")

# ===========================================================================
# OUTPUT MODE FLAG
# ===========================================================================
# INCLUDE_ALL_COLUMNS = True  → output ALL scorecard variables (full report)
# INCLUDE_ALL_COLUMNS = False → output SCORE ONLY (5 columns):
#     cust_id, scorecard_name, final_score, score_breakdown,
#     is_eligible_for_st2, is_eligible_for_st3
#
# Override at runtime via environment variable:
#   SCORECARD_FULL_OUTPUT=false python con_25.py ...   → score only
#   SCORECARD_FULL_OUTPUT=true  python con_25.py ...   → full output
# ---------------------------------------------------------------------------
import os as _os_flag
INCLUDE_ALL_COLUMNS: bool = _os_flag.environ.get(
    "SCORECARD_FULL_OUTPUT", "true"
).strip().lower() not in ("false", "0", "no")
print(f"  Output mode: {'FULL (all columns)' if INCLUDE_ALL_COLUMNS else 'SCORE ONLY (5 columns)'}")
print("=" * 60)

#
# INPUT DATASETS
# ─────────────────────────────────────────────────────────────────────────────
#   trade_data      : raw bureau trade CSV  (1,048,575 rows, 24 cols)
#                     Columns: accno, cons_acct_key, open_dt, acct_nb,
#                              closed_dt, bureau_mbr_id, account_type_cd,
#                              term_freq, charge_off_amt, resp_code,
#                              balance_dt, account_status_cd,
#                              actual_pymnt_amt, balance_amt, credit_lim_amt,
#                              original_loan_amt, past_due_amt, pay_rating_cd,
#                              dayspastdue, written_off_amt,
#                              principal_written_off,
#                              SUITFILED_WILFULDEFAULT,
#                              SUITFILEDWILLFULDEFAULT,
#                              WRITTEN_OFF_AND_SETTLED
#
#   account_mapping : cust_id ↔ score_dt only (real schema: cust_id + app_date)
#                     Columns: cust_id (LONG), app_date (INTEGER yyyyMMdd)
#                     Pipeline derives: accno=cust_id, score_dt=app_date, relFinCd=1
#
#   product_mapping : account_type_cd → Sec_Uns, Reg_Com, Sec_Mov, Pdt_Cls
#                     Real format: all values wrapped in b'' byte-string notation
#                       e.g. account_type_cd="b'5'", Sec_Uns="b'UnSec'"
#                     Pipeline strips b'' wrapping on load.
#                     Sec_Uns values: "UnSec"/"Sec" (NOT "U"/"S")
#                     Pdt_Cls values: "RegUns"/"RegSec"/"ComUns"/"ComSec"/"Nth"/"Govt"
#                     Sec_Mov values: "SecMov"/"Rest"  Reg_Com: "Regular"/"NonAgriPSL"/"AgriPSL"
#
#   bank_mapping    : BUREAU_MBR_ID (INTEGER) → Category (PSB/PVT/NBF/COB/SFB/FOR/FIN)
#
# PIPELINE PHASES
# ─────────────────────────────────────────────────────────────────────────────
#   Phase 0 : SparkSession, imports, constants
#   Phase 1 : fact2 build  — join trade + account_mapping + product_mapping + bank_mapping
#   Phase 2 : Per-month variables (dpd_new, modified_limit, derog_flag, product flags)
#   Phase 3 : Per-account aggregation (mob, monthly_data array, worst-delq)
#   Phase 4 : Global attributes  (4A–4K core  +  4L–4V new attrs)
#   Phase 5 : Scorecard trigger + final score
#   Phase 6 : Output
#


import os
os.environ["JAVA_TOOL_OPTIONS"] = (
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED"
)

# ---------------------------------------------------------------------------
# PHASE 0 — IMPORTS & SPARK SESSION
# ---------------------------------------------------------------------------
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, ArrayType, FloatType
)
from pyspark.sql.functions import (
    col, lit, when, coalesce, greatest, least,
    collect_list, struct, sort_array, explode,
    array_sort, expr, udf,
    sum as fsum, max as fmax, min as fmin, avg as favg,
    count, countDistinct, lag, lead, first,
    broadcast, row_number,
    datediff, months_between, to_date, year, month
)
import pandas as pd
import numpy as np
import math as _math

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ConsumerScorecardV4_Final") \
    .config("spark.driver.memory",          "4g") \
    .config("spark.executor.memory",        "4g") \
    .config("spark.driver.maxResultSize",   "2g") \
    .config("spark.sql.execution.arrow.pyspark.enabled",          "false") \
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.security.authentication", "simple") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.driver.extraJavaOptions",
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
    .getOrCreate()



# ── Arrow / Java 25 fix: use Unsafe allocator instead of Netty ───────────
# The JVM flag -Darrow.memory.manager=unsafe (set in SparkSession config)
# bypasses the Netty allocator that crashes on Java 25.
# arrow_on() is now a no-op kept for call-site compatibility.
import contextlib

@contextlib.contextmanager
def arrow_on():
    """No-op: Arrow always enabled; Netty bypassed via -Darrow.memory.manager=unsafe."""
    yield

spark.sparkContext.setLogLevel("ERROR")


# ===========================================================================
# 0A. PRODUCT-CODE CONFIGURATION
# ===========================================================================
# All codes derived from product_mapping.csv (59 rows, b'' stripped).
# Pdt_Cls column is the authoritative source for category assignment.
# ---------------------------------------------------------------------------

# CC = Credit Card: types 5, 213, 225 only (RegUns Regular confirmed CC).
# REMOVED 121 (ComUns), 214 (ComUns), 220 (RegSec) — all wrong per mapping.
CC_CODES        = {"5", "213", "220", "225"}  # FIXED: org_car L398/L1582 & custom_var L221: cc_prods={5,213,220,225}. 196=KCC is UnSec not CC. 220=RegSec IS CC per both references.

CL_CODES        = {"189"}  # Java allCLProductCodes = {189} only (CD/Credit Line)
HL_CODES        = {"002", "058", "2", "58", "168", "240"}  # FIX: org_car.py L1580 cond['hl']=[58,168,240]. 195=LAP only in hl_lap, NOT in hl.
AL_CODES        = {"47", "047", "221"}  # FIX: Java allALProductCodes={047,221}. 173=TW not AL — caused bidirectional nbr_al miscounts
TWO_W_CODES     = {"013", "173", "13"}
GL_CODES        = {"191", "243"}  # custom_var L222: gl_prods={191,243}. 7/007 not in product_mapping

# PL = Personal Loan: Pdt_Cls=RegUns MINUS CC types (5, 213, 225).
# ADDED: 169, 170, 189, 242, 245, 247 (all RegUns per mapping, not in old set).
# REMOVED: 172 (ComSec), 195 (ComSec), 215/216/217 (Nth), 219 (ComSec),
#          221 (RegSec), 222 (ComSec), 243 (ComSec) — all wrong per mapping.
PL_CODES        = {"123", "242"}  # FIX 1: Java bru.py L461 uses {123,242} only. 169 for EMI only.
# EMI product codes (custom_var.py) — SEPARATE from count codes above
# These are used only in the EMI outflow loop dispatch
EMI_HL_CODES    = {"58", "195"}        # custom_var.py line 1040: HL/LAP for EMI
EMI_PL_CODES    = {"123", "228"}       # custom_var.py line 1036: PL for EMI
EMI_AL_TW_CODES = {"47", "173"}        # custom_var.py: AL/TW for EMI dispatch (173=TW for EMI, NOT for AL count)
EMI_CD_CODES    = {"189"}              # custom_var.py line 1044: CD for EMI (same)
  # custom_var.py confirmed: PL = codes 123 and 228  # custom_var.py: PL=[123,228], removed 169/242  # Java: only these 3 for nbr_pl counts

AGRI_CODES      = {"167", "177", "178", "179", "198", "199", "200", "223", "224", "226", "227"}

# COM_SEC = Commercial Secured: all Pdt_Cls=ComSec from mapping.
# ADDED: 172, 184, 185, 195, 219, 222, 223, 248.
# REMOVED: 007 (not in mapping), 176 (ComUns), 228 (ComUns).
COM_SEC_CODES   = {"172", "175", "184", "185", "191", "195", "219", "222", "223",
                   "241", "243", "248"}  # 243 ADDED: Pdt_Cls=ComSec in product_mapping (was missing → LOW errors in nbr_comsec)

# COM_UNSEC = Commercial Unsecured: all Pdt_Cls=ComUns from mapping.
# ADDED: 121, 167, 181, 187, 197, 198, 199, 200, 214, 244, 249, 250, 251, 252.
COM_UNSEC_CODES = {"121", "167", "176", "177", "178", "179", "181", "187",
                   "197", "198", "199", "200", "214", "228", "244", "249",
                   "250", "251", "252"}

REGULAR_CODES   = {
    # Aligned with product_mapping Reg_Com='Regular'
    "5", "005", "47", "047", "58", "058",
    "123", "130", "168", "169", "170", "173",
    "189", "196", "213", "220", "221", "225",
    "240", "242", "243", "244", "245", "246", "247",
}
PL_CD_TW_CODES  = {"173", "013", "13", "123", "242", "189"}   # PL + CD + TW as per spec
LAP_HL_CODES    = {"058", "195", "58"}                        # HL + LAP (excluded from freq_installment)
GL_CODES_EXT    = {"191", "243"}                        # GL codes for max_simul_inc_gl

# Custom_var e2_uns (L1927-1928): exact allUnSecProductCodes set used by Java/custom_var
# DIFFERENT from product_mapping Sec_Uns=UnSec: excludes 242,244,245,247,249-252
# Used specifically for freq_between_accts_unsec_wo_cc filtering
_CV_UNS_FREQ = {"5","121","123","130","167","169","170","176","177","178","179",
                "181","187","189","196","197","198","199","200","213","214","215",
                "216","217","224","225","226","227","228","999"}

DEROG_SFW_LIST  = {"05","06","07","08","09","10","11","12","13","14","15","16","17"}
DEROG_WOST_LIST = {"02","03","04","05","06","07","08","09","10","11","12","13","14","15"}
OWNERSHIP_CD    = {"1", "2"}


# ===========================================================================
# PHASE 1 — DATA INGESTION  (fact2 build from separate input files)
# ===========================================================================

def load_inputs(trade_csv, account_mapping_csv, product_mapping_csv, bank_mapping_csv):
    """
    Load all four input CSVs via pandas (Java 25 Hadoop workaround).
    Returns four Spark DataFrames.
    """
    def _read(path, str_cols=None, int_cols=None, dbl_cols=None):
        dtype_map = {c: str for c in (str_cols or [])}
        # Auto-detect Excel vs CSV by extension
        ext = os.path.splitext(str(path))[-1].lower()
        if ext in ('.xlsx', '.xls', '.xlsm'):
            pdf = pd.read_excel(path, dtype=dtype_map)
        else:
            pdf = pd.read_csv(path, dtype=dtype_map, low_memory=False)
        for c in (int_cols or []):
            if c in pdf.columns:
                pdf[c] = pd.to_numeric(pdf[c], errors="coerce").fillna(0).astype("int64")
        for c in (dbl_cols or []):
            if c in pdf.columns:
                pdf[c] = pd.to_numeric(pdf[c], errors="coerce").fillna(0.0).astype("float64")
        return pdf

    # ── trade_data ────────────────────────────────────────────────────────────
    trade_str  = ["account_type_cd", "pay_rating_cd", "account_status_cd",
                  "SUITFILED_WILFULDEFAULT", "SUITFILEDWILLFULDEFAULT",
                  "WRITTEN_OFF_AND_SETTLED", "acct_nb", "resp_code", "closed_dt"]
    trade_int  = ["accno", "bureau_mbr_id", "open_dt",
                  "balance_dt", "dayspastdue", "term_freq"]
    trade_dbl  = ["balance_amt", "credit_lim_amt", "original_loan_amt",
                  "past_due_amt", "actual_pymnt_amt", "written_off_amt",
                  "principal_written_off", "charge_off_amt"]
    trade_pdf  = _read(trade_csv, trade_str, trade_int, trade_dbl)

    # Normalise cons_acct_key as the unique trade identifier (use accno as fallback)
    if "cons_acct_key" not in trade_pdf.columns:
        trade_pdf["cons_acct_key"] = trade_pdf["accno"]
    else:
        pass  # cons_acct_key already present and correct

    # FIX D: Deduplicate trade data on (cons_acct_key, balance_dt).
    # Some datasets have duplicate rows per account per reporting month.
    # Duplicates inflate sum() aggregations (balance, past_due, etc.) by 2x.
    # fmax/fmin/favg are unaffected but fsum is critical for util, outflow, sanction amounts.
    _before = len(trade_pdf)
    trade_pdf = trade_pdf.drop_duplicates(subset=["cons_acct_key", "balance_dt"])
    _after = len(trade_pdf)
    if _before != _after:
        print(f"  [INFO] Deduplicated trade_data: {_before} → {_after} rows (removed {_before-_after} duplicates)")

    # ── account_mapping ───────────────────────────────────────────────────────
    # Real data schema: cust_id + app_date only (no accno, no score_dt, no relFinCd).
    # Derive the missing columns:
    #   score_dt  ← app_date  (the application/scoring date)
    #   accno     ← cust_id   (in this dataset trade.accno == cust_id)
    #   relFinCd  ← "1"       (all rows are primary owners)
    acct_map_pdf = _read(account_mapping_csv,
                         int_cols=["cust_id", "app_date"])
    # Rename app_date → score_dt
    if "app_date" in acct_map_pdf.columns and "score_dt" not in acct_map_pdf.columns:
        acct_map_pdf = acct_map_pdf.rename(columns={"app_date": "score_dt"})
    # Derive accno from cust_id (trade.accno == cust_id in this dataset)
    if "accno" not in acct_map_pdf.columns:
        acct_map_pdf["accno"] = acct_map_pdf["cust_id"]
    # Add relFinCd = "1" (all primary owners in this dataset)
    if "relFinCd" not in acct_map_pdf.columns:
        acct_map_pdf["relFinCd"] = "1"
    # Ensure string types for join keys
    acct_map_pdf["accno"]    = acct_map_pdf["accno"].astype(str)
    acct_map_pdf["cust_id"]  = acct_map_pdf["cust_id"].astype(str)
    acct_map_pdf["score_dt"] = acct_map_pdf["score_dt"].astype(str)

    # ── product_mapping ───────────────────────────────────────────────────────
    # Real data format: all values wrapped in b'' byte-string notation.
    #   account_type_cd: "b'5'"   → strip to "5"
    #   Sec_Uns:         "b'UnSec'" → strip to "UnSec"
    #   Pdt_Cls:         "b'RegUns'" → strip to "RegUns"
    prod_pdf = _read(product_mapping_csv,
                     str_cols=["account_type_cd", "Sec_Uns", "Reg_Com", "Sec_Mov", "Pdt_Cls"])
    # Strip b'' byte-string wrapping from ALL classification columns
    for _c in ["account_type_cd", "Sec_Uns", "Reg_Com", "Sec_Mov", "Pdt_Cls"]:
        if _c in prod_pdf.columns:
            prod_pdf[_c] = (prod_pdf[_c].astype(str)
                            .str.replace(r"^b'|'$", "", regex=True)
                            .str.strip())
    # Normalise account_type_cd: strip leading zeros so "005" matches "5" in trade_data
    prod_pdf["account_type_cd"] = prod_pdf["account_type_cd"].str.lstrip("0").replace("", "0")

    # ── bank_mapping ──────────────────────────────────────────────────────────
    bank_pdf = _read(bank_mapping_csv,
                     str_cols=["Category", "Bank_Name", "LEGACY_CUST_NB"],
                     int_cols=["BUREAU_MBR_ID"])

    # Convert to Spark
    def _pdf_to_spark(pdf):
        """Convert pandas DataFrame to Spark via RDD — avoids Arrow in createDataFrame."""
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
        rows = [tuple(str(v) if v is not None else None for v in r) for r in pdf.itertuples(index=False)]
        fields = [StructField(c, StringType(), True) for c in pdf.columns]
        return spark.createDataFrame(spark.sparkContext.parallelize(rows), StructType(fields))

    trade_df   = _pdf_to_spark(trade_pdf)
    acct_map   = _pdf_to_spark(acct_map_pdf)
    prod_map   = _pdf_to_spark(prod_pdf)
    bank_map   = _pdf_to_spark(bank_pdf)

    print(f"  trade_data     : {trade_pdf.shape[0]:,} rows")
    print(f"  account_mapping: {acct_map_pdf.shape[0]:,} rows")
    print(f"  product_mapping: {prod_pdf.shape[0]:,} rows")
    print(f"  bank_mapping   : {bank_pdf.shape[0]:,} rows")

    return trade_df, acct_map, prod_map, bank_map


def build_fact2(trade_df, acct_map, prod_map, bank_map):
    """
    Joins trade → account_mapping → product_mapping → bank_mapping.
    Applies OWNERSHIP_CD filter (relFinCd IN {1,2}).
    Computes month_diff = (score_dt year*12+month) - (balance_dt year*12+month).
    Keeps month_diff in [0, 35].
    """
    # Filter on ownership
    
    acct_filtered = acct_map.filter(
        col("relFinCd").cast("string").isin(*OWNERSHIP_CD)
    )

    # Join trade to account mapping (accno is the join key)
    df = trade_df.join(
        acct_filtered.select("accno", "cust_id", "score_dt", "relFinCd"),
        on=trade_df["accno"] == acct_filtered["accno"],
        how="inner"
    ).drop(acct_filtered["accno"])

    # month_diff = months between score_dt and balance_dt
    def to_abs_months(date_col):
        d = to_date(date_col.cast("string"), "yyyyMMdd")
        return year(d) * 12 + month(d)

    df = (
        df
        .withColumn("_score_abs",   to_abs_months(col("score_dt")))
        .withColumn("_balance_abs",  to_abs_months(col("balance_dt")))
        .withColumn("month_diff", col("_score_abs") - col("_balance_abs"))
        .drop("_score_abs", "_balance_abs")
        .filter((col("month_diff") >= 0) & (col("month_diff") <= 35))
    )

    # Join product mapping.
    # Normalise both sides: strip leading zeros so "005" in trade matches "5" in prod_map.
    # prod_map account_type_cd was already stripped in load_inputs (pandas side).
    # trade account_type_cd may have leading zeros from bureau data — strip here.
    from pyspark.sql.functions import regexp_replace as _rrep
    prod_sel = broadcast(prod_map.select(
        _rrep(col("account_type_cd").cast("string"), r"^0+", "").alias("_prod_acct_cd"),
        "Sec_Uns", "Reg_Com", "Sec_Mov", "Pdt_Cls"
    ))
    df = df.join(
        prod_sel,
        on=_rrep(df["account_type_cd"].cast("string"), r"^0+", "") == col("_prod_acct_cd"),
        how="left"
    ).drop("_prod_acct_cd")

    # Join bank mapping
    df = df.join(
        broadcast(bank_map.select(
            col("BUREAU_MBR_ID").cast("long").alias("_bk_id"),
            col("Category")
        )),
        on=df["bureau_mbr_id"].cast("long") == col("_bk_id"),
        how="left"
    ).drop("_bk_id")

    return df


# ===========================================================================
# PHASE 2 — PER-MONTH VARIABLE CALCULATION
# ===========================================================================

@udf(IntegerType())
def dpd_new_udf(dayspastdue, pay_rating_cd, suit_filed, wo_settled, balance_amt):
    """Normalised DPD bucket 0-7. Mirrors ScoreCardHelperV4.getDpd_new() exactly.
    
    Java L4283: paddedWOST = leftPad(wOST_String, 3, '0') — '0' → '000', '2' → '002'
    Java L4306: wostCond = paddedWOST == '000'  → dpd=6 branch
    Java L4289: cond4 = getAllWOSStatusCodes().contains(paddedWOST) → dpd=7 branch
                getAllWOSStatusCodes = {002,003,004,006,008,009,013,014,015,016,017}
    
    KEY BUGS FIXED:
    - WO='0': Java treats as '000' (valid code, wostCond=True → dpd=6)
              Old code: _clean('0') returned '' (treated as sentinel) → dpd=0
    - WOS set: Java set is {002,003,004,006,008,009,013,014,015,016,017}
               Old set included 005,007,010,011,012 and missed 016,017
    """
    try: bal = float(balance_amt) if balance_amt is not None else -1.0
    except: bal = -1.0
    try: dpd = int(float(dayspastdue)) if dayspastdue is not None else -1
    except: dpd = -1
    pr = str(pay_rating_cd).strip() if pay_rating_cd is not None else ''

    # Java: only None/empty string and truly missing → skip WO check
    # '0' is a VALID WO code that pads to '000' (wostCond)
    # Sentinels: None, empty, -1, negative, 99/099/999 (not applicable)
    def _parse_wo(val):
        if val is None: return ''
        s = str(val).strip()
        if s.lower() in ('', 'nan', 'none', 'null'): return ''
        try:
            fv = float(s)
            if _math.isnan(fv): return ''
            if fv < 0: return ''          # -1 = not applicable
            # 99/099/999 = sentinel "not applicable"
            if fv in (99, 999): return ''
            # 0 IS a valid code → '000' in Java
            return str(int(fv))           # '0' stays '0', '2' stays '2', etc.
        except: pass
        return s if s not in ('nan','none','null') else ''

    wo_raw = _parse_wo(wo_settled)
    wo = wo_raw.zfill(3) if wo_raw else ''  # Java: leftPad(wOST, 3, '0')

    # Java getAllWOSStatusCodes = {002,003,004,006,008,009,013,014,015,016,017}
    # Note: 005 (MISSING from Java set), 007, 010, 011, 012 also absent; 016, 017 present
    _WOS_CODES = {'002','003','004','006','008','009','013','014','015','016','017'}

    wost_cond = (wo == '000')   # Java L4306: wostCond = paddedWOST == "000"
    cond4     = wo in _WOS_CODES  # Java L4289: getAllWOSStatusCodes().contains()

    if bal <= 500:
        return 0
    # main1 → dpd=7
    if dpd > 720 or (pr == 'L' and dpd == -1) or cond4:
        return 7
    # main2 → dpd=6: cond5 OR (pr in {L,D} AND dpd==-1) OR wostCond
    if (361 <= dpd <= 720) or (pr in {'L','D'} and dpd == -1) or wost_cond:
        return 6
    # main3 → dpd=5
    if (181 <= dpd <= 360) or (pr in {'L','D'} and dpd == -1) or wost_cond:
        return 5
    # main4 → dpd=4
    if (91 <= dpd <= 180) or (pr in {'L','B','D'} and dpd == -1) or wost_cond:
        return 4
    # main5 → dpd=3
    if (61 <= dpd <= 90) or (pr == 'M' and dpd == -1):
        return 3
    # main6 → dpd=2
    if 31 <= dpd <= 60:
        return 2
    # main7 → dpd=1
    if 1 <= dpd <= 30:
        return 1
    # main8 → dpd=0: dpd==0 or pr=='S' and dpd==-1
    return 0


@udf(DoubleType())
def modified_limit_udf(balance_amt, credit_lim_amt, original_loan_amt):
    """Returns max(balance, credit_limit, original_loan).
    Sentinel values: None, -1, '-1', '0' → treated as 0 (not a real limit).
    All values arrive as strings after _pdf_to_spark conversion.
    """
    _SENTINELS = {None, -1, "-1", "-1.0", "0", "0.0"}
    _MAX_LIMIT = 1_000_000_000  # 100 crore cap — filters data errors like 686B rows
    def _to_float(v):
        if v in _SENTINELS: return 0.0
        try:
            fv = float(v)
            if fv < 0: return 0.0
            return min(fv, _MAX_LIMIT)
        except (TypeError, ValueError):
            return 0.0
    return max(_to_float(balance_amt), _to_float(credit_lim_amt), _to_float(original_loan_amt))


@udf(BooleanType())
def derog_flag_udf(suit_filed, wo_settled, pay_rating_cd):
    def _clean(val):
        """Sentinel values 0, -1, 0.0, -1.0 mean 'not set' — return empty string."""
        if val is None: return ""
        try:
            fv = float(val)
            if _math.isnan(fv): return ""
            if fv <= 0: return ""
        except (ValueError, TypeError): pass
        s = str(val).strip()
        return "" if s.lower() in ("", "nan", "none", "null", "0", "-1", "0.0", "-1.0") else s

    sfw_raw = _clean(suit_filed)
    wo_raw  = _clean(wo_settled)
    pr      = str(pay_rating_cd).strip() if pay_rating_cd is not None else ""

    sfw = sfw_raw.zfill(3) if sfw_raw else ""
    wo  = wo_raw.zfill(3)  if wo_raw  else ""

    if sfw and sfw in {"005","006","007","008","009","010","011","012",
                       "013","014","015","016","017"}:
        return True
    if wo and wo in {"002","003","004","005","006","007","008","009",
                     "010","011","012","013","014","015"}:
        return True
    # GENERIC FIX: '000' means "write-off code 0" (not set), not a derog event
    # Only trigger on non-zero WO codes. '001' onward = real write-off.
    if wo and wo[:2] in {"01"}:  # removed "00" — "000" = not set, should not derog
        return True
    if sfw_raw == "200":
        return True
    if pr in {"D","L"}:
        return True
    return False


def add_product_flags(df):
    cd = col("account_type_cd").cast("string")
    return (
        df
        .withColumn("isCC",       cd.isin(*CC_CODES))
        .withColumn("isCL",       cd.isin(*CL_CODES))
        .withColumn("isHL",       cd.isin(*HL_CODES))
        .withColumn("isAL",       cd.isin(*AL_CODES))
        .withColumn("isTW",       cd.isin(*TWO_W_CODES))
        .withColumn("isGL",       cd.isin(*GL_CODES))
        .withColumn("isGLExt",    cd.isin(*GL_CODES_EXT))      # GL for simul_inc_gl
        .withColumn("isPL",       cd.isin(*PL_CODES))
        .withColumn("isAgri",     cd.isin(*AGRI_CODES))
        .withColumn("isComSec",   cd.isin(*COM_SEC_CODES))
        .withColumn("isComUnSec", cd.isin(*COM_UNSEC_CODES))
        .withColumn("isRegular",  cd.isin(*REGULAR_CODES))
        .withColumn("isPlCdTw",   cd.isin(*PL_CD_TW_CODES))    # PL+CD+TW
        .withColumn("isLapHl",    cd.isin(*LAP_HL_CODES))      # for freq_installment exclusion
        # ── Sec_Uns / Sec_Mov / Pdt_Cls / Reg_Com flags ──────────────────────
        # Real product_mapping values (after b'' strip):
        #   Sec_Uns : "UnSec" / "Sec"
        #   Sec_Mov : "SecMov" / "Rest"
        #   Pdt_Cls : "RegUns" / "RegSec" / "ComUns" / "ComSec" / "Nth" / "Govt"
        #   Reg_Com : "Regular" / "NonAgriPSL" / "AgriPSL" / "Other"
        .withColumn("isUns",      col("Sec_Uns") == "UnSec")
        .withColumn("isSec",      col("Sec_Uns") == "Sec")
        .withColumn("isSecMov",   col("Sec_Mov") == "SecMov")
        .withColumn("isRegSec",   col("Pdt_Cls") == "RegSec")
        .withColumn("isRegUns",   col("Pdt_Cls") == "RegUns")
        .withColumn("isComCls",   col("Reg_Com").isin("NonAgriPSL", "AgriPSL"))
        .withColumn("isPSB",      col("Category") == "PUB")   # bank_mapping uses "PUB" (not "PSB") for public sector banks
        .withColumn("isPVT",      col("Category") == "PVT")
        .withColumn("isNBFC",     col("Category").isin("NBF","NBFC"))
        .withColumn("isSFB",      col("Category") == "SFB")
        # SecMov + RegSec = secured movable (for outflow_uns_secmov)
        # SecMov + RegSec combination (for outflow_uns_secmov)
        # Real values: Sec_Mov="SecMov", Pdt_Cls="RegSec"
        .withColumn("isSecMovRegSec",
                    (col("Sec_Mov") == "SecMov") & (col("Pdt_Cls") == "RegSec"))
    )

# DataFrame[accno: string, cons_acct_key: string, open_dt: string, acct_nb: string, closed_dt: string, bureau_mbr_id: string, account_type_cd: string, term_freq: string, charge_off_amt: string, resp_code: string, balance_dt: string, account_status_cd: string, actual_pymnt_amt: string, balance_amt: string, credit_lim_amt: string, original_loan_amt: string, past_due_amt: string, pay_rating_cd: string, dayspastdue: string, written_off_amt: string, principal_written_off: string, SUITFILED_WILFULDEFAULT: string, SUITFILEDWILLFULDEFAULT: string, WRITTEN_OFF_AND_SETTLED: string, cust_id: string, score_dt: string, relFinCd: string, month_diff: int, Sec_Uns: string, Reg_Com: string, Sec_Mov: string, Pdt_Cls: string, Category: string]

def build_fact2_enriched(fact2):
    df = fact2

    df = (
        df
        .withColumn("balance_amt",       col("balance_amt").cast("double"))
        .withColumn("credit_lim_amt",    col("credit_lim_amt").cast("double"))
        .withColumn("original_loan_amt", col("original_loan_amt").cast("double"))
        .withColumn("dayspastdue",       col("dayspastdue").cast("int"))
        .withColumn("actual_pymnt_amt",  col("actual_pymnt_amt").cast("double"))
        .withColumn("past_due_amt",      col("past_due_amt").cast("double"))
        .withColumn("account_type_cd",   col("account_type_cd").cast("string"))
    )
    df = (
        df
        .withColumn("dpd_new",
            dpd_new_udf(
                col("dayspastdue"), col("pay_rating_cd"),
                col("SUITFILED_WILFULDEFAULT").cast("string"),
                col("WRITTEN_OFF_AND_SETTLED").cast("string"),
                col("balance_amt")))
        .withColumn("modified_limit",
            modified_limit_udf(
                col("balance_amt"), col("credit_lim_amt"), col("original_loan_amt")))
        .withColumn("derog_flag",
            derog_flag_udf(
                col("SUITFILED_WILFULDEFAULT").cast("string"),
                col("WRITTEN_OFF_AND_SETTLED").cast("string"),
                col("pay_rating_cd")))
        # closed_dt in this dataset is NULL for open accounts (not integer 0).
        # All values are strings after pandas→Spark conversion.
        # Sentinel values: NULL, "", "0", "0.0", "." → account is live.
        # Real close date is a yyyyMMdd string — compare lexicographically with score_dt.
        .withColumn("is_live_month0",
            (col("month_diff") == 0) & (
                (F.trim(col("account_status_cd")).substr(1, 1) == "O") &
                ~(col("closed_dt").isNotNull() &
                  ~col("closed_dt").isin("", "0", "0.0", ".", "nan", "NaN", "None") &
                  (col("closed_dt") < col("score_dt"))))  # FIX 1
        )
        .withColumn("is_not_closed",
            # FIX 1: account_status_cd primary; real past closed_dt overrides
            ~(col("closed_dt").isNotNull() &
              ~col("closed_dt").isin("", "0", "0.0", ".", "nan", "NaN", "None") &
              (col("closed_dt") < col("score_dt"))) &
            (F.trim(col("account_status_cd")).substr(1, 1) == "O"))
    )
    df = add_product_flags(df)
    return df


# ===========================================================================
# PHASE 3 — PER-ACCOUNT AGGREGATION
# ===========================================================================

def build_account_details(fact2_enriched):
    df = fact2_enriched

    # BUG-FIX: group by cons_acct_key so each account is counted separately
    # Previously groupBy accno caused all accounts per customer to collapse into 1
    acct_agg = (
        df.groupBy("cust_id", "cons_acct_key").agg(
            first("accno").alias("accno"),
            first("account_type_cd").alias("account_type_cd"),
            fmax("open_dt").alias("open_dt"),  # Keep fmax for MOB calc. freq_between uses separate fmin per account.
            first("closed_dt").alias("closed_dt"),
            first("score_dt").alias("score_dt"),
            first("bureau_mbr_id").alias("bureau_mbr_id"),
            # ROOT CAUSE FIX: Java bru.py L3100-3104 uses account_type_cd from the
            # MOST RECENT month (min month_diff). fmax(isXxx) picks True if ANY month
            # had that type — wrong when type changes. Replaced with first() ordered
            # by month_diff ASC, applied after groupBy. See _min_idx_acd join below.
            first("isCC").alias("isCC"),
            first("isHL").alias("isHL"),
            first("isAL").alias("isAL"),
            first("isTW").alias("isTW"),
            first("isGL").alias("isGL"),
            first("isGLExt").alias("isGLExt"),
            first("isPL").alias("isPL"),
            first("isAgri").alias("isAgri"),
            first("isComSec").alias("isComSec"),
            first("isComUnSec").alias("isComUnSec"),
            first("isRegular").alias("isRegular"),
            first("isPlCdTw").alias("isPlCdTw"),
            first("isLapHl").alias("isLapHl"),
            first("isUns").alias("isUns"),
            first("isSec").alias("isSec"),
            first("isSecMov").alias("isSecMov"),
            first("isSecMovRegSec").alias("isSecMovRegSec"),
            first("isRegSec").alias("isRegSec"),
            first("isRegUns").alias("isRegUns"),
            first("isComCls").alias("isComCls"),
            first("isPSB").alias("isPSB"),
            first("isPVT").alias("isPVT"),
            first("isNBFC").alias("isNBFC"),
            first("isSFB").alias("isSFB"),
            first("Category").alias("bank_category"),
            first("Pdt_Cls").alias("Pdt_Cls"),
            first("Sec_Uns").alias("Sec_Uns"),
            first("Reg_Com").alias("Reg_Com"),
            fmax("isCL").alias("isCL"),
            first("Sec_Mov").alias("Sec_Mov"),
            fmax(when(col("month_diff").isNotNull(), col("modified_limit"))).alias("latest_modified_limit"),  # FIX 2: Java uses max limit across all months (updateAccountLevelDetails keeps running max)
            # B1 FIX: account is live if closed_dt is null/missing OR
            # any row within 6m has account_status_cd == 'O' (not closed)
            # Primary: use closed_dt if available; fallback to status rows
            # String-safe: closed_dt is NULL for open accounts in this dataset.
            # Also handle sentinels "", "0", "0.0", "." and future-date close dates.
            # FIX 1: account_status_cd is the primary liveness signal.
            # closed_dt=NaN becomes string "nan" in Spark. "nan" > "20250930" is True
            # lexicographically, so it was treated as not-closed. Fixed by checking
            # account_status_cd directly and treating any real past closed_dt as closed.
            # FIX: isLiveAccount must be False if ANY row has a real past closed_dt.
            # The old fmax() approach failed when a closed row (False) coexisted with an
            # older O-status row (True) — fmax picked True (WRONG).
            # Fix: use two separate aggregations:
            #   has_close = True if any row has a real past closed_dt
            #   has_open  = True if any row has account_status_cd starting with 'O'
            # isLiveAccount = has_open AND NOT has_close
            fmax(
                when(
                    col("closed_dt").isNotNull() &
                    ~col("closed_dt").isin("", "0", "0.0", ".", "nan", "NaN", "None") &
                    (col("closed_dt") < col("score_dt")),
                    lit(True))
                .otherwise(lit(False))
            ).alias("_has_real_close"),
            fmax(
                when(F.trim(col("account_status_cd")).substr(1, 1) == "O", lit(True))
                .otherwise(lit(False))
            ).alias("_has_open_status"),
            fmax(when(col("month_diff") <= 11,
                      col("is_live_month0"))).alias("isReportedLiveIn12M"),
            # FIX: org_car L874: incl_flag_36 = app_diff_past <= 36 where app_diff_past > 0
            # Excludes idx=0 (current score month). Matches open_cnt formula.
            fmax(when((col("month_diff") >= 1) & (col("month_diff") <= 35),
                      lit(True))).alias("reportedIn36M"),
            fmax(when(col("month_diff") <= 11, lit(True))).alias("reportedIn12M"),
            fmax(col("derog_flag").cast("int")).cast("boolean").alias("derog"),
            collect_list(struct(
                col("month_diff").alias("idx"),
                col("dpd_new").alias("dpd"),
                col("dayspastdue").cast("int").alias("raw_dpd"),
                col("balance_amt").alias("bal"),
                col("modified_limit").alias("mod_lim"),
                col("pay_rating_cd").alias("asset_cd"),
                col("WRITTEN_OFF_AND_SETTLED").cast("string").alias("wo_status"),
                col("SUITFILED_WILFULDEFAULT").cast("string").alias("sfw_status"),
                col("actual_pymnt_amt").alias("pymnt_amt"),
                col("account_status_cd").alias("acct_status"),
                col("past_due_amt").alias("past_due"),
                col("derog_flag").alias("derog_flag"),
                col("is_not_closed").alias("is_not_closed"),
            )).alias("monthly_data"),
            fmax(col("dpd_new")).alias("max_dpd_l36m"),
            fmax(when(col("month_diff") <= 11, col("dpd_new"))).alias("max_dpd_l12m"),
            fmax(when(col("month_diff") == 0,  col("dpd_new"))).alias("dpd_m0"),
            F.try_divide(
                fsum(when((col("month_diff") <= 11) & (col("balance_amt") > 0),
                          lit(1)).otherwise(lit(0))) * 100.0,
                count(when(col("month_diff") <= 11, lit(1)))
            ).alias("pct_instance_l12m"),
            favg(when(col("month_diff") <= 2, col("balance_amt"))).alias("avgBal_l3m"),
            favg(when(col("month_diff") <= 2, col("modified_limit"))).alias("avgModifiedLimit_l3m"),
            fmax(when(col("month_diff") == 0, col("balance_amt"))).alias("bal_m0"),
            fmin("month_diff").alias("latestReportedIndex"),
            fmax(when(col("month_diff") <= 23, col("balance_amt"))).alias("max_bal_l24m"),
            fmax(when(col("month_diff") <= 11, col("balance_amt"))).alias("max_bal_l12m"),
        )
    )

    # ── Overwrite product flags with values from MOST RECENT month (min month_diff) ──
    # Java bru.py L3100-3104: updateRecentAccountDetails is called only when index < minIndex
    # → account flags (isAgri, isComSec etc.) come from the LOWEST index row (most recent month).
    # Spark's groupBy first() picks an arbitrary row. Fix: compute a per-account window
    # ordered by month_diff ASC, then pick row_number==1 (most recent month) for each flag.
    _flag_cols = ["isCC","isHL","isAL","isTW","isGL","isGLExt","isPL","isAgri",
                  "isComSec","isComUnSec","isRegular","isPlCdTw","isLapHl",
                  "isUns","isSec","isSecMov","isSecMovRegSec","isRegSec","isRegUns",
                  "isComCls","isPSB","isPVT","isNBFC","isSFB","isCL"]
    _w_recent = Window.partitionBy("cust_id","cons_acct_key").orderBy(col("month_diff").asc())
    _recent_flags = (
        df.withColumn("_rn", row_number().over(_w_recent))
        .filter(col("_rn") == 1)
        # FIX 3: include account_type_cd from most-recent month so nbr_pl_le50 filter
        # uses the current type (matches Java updateRecentAccountDetails L2785-2790)
        .select("cust_id","cons_acct_key",
                col("account_type_cd").alias("_rf_account_type_cd"),
                *[col(c).alias(f"_rf_{c}") for c in _flag_cols])
    )
    acct_agg = acct_agg.join(_recent_flags, ["cust_id","cons_acct_key"], "left")
    for c in _flag_cols:
        acct_agg = acct_agg.withColumn(c, col(f"_rf_{c}")).drop(f"_rf_{c}")
    # Also overwrite account_type_cd with value from most-recent month (FIX 3)
    acct_agg = acct_agg.withColumn("account_type_cd", col("_rf_account_type_cd")).drop("_rf_account_type_cd")

    # Derive isLiveAccount: open status AND no real past closed_dt
    acct_agg = acct_agg.withColumn(
        "isLiveAccount",
        col("_has_open_status") & ~col("_has_real_close")
    ).drop("_has_real_close", "_has_open_status")

    # Months-on-Book
    def yyyymmdd_to_abs_months(dt_col):
        d = to_date(dt_col.cast("string"), "yyyyMMdd")
        return year(d) * 12 + month(d)

    acct_agg = (
        acct_agg
        .withColumn("_score_abs", yyyymmdd_to_abs_months(col("score_dt")))
        .withColumn("_open_abs",  yyyymmdd_to_abs_months(col("open_dt")))
        .withColumn("mob", col("_score_abs") - col("_open_abs"))  # no +1 offset: matches ground truth
        .drop("_score_abs")  # keep _open_abs for freq_between calculations
    )

    # firstReportedOnlyIn36M (reported in 36m but NOT in 12m)
    in_12m = (
        fact2_enriched.filter(col("month_diff") <= 12)
        .groupBy("cust_id", "cons_acct_key")
        .agg(lit(True).alias("_in_12m"))
    )
    acct_agg = (
        acct_agg.join(in_12m, ["cust_id", "cons_acct_key"], "left")
        .withColumn("firstReportedOnlyIn36M",
                    col("_in_12m").isNull() & col("reportedIn36M"))
        .drop("_in_12m")
    )

    # m_since_max_bal_l24m
    # Fix 1: Java L2074: .min(Integer::compareTo) = MINIMUM key = MOST RECENT month with max balance.
    # Our old code said "GT always picks OLDEST (fmax)" — that was wrong.
    # Java: for each non-derog UNS account, find the month with max balance in 0..23,
    # when tied take the MINIMUM month_diff (most recent). Then return minKey + 1.
    # Java L2070: filter(entry -> entry.getValue() >= 0) — only non-negative balances
    _max_bal_per_acct = (
        fact2_enriched.filter((col("month_diff") <= 23) & (col("balance_amt") >= 0))
        .groupBy("cust_id", "cons_acct_key")
        .agg(fmax("balance_amt").alias("_peak_bal"))
    )
    max_bal_idx = (
        fact2_enriched.filter((col("month_diff") <= 23) & (col("balance_amt") >= 0))
        .join(_max_bal_per_acct, ["cust_id", "cons_acct_key"], "inner")
        .filter(col("balance_amt") == col("_peak_bal"))
        .groupBy("cust_id", "cons_acct_key")
        .agg(fmin("month_diff").alias("m_since_max_bal_l24m"))  # MOST RECENT month (min idx), matches Java
    )
    acct_agg = acct_agg.join(max_bal_idx, ["cust_id", "cons_acct_key"], "left")

    # Worst delinquency history
    worst_delq_df = (
        fact2_enriched
        .withColumn("_max_dpd_acct",
                    fmax("dpd_new").over(Window.partitionBy("cust_id", "cons_acct_key")))
        .filter(col("dpd_new") == col("_max_dpd_acct"))
        .filter(col("dpd_new") > 0)
        .groupBy("cust_id", "cons_acct_key").agg(
            fmax("month_diff").alias("mon_since_first_worst_delq"),
            fmin("month_diff").alias("mon_since_recent_worst_delq"),
        )
    )
    acct_agg = acct_agg.join(worst_delq_df, ["cust_id", "cons_acct_key"], "left")

    return acct_agg


# ===========================================================================
# PHASE 4 — GLOBAL ATTRIBUTE CALCULATION
# ===========================================================================



# ════════════════════════════════════════════════════════════════════════════
#                                                                            ░
#   PHASE 4 HELPER FUNCTIONS — Business-Domain Modules                       ░
#                                                                            ░
#   These functions decompose build_global_attrs into independently testable ░
#   units, each computing one business metric or related family of metrics.  ░
#                                                                            ░
#   Execution order (called by build_global_attrs in this sequence):         ░
#     1.  prepare_monthly_exploded            — shared base for all metrics  ░
#     2.  count_accounts_by_product_type      — 4A: account inventory        ░
#     3.  count_accounts_opened_by_window     — 4B: open-month windows       ░
#     4.  derive_max_dpd_attributes           — 4C: delinquency severity     ░
#     5.  calculate_max_sanction_secured      — secured loan max sanction    ░
#     6.  build_per_account_utilization_bases — per-account util building    ░
#     7.  calculate_credit_utilization_unsecured                             ░
#     8.  calculate_credit_utilization_all                                   ░
#     9.  calculate_credit_utilization_excluding_cc                          ░
#    10.  calculate_credit_utilization_cc_only                               ░
#                                                                            ░
#   All other business modules (cashflow, trends, simultaneity, balance     ░
#   ratios, frequency, recency, CC-40 metrics, etc.) currently remain        ░
#   inline within build_global_attrs and can be extracted incrementally      ░
#   following the same pattern shown here.                                   ░
#                                                                            ░
# ════════════════════════════════════════════════════════════════════════════


def prepare_monthly_exploded(ad):
    """Explode account_details.monthly_data into per-(account, month) rows.

    BUSINESS LOGIC:
        Each account has up to 36 monthly snapshots stored as a struct array.
        Many downstream metrics are computed per-month (DPD trends, balance
        trends, utilization windows). This function flattens the nested
        structure into a wide table indexed by (cust_id, account, month_idx).

    Output columns:
        cust_id, accno, _cons_key, account_type_cd, product flags
        (isCC..isSFB), idx (0=current, 35=oldest), dpd, raw_dpd, bal,
        mod_lim, asset_cd, wo_status, sfw_status, acct_status, past_due,
        row_derog (per-month derog), is_not_closed.
    """
    exploded = (
        ad.select(
            "cust_id", "accno",
            col("_acct_id").alias("_cons_key"),   # ← unique account key baked in
            "account_type_cd",
            "isCC", "isHL", "isAL", "isTW", "isGL", "isGLExt", "isPL", "isCL",
            "isUns", "isSec", "isSecMov", "isSecMovRegSec", "isAgri",
            "isComSec", "isComUnSec", "isRegSec", "isRegUns", "isRegular",
            "isPlCdTw", "isLapHl", "isPSB", "isPVT", "isNBFC", "isSFB",
            F.explode("monthly_data").alias("m")
        )
        .select(
            "cust_id", "accno", "_cons_key",
            "account_type_cd",
            "isCC", "isHL", "isAL", "isTW", "isGL", "isGLExt", "isPL", "isCL",
            "isUns", "isSec", "isSecMov", "isSecMovRegSec", "isAgri",
            "isComSec", "isComUnSec", "isRegSec", "isRegUns", "isRegular",
            "isPlCdTw", "isLapHl", "isPSB", "isPVT", "isNBFC", "isSFB",
            col("m.idx").alias("idx"),
            col("m.dpd").alias("dpd"),
            col("m.raw_dpd").alias("raw_dpd"),   # raw dayspastdue (before balance<=500 zeroing)
            col("m.bal").alias("bal"),
            col("m.mod_lim").alias("mod_lim"),
            col("m.asset_cd").alias("asset_cd"),
            col("m.wo_status").alias("wo_status"),
            col("m.sfw_status").alias("sfw_status"),
            col("m.acct_status").alias("acct_status"),
            col("m.past_due").alias("past_due"),
            col("m.derog_flag").alias("row_derog"),
            col("m.is_not_closed").alias("is_not_closed"),
        )
    )
    exploded.cache()
    return exploded


def count_accounts_by_product_type(exploded, ad):
    """4A — Account inventory counts by product category and reporting window.

    BUSINESS LOGIC:
        Counts accounts per customer by product category, split by:
          - 36-month reporting window (ever-reported)
          - "live" accounts (currently active)
          - first 6 months of MOB

        Categories: total, secured, unsecured, comsec, comunsec, regsec,
        reguns, agri, regular, CC, HL, AL, TW, GL, GLext, PL, CL,
        plcd_tw, lap_hl. Counts are NaN-preserving — returns NaN when
        NO accounts of a type exist (matches Java/org_car grp_nansum).

    Returns: DataFrame keyed on cust_id with all account-count attributes.
    """
    # ── 4A. Account counts ───────────────────────────────────────────────────
    # BUG-FIX: count("cons_acct_key") to count distinct accounts (each has unique cons_acct_key)
    acct_counts = (
        ad.groupBy("cust_id").agg(
            count("_acct_id").alias("nbr_tot_accts_36"),
            fsum(col("isLiveAccount").cast("int")).alias("nbr_live_accts_36"),
            fsum(col("reportedIn12M").cast("int")).alias("nbr_tot_accts_12"),
            fsum((col("reportedIn12M") & col("isLiveAccount")).cast("int")).alias("nbr_live_accts_12"),
            # Java CC codes include 220 (RegSec Regular) for count variables
            fsum(when((col("isCC") | (col("account_type_cd")=="220")) & col("reportedIn36M"), lit(1))).alias("nbr_cc_tot_accts_36"),
            # Java CC codes include 220 for live count too
            fsum(when((col("isCC") | (col("account_type_cd")=="220")) & col("isLiveAccount"), lit(1))).alias("nbr_cc_live_accts_36"),
            fsum(when(col("isHL")     & col("reportedIn36M"), lit(1))).alias("nbr_hl_tot_accts_36"),
            fsum(when(col("isHL")     & col("isLiveAccount"), lit(1))).alias("nbr_hl_live_accts_36"),
            fsum(when(col("isAL")     & col("reportedIn36M"), lit(1))).alias("nbr_al_tot_accts_36"),
            fsum(when(col("isAL")     & col("isLiveAccount"), lit(1))).alias("nbr_al_live_accts_36"),
            # NaN-preserving counts: returns NaN when NO accounts of this type exist (matches org_car grp_nansum)
            fsum(when(col("isGL")     & col("reportedIn36M"), lit(1))).alias("nbr_gl_tot_accts_36"),
            fsum(when(col("isGL")     & col("isLiveAccount"), lit(1))).alias("nbr_gl_live_accts_36"),
            fsum(when(col("isPL")     & col("reportedIn36M"), lit(1))).alias("nbr_pl_tot_accts_36"),
            fsum(when(col("isPL")     & col("isLiveAccount"), lit(1))).alias("nbr_pl_live_accts_36"),
            fsum(when(col("isAgri")   & col("reportedIn36M"), lit(1))).alias("nbr_agri_tot_accts_36"),
            fsum(when(col("isAgri")   & col("isLiveAccount"), lit(1))).alias("nbr_agri_live_accts_36"),
            fsum(when(col("isComSec") & col("reportedIn36M"), lit(1))).alias("nbr_comsec_tot_accts_36"),
            fsum(when(col("isComSec") & col("isLiveAccount"), lit(1))).alias("nbr_comsec_live_accts_36"),
            fsum(when(col("isComUnSec") & col("reportedIn36M"), lit(1))).alias("nbr_comuns_tot_accts_36"),
            fsum(when(col("isUns") & col("reportedIn36M") & ~col("isCC"), lit(1))).alias("nbr_uns_wo_cc_tot_accts_36"),
            fsum(when(col("isUns") & col("isLiveAccount") & ~col("isCC"), lit(1))).alias("nbr_uns_wo_cc_live_accts_36"),
            fsum(col("derog").cast("int")).alias("nbr_derog_accts"),
            fmax("mob").alias("max_mob_all_36"),
            fmin("mob").alias("min_mob_all_36"),
            # avg_mob_reg_36: exclude LAP (type 195) accounts.
            # After mapping fix: type 195 is now isComSec=True, isPL=False.
            # Old exclusion ~(isPL & isSec) no longer fires for type 195.
            # Fix: use isLapHl which directly covers type 195 (LAP_HL_CODES = {58,195}).
            # Type 58 (HL) is also in isLapHl — but HL should stay in avg_mob.
            # So: exclude only type 195 specifically via account_type_cd.
            favg(when(col("isRegular") & (col("account_type_cd").cast("string") != "195"), col("mob"))).alias("avg_mob_reg_36"),
            fmax(when(col("isCC"), col("mob"))).alias("max_mob_cc"),
            fmin(when(~col("isCC"), col("mob"))).alias("min_mob_wo_cc"),
            fsum((~(col("isCC") | col("isHL") | col("isAL"))).cast("int")).alias("nbr_not_al_cc_hl"),
            fsum(when(col("isTW")  & col("reportedIn36M"), lit(1))).alias("nbr_tw_tot_accts_36"),
            fsum(when(col("isTW")  & col("isLiveAccount"), lit(1))).alias("nbr_tw_live_accts_36"),
            fsum(when(col("isCL") & col("reportedIn36M"), lit(1))).alias("nbr_cl_tot_accts_36"),
            # fsum((col("account_type_cd").isin("240","242","244","245","246","247","248","249") &
            #       col("reportedIn36M")).cast("int")).alias("nbr_cl_tot_accts_36"),
            fmin(when(col("isAgri")     & col("isLiveAccount"), col("mob"))).alias("min_mob_agri_live_36"),
            fmax(when(col("isComUnSec") & col("isLiveAccount"), col("mob"))).alias("max_mob_comuns_live_36"),
            fmin(when(col("isUns") & ~col("isCC"), col("mob"))).alias("min_mob_uns_wo_cc_36"),
            fmax(when(col("isCC"), col("mob"))).alias("max_mob_cc_36"),
        )
    )
    return acct_counts


def count_accounts_opened_by_window(exploded, ad):
    """4B — Accounts opened by month-of-business (MOB) window.

    BUSINESS LOGIC:
        Counts MONTHLY RECORDS where reporting month equals open month
        (idx==mob) AND modified_limit > 0. Used to compute the
        openCnt0_6By7_12Bin attribute family — measures concentration
        of recent (last 6m) vs older (7-12m) account opening activity.

        Java reference: bru.py L1800-1812 (calcOpenCntBin).

    Returns: DataFrame keyed on cust_id with opened-window count metrics.
    """
    # ── 4B. Accounts opened by mob window ────────────────────────────────────
    # _open_cnt sub-DF: Java openCnt0_6By7_12Bin (bru.py L1800-1812)
    # counts MONTHLY RECORDS where diff(acctDt, openDate)==0 AND modifiedLimit>0
    # diff==0 ↔ reporting month == open month ↔ idx==mob (for that specific row)
    # Use exploded (row-level) not account_details (account-level).
    _open_cnt_df = (
        exploded
        .filter((col("idx") == col("mob")) & (col("mod_lim") > 0))
        .groupBy("cust_id").agg(
            fsum(when(col("idx").between(0, 6),  lit(1))).alias("_open_cnt_l6m"),
            fsum(when(col("idx").between(7, 12), lit(1))).alias("_open_cnt_7to12m"),
        )
    )

    accts_opened = (
        ad.groupBy("cust_id").agg(
            fsum(when((col("mob") <= 3) & (col("mob") > 0), lit(1))).alias("nbr_accts_open_l3m"),
            # Java: monthDiff > 0 && < 6 (strictly less than 6 = mob 1..5)
            fsum(when((col("mob") > 0) & (col("mob") < 6), lit(1))).alias("nbr_accts_open_l6m"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0), lit(1))).alias("nbr_accts_open_l12m"),
            fsum(when((col("mob") <= 16) & (col("mob") > 0), lit(1))).alias("nbr_accts_open_l16m"),
            fsum(when((col("mob") <= 24) & (col("mob") > 0), lit(1))).alias("nbr_accts_open_l24m"),
            fsum(when((col("mob") >= 4) & (col("mob") <= 6), lit(1))).alias("nbr_accts_open_4to6m"),
            # Java: monthDiff >= 6 && monthDiff < 12 = mob 6..11
            fsum(when((col("mob") >= 6) & (col("mob") < 12), lit(1))).alias("nbr_accts_open_7to12m"),
            fsum(when((col("mob") >= 13) & (col("mob") <= 24), lit(1))).alias("nbr_accts_open_13to24m"),
            fsum(when((col("mob").between(0, 6)) & (col("latest_modified_limit") > 0), lit(1))).alias("_open_cnt_l6m_old"),
            fsum(when((col("mob").between(7, 12)) & (col("latest_modified_limit") > 0), lit(1))).alias("_open_cnt_7to12m_old"),
            fsum(when((col("mob") <= 6) & (col("mob") > 0) & ~col("isCC"), lit(1))).alias("nbr_accts_open_l6m_wo_cc"),
            # Java: monthDiff > 0 && < 12 (strictly less than 12 = mob 1..11)
            fsum(when((col("mob") > 0) & (col("mob") < 12) & ~col("isCC"), lit(1))).alias("nbr_accts_open_l12m_wo_cc"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0) & col("isCC"), lit(1))).alias("nbr_accts_open_l12m_cc"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0) & col("isHL"), lit(1))).alias("nbr_accts_open_l12m_hl"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0) & col("isAL"), lit(1))).alias("nbr_accts_open_l12m_al"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0) & col("isAgri"), lit(1))).alias("nbr_accts_open_l12m_agri"),
            fmin("mob").alias("mon_since_last_acct_open"),
        )
    )
    return _open_cnt_df, accts_opened


def derive_max_dpd_attributes(exploded, ad):
    """4C — Maximum Days-Past-Due (DPD) metrics across multiple time windows.

    BUSINESS LOGIC:
        DPD measures payment delinquency severity. Computes:
          - max_dpd_uns_l36m / l12m         — worst unsecured DPD ever / 12m
          - max_dpd_UNS_L6_M / L6_12_M      — worst unsec DPD in 6m / 6-12m
          - max_dpd_uns_m0                  — current-month unsec DPD
          - max_dpd_*_l3m / l6m / l12m_*    — same family for various subsets
          - min_mon_sin_recent_*            — months since last DPD>X event

        max_dpd_UNS_L6_M / L6_12_M use NaN-preserving formulation: returns
        NaN when no unsecured rows exist in the window (Java distinguishes
        "no data" from "data but no DPD = 0").

    Returns: DataFrame keyed on cust_id with all DPD attributes.
    """
    dpd_attrs = (
        exploded.groupBy("cust_id").agg(
            fmax(col("dpd")).alias("max_dpd_all_l36m"),
            # Use effective_dpd to catch DPD events on low-balance accounts
            # Java max_dpd_L30M uses dpd_new directly (NOT _effective_dpd) — confirmed 5/6 customers fixed
            fmax(when(col("idx") <= 30, col("dpd"))).alias("max_dpd_l30m"),
            # NaN-preserving for unsecured: returns NaN when no unsec accounts at all
            # Java uses dpd_new directly for UNS max DPD
            fmax(when(col("isUns") & (col("dpd") > 0), col("dpd"))).alias("max_dpd_uns_l36m"),
            fmax(when(col("isUns") & (col("idx") <= 11) & (col("dpd") > 0), col("dpd"))).alias("max_dpd_uns_l12m"),
            # FIX UNS DPD: Apply effective_dpd so accounts with balance<=500 (dpd_new=0)
            # but real dayspastdue>0 are captured. This was the root cause of 67+60 Code=NaN.
            # Java uses dpd_new directly (no effective_dpd) — confirmed by mismatch analysis
            # Java: index < 6 → idx 0..5 (not 0..6)
            # Fix: Java L1518: accountIndex <= 6 → idx 0..6 (not 0..5!)
            # FIX: NaN-preserving. Returns NULL when customer has NO UnSec in idx 0..6
            # (Java: NaN). Returns 0 when UnSec exists but no DPD>0. Returns n otherwise.
            fmax(when(col("isUns") & (col("idx") <= 6),
                     when(col("dpd") > 0, col("dpd")).otherwise(lit(0)))).alias("max_dpd_UNS_L6_M"),
            # Fix: Java L1513: accountIndex > 6 AND accountIndex <= 12 → idx 7..12 (not 6..11!)
            # FIX: NaN-preserving for no UnSec in idx 7..12
            fmax(when(col("isUns") & (col("idx") > 6) & (col("idx") <= 12),
                     when(col("dpd") > 0, col("dpd")).otherwise(lit(0)))).alias("max_dpd_UNS_6_12_M"),
            fmax(when(col("isUns") & (col("idx") == 0) & (col("dpd") > 0), col("dpd"))).alias("max_dpd_uns_m0"),
            fmax(when(col("isSec"), col("dpd"))).alias("max_dpd_sec_l36m"),
            fmax(when(col("isHL"),  col("dpd"))).alias("max_dpd_hl_l36m"),
            fmax(when(col("isCC"),  col("dpd"))).alias("max_dpd_cc_l36m"),
            fmax(when(col("isAL"),  col("dpd"))).alias("max_dpd_al_l36m"),
            fmax(when(col("isPL"),  col("dpd"))).alias("max_dpd_pl_l36m"),
            # DPD counts
            fsum(when((col("dpd") > 0) & (col("idx") <= 23), lit(1)).otherwise(lit(0))).alias("nbr_0_24m_all"),
            fsum(when((col("dpd") > 1) & (col("idx") <= 23), lit(1)).otherwise(lit(0))).alias("nbr_30_24m_all"),
            fsum(when((col("dpd") > 2) & (col("idx") <= 23), lit(1)).otherwise(lit(0))).alias("nbr_60_24m_all"),
            fsum(when((col("dpd") > 3) & (col("idx") <= 23), lit(1)).otherwise(lit(0))).alias("nbr_90_24m_all"),
            fsum(col("row_derog").cast("int")).alias("nbr_derog_months"),
            # Months since any 1+ DPD
            # GENERIC FIX: org_car mon_since_0 uses dpd_new >= 30 raw value (bucket ≥ 2)
            # bucket 1 = 1-30 DPD (dpd_new=29), bucket 2 = 31-60 DPD (dpd_new=30)
            # min_mon_sin_recent_1 for binning: still uses any DPD (bucket≥1)
            # mon_sin_recent_1 for output: uses dpd≥2 (≥30 raw days) to match org_car
            fmin(when(col("dpd") >= 1, col("idx"))).alias("min_mon_sin_recent_1"),
            # Java: dpd_new==1 = 1-30 days past due (bucket 1) AND bal>500
            fmin(when(col("dpd") == 1, col("idx"))).alias("mon_sin_recent_1"),
            fmin(when(col("dpd") == 2, col("idx"))).alias("mon_sin_recent_2"),
            fmin(when(col("dpd") == 3, col("idx"))).alias("mon_sin_recent_3"),
            fmin(when(col("dpd") == 4, col("idx"))).alias("mon_sin_recent_4"),
            fmin(when(col("dpd") == 5, col("idx"))).alias("mon_sin_recent_5"),
            fmin(when(col("dpd") == 6, col("idx"))).alias("mon_sin_recent_6"),
            fmin(when(col("dpd") == 7, col("idx"))).alias("mon_sin_recent_7"),
            # Java: dpd_new==1 = 1-30 days (bucket 1), consistent with mon_sin_recent_1
            fmax(when(col("dpd") == 1, col("idx"))).alias("mon_sin_first_1"),
            fmax(when(col("dpd") == 2, col("idx"))).alias("mon_sin_first_2"),
            fmax(when(col("dpd") == 3, col("idx"))).alias("mon_sin_first_3"),
            fmax(when(col("dpd") == 4, col("idx"))).alias("mon_sin_first_4"),
            fmax(when(col("dpd") == 5, col("idx"))).alias("mon_sin_first_5"),
            fmax(when(col("dpd") == 6, col("idx"))).alias("mon_sin_first_6"),
            fmax(when(col("dpd") == 7, col("idx"))).alias("mon_sin_first_7"),
        )
    )
    return dpd_attrs


def calculate_max_sanction_secured(exploded):
    """Maximum sanctioned amount on Secured accounts within 36-month window.

    BUSINESS LOGIC:
        Finds the largest mod_lim (sanction) ever observed on any Secured
        account in the 36-month reporting window. Two variants:
          - max_sanc_amt_sec        — across ALL Secured accounts
          - max_sanc_amt_secmov_real — across SecMov subset only

    Returns: DataFrame keyed on cust_id with max_sanc attributes.
    """
    max_sanc_amt_sec_df = (
        exploded
        .filter(col("mod_lim").isNotNull())
        .groupBy("cust_id")
        .agg(
            fmax(when(col("isSec") & (col("idx") <= 35), col("mod_lim"))).alias("max_sanc_amt_sec"),
            fmax(when(col("isSecMov") & (col("idx") <= 35), col("mod_lim"))).alias("max_sanc_amt_secmov_real"),
        )
    )
    return max_sanc_amt_sec_df


def build_per_account_utilization_bases(exploded, ad):
    """Per-account utilization base DataFrames — shared by all utilization metrics.

    BUSINESS LOGIC:
        Utilization = balance / limit, averaged over a window. For correct
        customer-level aggregation we need three TYPES of base:
          (a) avg-of-ratios across ALL accounts        → _per_acct_all_util
          (b) avg-of-ratios across UNSECURED accounts  → _per_acct_uns_util
          (c) sum-of-ratios numerator/denominator      → _per_acct_uns_util2

        Each base groups exploded by (cust_id, _cons_key) and produces
        balance sums, month counts, mod_lim, and per-account util ratios
        for L3M/L6M/L12M windows.

        Also builds _live_for_util (live-account filter), shared by the
        excluding-CC and CC-only utilization functions.

    Returns: tuple of 6 DataFrames:
        (_live_for_util,
         _per_acct_all_util, _per_acct_uns_util, _per_acct_uns_util_l12m,
         _per_acct_uns_util2, _per_acct_uns_util_l12m2)
    """
    # Live-account filter (used by _per_acct_uns_util2 and _per_acct_uns_util_l12m2)
    _live_for_util = (
        ad.select(
            "cust_id",
            "accno",
            F.col("_acct_id").alias("_cons_key"),
            "isLiveAccount"
        )
        .filter(F.col("isLiveAccount") == True)
    )

    # 3. ALL-accounts per-account utilisation (avg-of-ratios)
    # FIX E: Java L1384-1395: denominator = max(mod_lim) × count(ALL months in window).
    # OLD: filter(mod_lim > 0) undercounted months. NEW: all rows, Java-exact denom.
    # ────────────────────────────────────────────────────────────────
    _per_acct_all_util = (
        exploded
        .groupBy("cust_id", "_cons_key")
        .agg(
            fsum(when(col("idx") <= 2,  col("bal"))).alias("_bal_l3m"),
            fsum(when(col("idx") <= 5,  col("bal"))).alias("_bal_l6m"),
            fsum(when(col("idx") <= 11, col("bal"))).alias("_bal_l12m"),
            fsum(when(col("idx") <= 2,  lit(1))).alias("_n_l3m"),
            fsum(when(col("idx") <= 5,  lit(1))).alias("_n_l6m"),
            fsum(when(col("idx") <= 11, lit(1))).alias("_n_l12m"),
            fmax("mod_lim").alias("_lim"),
        )
        .withColumn("_u_l3m",  F.try_divide(col("_bal_l3m"),
            when((col("_n_l3m") >0) & (col("_lim")>0), col("_lim")*col("_n_l3m")).otherwise(lit(0.001))))
        .withColumn("_u_l6m",  F.try_divide(col("_bal_l6m"),
            when((col("_n_l6m") >0) & (col("_lim")>0), col("_lim")*col("_n_l6m")).otherwise(lit(0.001))))
        .withColumn("_u_l12m", F.try_divide(col("_bal_l12m"),
            when((col("_n_l12m")>0) & (col("_lim")>0), col("_lim")*col("_n_l12m")).otherwise(lit(0.001))))
    )

    # ────────────────────────────────────────────────────────────────
    # 4. UNS‑accounts only per-account utilisation
    # Uses live accounts only (isLiveAccount filter via _live_for_util).
    # Removing this filter was tested but worsened util_l12m_uns_tot (-7.9%)
    # and util_l6m_uns_tot (-3.0%) — reverted back to live-only.
    # ────────────────────────────────────────────────────────────────
    _per_acct_uns_util = (
        exploded
        .join(_live_for_util, ["cust_id", "_cons_key"], "inner")
        # Fix: use isUns (product_mapping) not _CV_UNS — includes 244,245,247 etc.
        .filter(col("isUns") & (F.col("mod_lim") > 0))
        .groupBy("cust_id", "_cons_key")
        .agg(
            F.sum(F.when(F.col("idx") <= 2,  F.col("bal"))).alias("_bal_l3m"),
            F.sum(F.when(F.col("idx") <= 5,  F.col("bal"))).alias("_bal_l6m"),
            F.sum(F.when(F.col("idx") <= 2,  F.lit(1))).alias("_n_l3m"),
            F.sum(F.when(F.col("idx") <= 5,  F.lit(1))).alias("_n_l6m"),
            F.max("mod_lim").alias("_lim"),
        )
        .withColumn("_u_l3m",  F.try_divide(F.col("_bal_l3m"),  F.col("_lim") * F.col("_n_l3m")))
        .withColumn("_u_l6m",  F.try_divide(F.col("_bal_l6m"),  F.col("_lim") * F.col("_n_l6m")))
        )

    # Java calculateUtilL12mUnsTot: ALL accounts (no live filter) — confirmed from Java source
    _per_acct_uns_util_l12m = (
        exploded
        # Fix: use isUns (product_mapping) not _CV_UNS — includes 244,245,247 etc.
        .filter(col("isUns") & (F.col("mod_lim") > 0))
        .groupBy("cust_id", "_cons_key")
        .agg(
            F.sum(F.when(F.col("idx") <= 11, F.col("bal"))).alias("_bal_l12m"),
            F.sum(F.when(F.col("idx") <= 11, F.lit(1))).alias("_n_l12m"),
            F.max("mod_lim").alias("_lim"),
        )
    )

    # UNS aggregated — Java uses ratio-of-sums (not avg-of-ratios)
    # Java calculateUtilL6mAllTot: sum(bal_6m) / sum(limit×count_months)
    # When limit=0 → use MODIFIED_LIMIT_DEFAULT=0.001 per account
    # FIX: switch from favg(_u_lXm) to fsum(_bal_lXm) / fsum(_eff_lim_lXm)
    # util_uns: use count_reported in denominator (actual months with data)
    _per_acct_uns_util2 = (
        _per_acct_uns_util
        .withColumn("_eff_lim_l3m",
                    when(col("_lim") > 0, col("_lim") * col("_n_l3m")).otherwise(lit(0.001)))
        .withColumn("_eff_lim_l6m",
                    when(col("_lim") > 0, col("_lim") * col("_n_l6m")).otherwise(lit(0.001)))
    )
    # util_l12m: all accounts, account-level max limit — from Java source
    _per_acct_uns_util_l12m2 = (
        _per_acct_uns_util_l12m
        .withColumn("_eff_lim_l12m",
                    when(col("_lim") > 0, col("_lim") * col("_n_l12m")).otherwise(lit(0.001)))
    )
    return (_live_for_util,
            _per_acct_all_util, _per_acct_uns_util, _per_acct_uns_util_l12m,
            _per_acct_uns_util2, _per_acct_uns_util_l12m2)


def calculate_credit_utilization_unsecured(_per_acct_uns_util, _per_acct_uns_util_l12m,
                                            _per_acct_uns_util2, _per_acct_uns_util_l12m2):
    """Customer-level utilization metrics for UNSECURED accounts.

    BUSINESS LOGIC:
        Two computations from the same per-account base:
          1. avg_util_*_uns_tot  = mean(per_account_util) across uns accts
             (each account weighted equally)
          2. util_*_uns_tot      = sum(bal) / sum(lim)
             (each account weighted by its limit)
        Both computed for L3M / L6M / L12M / live windows.

    Returns: DataFrame keyed on cust_id with all unsecured-utilization metrics.
    """
    util_uns_df = (
        _per_acct_uns_util2
        .groupBy("cust_id")
        .agg(
            # FIX: 0/0 → 0 (Java behavior) instead of NaN
            F.when(fsum(when(col("_n_l3m") > 0, col("_eff_lim_l3m"))) > 0,
                   fsum(when(col("_n_l3m") > 0, col("_bal_l3m"))) /
                   fsum(when(col("_n_l3m") > 0, col("_eff_lim_l3m"))))
             .otherwise(lit(0.0)).alias("util_l3m_uns_tot"),
            F.when(fsum(when(col("_n_l6m") > 0, col("_eff_lim_l6m"))) > 0,
                   fsum(when(col("_n_l6m") > 0, col("_bal_l6m"))) /
                   fsum(when(col("_n_l6m") > 0, col("_eff_lim_l6m"))))
             .otherwise(lit(0.0)).alias("util_l6m_uns_tot"),
        )
        .join(
            _per_acct_uns_util_l12m2.groupBy("cust_id").agg(
                F.when(fsum(when(col("_n_l12m") > 0, col("_eff_lim_l12m"))) > 0,
                       fsum(when(col("_n_l12m") > 0, col("_bal_l12m"))) /
                       fsum(when(col("_n_l12m") > 0, col("_eff_lim_l12m"))))
                 .otherwise(lit(0.0)).alias("util_l12m_uns_tot"),
            ),
            "cust_id", "left"
        )
    )
    return util_uns_df


def calculate_credit_utilization_all(_per_acct_all_util, exploded):
    """Customer-level utilization metrics across ALL accounts (any product).

    BUSINESS LOGIC:
        Mirrors the unsecured version but uses the all-accounts per-account
        base. Includes the FIX where avg_util_*_all_tot wraps the numerator
        with F.coalesce(_, lit(0.0)) — when ALL accounts have bal_l6m <= 0,
        fsum returns NULL but Java initializes to 0.

    Returns: DataFrame keyed on cust_id with all-utilization metrics joined
             with the corresponding ROS (return-on-sales-style) totals.
    """
    # FIX D: _ros_all_per_acct — Java L1317-1321: count ALL months (not just mod_lim>0);
    # denom per account = max_lim × count or 0.001 (MODIFIED_LIMIT_DEFAULT).
    _ros_all_per_acct = (
        exploded
        .groupBy("cust_id", "_cons_key")
        .agg(
            fsum(when(col("idx") <= 2,  col("bal"))).alias("_bal_l3m"),
            fsum(when(col("idx") <= 5,  col("bal"))).alias("_bal_l6m"),
            fsum(when(col("idx") <= 11, col("bal"))).alias("_bal_l12m"),
            fsum(when(col("idx") <= 2,  lit(1))).alias("_n_l3m"),
            fsum(when(col("idx") <= 5,  lit(1))).alias("_n_l6m"),
            fsum(when(col("idx") <= 11, lit(1))).alias("_n_l12m"),
            fmax("mod_lim").alias("_max_lim"),
        )
        .withColumn("_d_l3m",  when((col("_n_l3m") >0) & (col("_max_lim")>0), col("_max_lim")*col("_n_l3m")).otherwise(lit(0.001)))
        .withColumn("_d_l6m",  when((col("_n_l6m") >0) & (col("_max_lim")>0), col("_max_lim")*col("_n_l6m")).otherwise(lit(0.001)))
        .withColumn("_d_l12m", when((col("_n_l12m")>0) & (col("_max_lim")>0), col("_max_lim")*col("_n_l12m")).otherwise(lit(0.001)))
    )
    _ros_all_util = (
        _ros_all_per_acct
        .groupBy("cust_id")
        .agg(
            F.try_divide(fsum(col("_bal_l3m")),  fsum(col("_d_l3m"))).alias("util_l3m_all_tot"),
            F.try_divide(fsum(col("_bal_l6m")),  fsum(col("_d_l6m"))).alias("util_l6m_all_tot"),
            F.try_divide(fsum(col("_bal_l12m")), fsum(col("_d_l12m"))).alias("util_l12m_all_tot"),
        )
    )

    # ────────────────────────────────────────────────────────────────
    # 6. Final ALL‑accounts util dataframe
    # ────────────────────────────────────────────────────────────────
    util_all_df = (
        _per_acct_all_util
        .groupBy("cust_id")
        .agg(
            # Java L1393: only add util IF accountLevelBalanceAmount > 0
            # sum(util when bal>0) / count(all accounts with n>0)
            # FIX: wrap in F.when so 0/0 → 0 (Java behavior) instead of NaN (Spark)
            # FIX 2: coalesce NULL numerator → 0. When all accounts have bal_l6m≤0,
            # fsum(when bal>0, u) returns NULL (Spark) but Java returns 0 (initializer).
            # This was producing NaN for 4384851 and 57696963 — accounts have valid n_l6m
            # but negative bal_l6m, so denominator > 0 but numerator was NULL.
            F.when(fsum(when(col("_n_l3m")>0, lit(1))) > 0,
                   F.coalesce(fsum(when((col("_n_l3m")>0) & (col("_bal_l3m")>0), col("_u_l3m"))), lit(0.0)) /
                   fsum(when(col("_n_l3m")>0, lit(1))))
             .otherwise(lit(0.0)).alias("avg_util_l3m_all_tot"),
            F.when(fsum(when(col("_n_l6m")>0, lit(1))) > 0,
                   F.coalesce(fsum(when((col("_n_l6m")>0) & (col("_bal_l6m")>0), col("_u_l6m"))), lit(0.0)) /
                   fsum(when(col("_n_l6m")>0, lit(1))))
             .otherwise(lit(0.0)).alias("avg_util_l6m_all_tot"),
            F.when(fsum(when(col("_n_l12m")>0, lit(1))) > 0,
                   F.coalesce(fsum(when((col("_n_l12m")>0) & (col("_bal_l12m")>0), col("_u_l12m"))), lit(0.0)) /
                   fsum(when(col("_n_l12m")>0, lit(1))))
             .otherwise(lit(0.0)).alias("avg_util_l12m_all_tot"),
        )
        .join(_ros_all_util, "cust_id", "left")
    )
    return util_all_df


def calculate_credit_utilization_excluding_cc(_per_acct_all_util, _live_for_util, exploded):
    """Customer-level utilization across NON-CREDIT-CARD accounts only.

    BUSINESS LOGIC:
        Filters _per_acct_all_util to exclude CC accounts (isCC==False)
        before customer-level aggregation. Same avg-of-ratios + sum-totals
        split as the all-accounts version, applied to the non-CC subset.
        Used to derive util_*_exc_cc_tot and avg_util_*_exc_cc_tot families.

    Returns: DataFrame keyed on cust_id with exc-CC utilization metrics.
    """
    _util_exc_cc_per_acct = (
        exploded
        .join(_live_for_util, ["cust_id", "_cons_key"], "inner")
        .filter(~col("isCC"))
        .groupBy("cust_id", "_cons_key")
        .agg(
            fsum(when(col("idx") <= 2, col("bal")).otherwise(lit(0.0))).alias("_bal_l3m"),
            fmax("mod_lim").alias("_max_lim"),
            fsum(when(col("idx") <= 2, lit(1))).alias("_cnt_l3m"),
        )
    )
    util_exc_cc_df = (
        _util_exc_cc_per_acct
        .withColumn("_denom",
            when((col("_cnt_l3m") > 0) & (col("_max_lim") > 0),
                 col("_max_lim") * col("_cnt_l3m"))
            .otherwise(lit(0.001)))
        .groupBy("cust_id").agg(
            F.try_divide(fsum("_bal_l3m"), fsum("_denom")).alias("util_l3m_exc_cc_live"),
        )
    )
    return util_exc_cc_df


def calculate_credit_utilization_cc_only(exploded, _live_for_util):
    """Customer-level utilization across CREDIT CARD accounts only.

    BUSINESS LOGIC:
        Computed on LIVE accounts (current month, isCC=True, mod_lim > 0).
        Two computations per window:
          - avg_util_*_cc_live  — mean per-account util ratio across live CCs
          - util_*_cc_live      — sum(bal)/sum(lim) weighted total
        Windows: L3M / L6M / L12M.

    Returns: DataFrame keyed on cust_id with cc-only utilization metrics.
    """
    # that still appear within the 36m window; isLiveAccount reflects the final status.
    _cc_live_per_acct = (
        exploded
        .join(_live_for_util, ["cust_id", "_cons_key"], "inner")   # FIX: inner join = live accts only
        # Java CC codes include 220 for util_l3m_cc_live calculation
        .filter((col("isCC") | (col("account_type_cd")=="220")) & (col("mod_lim") > 0))
        .groupBy("cust_id", "_cons_key")
        .agg(
            fsum(when(col("idx") <= 2,  col("bal"))).alias("_cc_bal_l3m"),
            fsum(when(col("idx") <= 5,  col("bal"))).alias("_cc_bal_l6m"),
            fsum(when(col("idx") <= 11, col("bal"))).alias("_cc_bal_l12m"),
            fsum(when(col("idx") <= 2,  lit(1))).alias("_cc_n_l3m"),
            fsum(when(col("idx") <= 5,  lit(1))).alias("_cc_n_l6m"),
            fsum(when(col("idx") <= 11, lit(1))).alias("_cc_n_l12m"),
            fmax("mod_lim").alias("_cc_lim"),
        )
        # util_cc: count_reported denominator
        .withColumn("_eff_l3m",
                    when(col("_cc_lim") > 0, col("_cc_lim") * col("_cc_n_l3m")).otherwise(lit(0.001)))
        .withColumn("_eff_l6m",
                    when(col("_cc_lim") > 0, col("_cc_lim") * col("_cc_n_l6m")).otherwise(lit(0.001)))
        .withColumn("_eff_l12m",
                    when(col("_cc_lim") > 0, col("_cc_lim") * col("_cc_n_l12m")).otherwise(lit(0.001)))
    )
    util_cc_df = (
        _cc_live_per_acct
        .groupBy("cust_id").agg(
            (fsum(when(col("_cc_n_l3m")  > 0, col("_cc_bal_l3m")))  /
             fsum(when(col("_cc_n_l3m")  > 0, col("_eff_l3m")))).alias("util_l3m_cc_live"),
            (fsum(when(col("_cc_n_l6m")  > 0, col("_cc_bal_l6m")))  /
             fsum(when(col("_cc_n_l6m")  > 0, col("_eff_l6m")))).alias("util_l6m_cc_live"),
            (fsum(when(col("_cc_n_l12m") > 0, col("_cc_bal_l12m"))) /
             fsum(when(col("_cc_n_l12m") > 0, col("_eff_l12m")))).alias("util_l12m_cc_live"),
            fmax(when(col("_cc_n_l3m") > 0, F.try_divide(col("_cc_bal_l3m"), col("_cc_lim")))).alias("util_m0_cc"),
        )
    )
    return util_cc_df


# ════════════════════════════════════════════════════════════════════════════
#  END HELPER FUNCTIONS — build_global_attrs orchestrator follows           ░
# ════════════════════════════════════════════════════════════════════════════


def build_global_attrs(account_details, fact2_enriched, open_dt_all=None):
    # Rename cons_acct_key to _acct_id to eliminate AMBIGUOUS_REFERENCE.
    # build_account_details groups by cons_acct_key and joins DataFrames that also
    # carry cons_acct_key — Spark sees the name twice in the column lineage.
    # Using a private alias throughout Phase 4 avoids this completely.
    ad = account_details.withColumnRenamed("cons_acct_key", "_acct_id")
    fe = fact2_enriched

    _CV_UNS = {5,121,123,130,167,169,170,176,177,178,179,181,187,189,
               196,197,198,199,200,213,214,215,216,217,
               224,225,226,227,228,999, 242}  # 242 added: UnSec in product_mapping


    # ─────────────────────────────────────────────────────────────────────────
    #   PHASE 4 ORCHESTRATION — call business-domain helper functions
    # ─────────────────────────────────────────────────────────────────────────
    # Each helper returns a customer-level DataFrame. They are joined together
    # in the FINAL ASSEMBLY block at the end of this function.
    # ─────────────────────────────────────────────────────────────────────────

    # 1. Shared base — exploded monthly snapshots
    exploded = prepare_monthly_exploded(ad)

    # ── DIAGNOSTIC 1: exploded structure for customer 9414008 ────────────────
    try:
        print()
        print("=" * 70)
        print("  DIAGNOSTIC 1: exploded structure for customer 9414008")
        print("=" * 70)
        _dc = "9414008"
        _e = exploded.filter(col("cust_id") == _dc)
        _n = _e.count()
        print(f"  exploded rows for {_dc}: {_n}  (expected 168)")
        if _n > 0:
            # Per-account row counts
            _per_acct = (_e.groupBy("_cons_key", "account_type_cd")
                         .agg(F.count("*").alias("n_rows"))
                         .orderBy("_cons_key"))
            print(f"  Per-account row counts in exploded:")
            _per_acct.show(20, False)
            # Per-idx rows (how many rows per month_diff)
            _per_idx = (_e.groupBy("idx").agg(F.count("*").alias("n_rows")).orderBy("idx"))
            print(f"  Per-idx row counts in exploded (should be #accounts per idx):")
            _per_idx.show(35, False)
            # Sample rows
            print(f"  Sample rows (first 5):")
            _e.select("cust_id","_cons_key","idx","bal","dpd","is_not_closed").show(5, False)
        print("=" * 70)
        print()
    except Exception as _de:
        print(f"  [DIAG1] Failed: {_de}")

    # 2. Account inventory — counts of accounts by product/category/window
    acct_counts = count_accounts_by_product_type(exploded, ad)

    # 3. Accounts opened by MOB window (recent vs older)
    #    _open_cnt_df is also needed downstream in the FINAL ASSEMBLY join
    _open_cnt_df, accts_opened = count_accounts_opened_by_window(exploded, ad)

    # 4. Maximum DPD attributes (delinquency severity across windows)
    dpd_attrs = derive_max_dpd_attributes(exploded, ad)

    # ─────────────────────────────────────────────────────────────────────────
    # FIX: max_dpd_UNS_L6_M and max_dpd_UNS_6_12_M use ACCOUNT-LEVEL isUns
    # (taken from most-recent month's account_type_cd). For accounts that
    # change their reported type over time (e.g. type 47 → 999), this misses
    # rows where the historical type WAS UnSec.
    #
    # Specific case: cust 32351314 / account 4659434958 has type 47 (Sec)
    # in 29 of 30 months and type 999 (UnSec) at idx=12 only. Account-level
    # isUns = False, so its idx=12 row gets excluded from max_dpd_UNS_6_12_M.
    # Java uses per-ROW isUns, so it captures the type-999 row.
    #
    # Fix: Re-compute these two metrics from fact2_enriched (which has
    # per-row isUns from the row-level product_mapping join), then OVERRIDE
    # the values in dpd_attrs. NaN-preserving formulation matches helper.
    # ─────────────────────────────────────────────────────────────────────────
    _dpd_uns_perrow = (
        fact2_enriched.filter(col("month_diff") <= 12)  # idx 0..12 only
        .groupBy("cust_id")
        .agg(
            fmax(when(col("isUns") & (col("month_diff") <= 6),
                     when(col("dpd_new") > 0, col("dpd_new")).otherwise(lit(0)))
                ).alias("_max_dpd_UNS_L6_M_perrow"),
            fmax(when(col("isUns") & (col("month_diff") > 6) & (col("month_diff") <= 12),
                     when(col("dpd_new") > 0, col("dpd_new")).otherwise(lit(0)))
                ).alias("_max_dpd_UNS_6_12_M_perrow"),
        )
    )
    # Override the helper's values where the per-row computation differs.
    # Use coalesce(per_row, original) so missing customers in fact2_enriched
    # fall through to the helper's value (preserves NaN-preserving semantic).
    dpd_attrs = (
        dpd_attrs.join(_dpd_uns_perrow, "cust_id", "left")
        .withColumn("max_dpd_UNS_L6_M",
                    F.coalesce(col("_max_dpd_UNS_L6_M_perrow"), col("max_dpd_UNS_L6_M")))
        .withColumn("max_dpd_UNS_6_12_M",
                    F.coalesce(col("_max_dpd_UNS_6_12_M_perrow"), col("max_dpd_UNS_6_12_M")))
        .drop("_max_dpd_UNS_L6_M_perrow", "_max_dpd_UNS_6_12_M_perrow")
    )

    # 5. Maximum sanctioned amount on Secured accounts
    max_sanc_amt_sec_df = calculate_max_sanction_secured(exploded)

    # 6. Build per-account utilization base DataFrames + live-account filter
    (_live_for_util,
     _per_acct_all_util, _per_acct_uns_util, _per_acct_uns_util_l12m,
     _per_acct_uns_util2, _per_acct_uns_util_l12m2) = build_per_account_utilization_bases(exploded, ad)

    # 7. Customer-level utilization — UNSECURED accounts
    util_uns_df = calculate_credit_utilization_unsecured(
        _per_acct_uns_util, _per_acct_uns_util_l12m,
        _per_acct_uns_util2, _per_acct_uns_util_l12m2)

    # 8. Customer-level utilization — ALL accounts
    util_all_df = calculate_credit_utilization_all(_per_acct_all_util, exploded)

    # 9. Customer-level utilization — EXCLUDING CC accounts
    util_exc_cc_df = calculate_credit_utilization_excluding_cc(_per_acct_all_util, _live_for_util, exploded)

    # 10. Customer-level utilization — CC ACCOUNTS ONLY
    util_cc_df = calculate_credit_utilization_cc_only(exploded, _live_for_util)

    # ─────────────────────────────────────────────────────────────────────────
    #   REMAINING BUSINESS MODULES — currently inline below.
    #   To be extracted into named functions following the pattern above:
    #     • Exposure / current-balance summary (4E)
    #     • Trend percentages over time
    #     • Cashflow / outflow by loan type
    #     • Balance ratios (0-12 vs 13-24 windows)
    #     • Account simultaneity metrics
    #     • Account opening frequency & recency
    #     • CC-40% utilization metrics
    #     • Special-case attributes (agri, derog months, bank concentration)
    #     • FINAL ASSEMBLY — all per-customer DataFrames left-joined → global_attrs
    # ─────────────────────────────────────────────────────────────────────────

    # ── 4E. Exposure / balance amounts ──────────────────────────────────────
    exp_df = (
        exploded.groupBy("cust_id").agg(
            fsum(when(col("idx") == 0, col("bal")).otherwise(lit(0))).alias("curr_tot_exp_amt"),
            fsum(when((col("idx") == 0) & col("isUns"), col("bal")).otherwise(lit(0))).alias("curr_tot_exp_amt_uns"),
            fsum(when((col("idx") == 0) & col("isSec"), col("bal")).otherwise(lit(0))).alias("curr_tot_exp_amt_sec"),
            fsum(when((col("idx") == 0) & col("isCC"),  col("bal")).otherwise(lit(0))).alias("curr_tot_exp_amt_cc"),
            fsum(when((col("idx") == 0) & col("isHL"),  col("bal")).otherwise(lit(0))).alias("curr_tot_exp_amt_hl"),
            favg(when(col("idx") <= 2,  col("bal"))).alias("avg_bal_l3m_all"),
            favg(when(col("idx") <= 5,  col("bal"))).alias("avg_bal_l6m_all"),
            favg(when(col("idx") <= 11, col("bal"))).alias("avg_bal_l12m_all"),
            favg(when((col("idx") <= 2) & col("isUns") & ~col("isCC"), col("bal"))).alias("avg_bal_l3m_uns_wo_cc"),
            fmax(when(col("idx") == 0, col("mod_lim"))).alias("max_sanc_amt_m0"),
            fsum(when(col("idx") == 0, col("mod_lim")).otherwise(lit(0))).alias("curr_tot_sanc_amt"),
            fmax(col("mod_lim")).alias("max_sanc_amt_ever"),
            fmax(when((col("idx") == 0) & col("isSec"), col("mod_lim"))).alias("max_sanc_amt_sec_m0"),
            # FIX 8: max_sanc_amt_sec = max mod_lim on ALL Secured accounts at current month
            # GT=2000000 for 10003821 (equals max_sanc_amt from ALL accounts)
            fmax(when((col("idx") == 0) & col("isSec"), col("mod_lim"))).alias("max_sanc_amt_sec"),
            # FIX 5: sum_sanc_amt_uns = sum of max mod_lim per unsec account across all months
            # Closed accounts have no idx==0 row → were excluded with idx==0 filter
            # GT=90000 for 10003821 (closed unsec account, max credit_lim=90000)
            # Computed via per-account max then aggregated in exp_df join chain below
            fsum(when((col("idx") == 0) & col("isUns"), col("mod_lim")).otherwise(lit(0))).alias("sum_sanc_amt_uns_curr"),
            # FIX 16: Use all months (not just idx==0) for product-level max sanction amounts.
            # Closed accounts have no idx=0 row, so fmax over only idx==0 returns NaN.
            # GT=1210658 for max_sanc_amt_al on 10003821 (AL account, closed before scoring date).
            # Using all months captures the max limit ever sanctioned for these product types.
            fmax(when(col("isPL"), col("mod_lim"))).alias("max_sanc_amt_pl"),
            # Java getAllALProductCodes includes code 221 (verified: all 3 Ref values come from code 221)
            fmax(when(col("isAL") | (col("account_type_cd") == "221"), col("mod_lim"))).alias("max_sanc_amt_al"),
            fmax(when(col("isTW"), col("mod_lim"))).alias("max_sanc_amt_tw"),
            fmax(when(col("isCL"), col("mod_lim"))).alias("max_sanc_amt_cl"),
            # fmax(when((col("idx") == 0) & col("account_type_cd").isin("240","242","244","245","246","247","248","249"),
            #           col("mod_lim"))).alias("max_sanc_amt_cl"),
        )
    )

    # ── 4F. Balance trend flags ──────────────────────────────────────────────
    # Two separate monthly balance DataFrames:
    #   monthly_bal_exccc — exc CC: used for totconsinc (increases in non-CC balance)
    #   monthly_bal_all   — ALL accounts: used for totconsdec (any balance decrease)
    #
    # Trade-data analysis (cust 10004499):
    #   totconsdec with excCC = 33 (HL loans always decrease) vs GT=24
    #   totconsdec with ALL accounts = 23 (CC volatile balances add INC months) → GT=24 ✓
    #   totconsinc_exc_cc variables MUST use excCC (names say "exc_cc") → same as before
    # FIX 5: Java bru.py L895 uses !accountLevelDetailsV4.isCC() — ACCOUNT-LEVEL isCC.
    # Our old code: fe.filter(~isCC) = per-row isCC → wrong when account changed type.
    # Fix: use exploded (account-level isCC from most-recent-month, con_47 fix).
    # exploded.bal = balance_amt, exploded.idx = month_diff.
    monthly_bal_exccc = (
        exploded.filter(~col("isCC"))
        .groupBy("cust_id", col("idx").alias("month_diff"))
        .agg(fsum("bal").alias("tot_bal"))
    )
    # totconsdec uses ALL accounts (no isCC exclusion) — fe is correct here
    monthly_bal_all = (
        fe
        .groupBy("cust_id", "month_diff")
        .agg(fsum("balance_amt").alias("tot_bal_all"))
    )
    # WINDOW: ASC order so lead() gives OLDER (higher month_diff) period.
    w_trend = Window.partitionBy("cust_id").orderBy(col("month_diff").asc())

    # totconsinc variables — based on excCC balance
    trend_exccc_df = (
        monthly_bal_exccc
        .withColumn("prev_bal", lead("tot_bal").over(w_trend))
        .withColumn("pct_chg",
                    when(col("prev_bal") > 0,
                         F.try_divide(col("tot_bal") - col("prev_bal"), col("prev_bal")))
                    .otherwise(lit(None)))
        .groupBy("cust_id").agg(
            # FIX: Extend lower bound from md>=1 to md>=0 (include current month transition).
            # Diagnostic on 8713231 & 31968676 showed:
            #   - 8713231 bal10_25m: 3→4 ✓ (matches ref=4)
            #   - 8713231 bal10_7m:  2→3 ✓ (matches ref=3)
            #   - 8713231 bal5_7m:   3→4 ✓ (matches ref=4)
            #   - 31968676 bal10_25m: 7→7 (no regression, value unchanged)
            # Net gain: +3 cells for 8713231, no regression on 31968676.
            # The earlier "+36 regressions" comment was about extending UPPER bound to 24/6
            # (different change). Lower-bound extension to 0 is safe and correct.
            # Java: > Percentage (strictly greater than, not >=) — keep strict comparison.
            fsum(when((col("month_diff") >= 0) & (col("month_diff") <= 23) & (col("pct_chg") > 0.10),
                      lit(1)).otherwise(lit(0))).alias("totconsinc_bal10_exc_cc_25m"),
            fsum(when((col("month_diff") >= 0) & (col("month_diff") <= 23) & (col("pct_chg") > 0.05),
                      lit(1)).otherwise(lit(0))).alias("totconsinc_bal5_exc_cc_25m"),
            fsum(when((col("month_diff") >= 0) & (col("month_diff") <= 5) & (col("pct_chg") > 0.10),
                      lit(1)).otherwise(lit(0))).alias("totconsinc_bal10_exc_cc_7m"),
            fsum(when((col("month_diff") >= 0) & (col("month_diff") <= 5) & (col("pct_chg") > 0.05),
                      lit(1)).otherwise(lit(0))).alias("totconsinc_bal5_exc_cc_7m"),
        )
    )

    # ──────────────────────────────────────────────────────────────────
    # DIAGNOSTIC: totconsinc_bal*_exc_cc trace for customers 8713231 & 31968676
    # 8713231: pipeline gives 3 for bal10_25m but sc2_3=4 (REAL BUG)
    # 8713231: pipeline gives 3 for bal5_7m but sc2_3=2 (REAL BUG)
    # 31968676: pipeline gives 7 for bal10_25m, sc2_3=7 (MATCHES — safety check)
    # If we change boundary [1,23]→[0,23], does 31968676 stay at 7?
    # ──────────────────────────────────────────────────────────────────
    for _diag_cust in ["8713231", "31968676"]:
        print()
        print("=" * 70)
        print(f"  DIAGNOSTIC: totconsinc_bal*_exc_cc trace for {_diag_cust}")
        print("=" * 70)
        _trc = (monthly_bal_exccc.filter(col("cust_id").cast("string") == _diag_cust)
                .withColumn("prev_bal", lead("tot_bal").over(w_trend))
                .withColumn("pct_chg",
                            when(col("prev_bal") > 0,
                                 F.try_divide(col("tot_bal") - col("prev_bal"), col("prev_bal")))
                            .otherwise(lit(None)))
                .orderBy("month_diff").collect())
        print(f"  monthly_bal_exccc rows for {_diag_cust}: {len(_trc)}")
        print(f"  {'idx':>3} {'tot_bal':>14} {'prev_bal':>14} {'pct_chg':>10} {'>0.10?':>7} {'>0.05?':>7}")
        _cnt_25m_10 = 0; _cnt_25m_5 = 0; _cnt_7m_10 = 0; _cnt_7m_5 = 0
        for _r in _trc:
            _md = _r["month_diff"]; _tb = _r["tot_bal"]; _pb = _r["prev_bal"]; _pc = _r["pct_chg"]
            _b10 = _pc is not None and _pc > 0.10
            _b5  = _pc is not None and _pc > 0.05
            if _md is not None:
                if 1 <= _md <= 23 and _b10: _cnt_25m_10 += 1
                if 1 <= _md <= 23 and _b5:  _cnt_25m_5  += 1
                if 1 <= _md <= 5  and _b10: _cnt_7m_10  += 1
                if 1 <= _md <= 5  and _b5:  _cnt_7m_5   += 1
            _tb_s = f"{_tb:.2f}" if _tb is not None else "None"
            _pb_s = f"{_pb:.2f}" if _pb is not None else "None"
            _pc_s = f"{_pc:.4f}" if _pc is not None else "None"
            print(f"  {_md:>3} {_tb_s:>14} {_pb_s:>14} {_pc_s:>10} {str(_b10):>7} {str(_b5):>7}")
        print(f"  totconsinc_bal10_exc_cc_25m (md∈[1,23], pct>0.10) = {_cnt_25m_10}")
        print(f"  totconsinc_bal5_exc_cc_25m  (md∈[1,23], pct>0.05) = {_cnt_25m_5}")
        print(f"  totconsinc_bal10_exc_cc_7m  (md∈[1,5],  pct>0.10) = {_cnt_7m_10}")
        print(f"  totconsinc_bal5_exc_cc_7m   (md∈[1,5],  pct>0.05) = {_cnt_7m_5}")
        # Try also with extended range md∈[0,23]
        _alt_25m_10 = sum(1 for _r in _trc if _r["month_diff"] is not None and 0 <= _r["month_diff"] <= 23
                           and _r["pct_chg"] is not None and _r["pct_chg"] > 0.10)
        _alt_7m_5 = sum(1 for _r in _trc if _r["month_diff"] is not None and 0 <= _r["month_diff"] <= 5
                           and _r["pct_chg"] is not None and _r["pct_chg"] > 0.05)
        print(f"  ALT bal10_25m (md∈[0,23]): {_alt_25m_10}")
        print(f"  ALT bal5_7m   (md∈[0,5]):  {_alt_7m_5}")
        # FLOOR truncation versions
        def _floor2(x):
            if x is None: return None
            return int(x * 100) / 100.0 if x >= 0 else -(int(-x * 100) / 100.0)
        _flr_25m_10 = sum(1 for _r in _trc if _r["month_diff"] is not None and 1 <= _r["month_diff"] <= 23
                          and _r["pct_chg"] is not None and _floor2(_r["pct_chg"]) > 0.10)
        _flr_7m_5 = sum(1 for _r in _trc if _r["month_diff"] is not None and 1 <= _r["month_diff"] <= 5
                          and _r["pct_chg"] is not None and _floor2(_r["pct_chg"]) > 0.05)
        _flr_25m_10_ext = sum(1 for _r in _trc if _r["month_diff"] is not None and 0 <= _r["month_diff"] <= 23
                          and _r["pct_chg"] is not None and _floor2(_r["pct_chg"]) > 0.10)
        _flr_7m_5_ext = sum(1 for _r in _trc if _r["month_diff"] is not None and 0 <= _r["month_diff"] <= 5
                          and _r["pct_chg"] is not None and _floor2(_r["pct_chg"]) > 0.05)
        print(f"  FLOOR bal10_25m [1,23]: {_flr_25m_10}    FLOOR bal5_7m [1,5]: {_flr_7m_5}")
        print(f"  FLOOR+EXT bal10_25m [0,23]: {_flr_25m_10_ext}    FLOOR+EXT bal5_7m [0,5]: {_flr_7m_5_ext}")
        print("=" * 70)
        print()
    # ──────────────────────────────────────────────────────────────────


    # totconsdec — based on ALL accounts with missing months filled as 0 (Java DEFAULT_LONG_VALUE=0L)
    # Java: for missing index in accountBalMap → defaults to 0. Missing month = 0 balance.
    # If prev month has bal>0 and current=0 → decrease → counted. Our pipeline skips None.
    # Fix: cross-join to full 0..35 range and fill missing months with 0.
    _cust_ids_df = monthly_bal_all.select("cust_id").distinct()
    _months_df   = spark.range(36).select(col("id").cast("int").alias("month_diff"))
    _full_grid   = _cust_ids_df.crossJoin(_months_df)
    _monthly_bal_all_filled = (
        _full_grid
        .join(monthly_bal_all, ["cust_id", "month_diff"], "left")
        .fillna(0.0, subset=["tot_bal_all"])
    )
    trend_all_df = (
        _monthly_bal_all_filled
        .withColumn("prev_bal_all", lead("tot_bal_all").over(w_trend))
        .withColumn("pct_chg_all",
                    # Java: (current - previous) / previous. When prev=0, result=0 (not counted).
                    # When prev is NULL (no older month), skip.
                    # FIX: use when(prev > 0) to match Java's prev==0 → 0.0 (not < 0) skip logic
                    # Java L4526: if(previous != 0) compute ratio — includes negative prev!
                    # Our old code used > 0 → skipped negative prev → undercounted decreases
                    # Fix: use != 0 to match Java exactly
                    when(col("prev_bal_all") != 0,
                         F.try_divide(col("tot_bal_all") - col("prev_bal_all"), col("prev_bal_all")))
                    .otherwise(lit(None)))
        .groupBy("cust_id").agg(
            # Java: counts pairs (idx, idx+1) for idx in 0..35 where change < 0.0
            # month_diff=35 pair: lead=NULL → pct_chg=None → not counted (same as Java prev=0 skip)
            fsum(when(col("pct_chg_all") < 0, lit(1)).otherwise(lit(0))).alias("totconsdec_bal_tot_36m"),
        )
    )

    # Merge the two trend DataFrames
    # FIX: Use OUTER join so customers with ONLY CC accounts (who are absent
    # from trend_exccc_df) are still in trend_df for totconsdec_bal_tot_36m.
    # Otherwise they ended up as NaN in the final join for totconsdec even though
    # reference has a valid value computed from the ALL-accounts series.
    trend_df = trend_exccc_df.join(trend_all_df, "cust_id", "outer")

    # Keep monthly_total_bal = excCC version for the consecutive UDF below
    monthly_total_bal = monthly_bal_exccc.withColumnRenamed("tot_bal", "tot_bal")

    # FIX 10 — consinc_bal10_exc_cc_25m: consecutive months from most recent
    # where exc-CC total balance increased >=10%. Different from totconsinc which
    # counts ALL months in the window. This counts from month_diff=1 outward and
    # stops at the first month that did NOT increase >=10%.
    _consec_bal_schema = StructType([
        StructField("cust_id",               LongType(),   nullable=False),
        StructField("consinc_bal10_exc_cc_25m", IntegerType(), nullable=True),
    ])

    def compute_consinc_bal10_exc_cc(pdf):
        # Java calculateConcPercIncreaseFlags(balInc, 25, 0.10):
        #   Builds percIncList[0..24]: pct[i] = (balInc[i]-balInc[i+1])/balInc[i+1]
        #   Then finds MAX CONSECUTIVE run where pct > 0.10
        #   DOES NOT break on first miss — scans all 25 positions for max run
        # FIX: was breaking on first non-qualifying month → always returned 0
        results = []
        for cust_id, grp in pdf.groupby("cust_id"):
            grp = grp.sort_values("month_diff")
            bal_map = dict(zip(grp["month_diff"], grp["tot_bal"]))
            max_cons = 0
            cons = 0
            for m in range(0, 25):   # Java: index 0..24 (indexLimit=25)
                b_curr = bal_map.get(m, 0)
                b_prev = bal_map.get(m + 1, 0)
                if b_prev > 0:
                    pct = (b_curr - b_prev) / b_prev
                    if pct > 0.10:    # Java: > Percentage (strict)
                        cons += 1
                        if cons > max_cons:
                            max_cons = cons
                    else:
                        cons = 0      # reset but CONTINUE scanning
                else:
                    cons = 0          # Java: DEFAULT_DOUBLE_VALUE breaks streak
            results.append({"cust_id": int(cust_id), "consinc_bal10_exc_cc_25m": max_cons})
        return pd.DataFrame(results, columns=["cust_id", "consinc_bal10_exc_cc_25m"])

    # consinc_bal10_exc_cc_25m consecutive UDF — pass raw balances for idx 0..24
    # Java uses bal[m] directly and computes pct=(bal[m]-bal[m+1])/bal[m+1]
    _trend_pct_rows = (
        monthly_bal_exccc
        .filter((col("month_diff") >= 0) & (col("month_diff") <= 24))
        .select("cust_id", "month_diff", col("tot_bal"))
    )
    _consec_rows = _trend_pct_rows.rdd.collect()
    _consec_pdf2  = pd.DataFrame(_consec_rows, columns=["cust_id","month_diff","tot_bal"])
    if len(_consec_pdf2) > 0:
        _consec_parts2  = [compute_consinc_bal10_exc_cc(grp) for _, grp in _consec_pdf2.groupby("cust_id")]
        _consec_result2 = pd.concat(_consec_parts2, ignore_index=True)
    else:
        _consec_result2 = pd.DataFrame(columns=["cust_id","consinc_bal10_exc_cc_25m"])
    consinc_bal10_exc_cc_computed = spark.createDataFrame(
        spark.sparkContext.parallelize([tuple(r) for r in _consec_result2.itertuples(index=False)])
        if len(_consec_result2) > 0 else spark.sparkContext.parallelize([], 1),
        schema=_consec_bal_schema)
    # FIX: Customers with non-CC accounts but no recent data should get 0 (not NaN).
    # Start from all cust_ids with non-CC accounts (from exploded), left-join computed.
    _exccc_holders = (
        exploded.filter(~col("isCC"))
        .select(col("cust_id").cast(LongType()).alias("cust_id"))
        .distinct()
    )
    consinc_bal10_exc_cc_df = (
        _exccc_holders.join(consinc_bal10_exc_cc_computed, "cust_id", "left")
        .withColumn("consinc_bal10_exc_cc_25m",
                    F.coalesce(col("consinc_bal10_exc_cc_25m"), lit(0)))
    )

    # ── consinc_bal10_tot_7m: consecutive >10% total balance increase in 7m ──
    # FIX 2: Changed window to ASC order so lead() gives the OLDER (higher month_diff) balance.
    # Old: orderBy(DESC) + lead() → lead gave NEWER month → comparison was (current - future) / future (WRONG).
    # New: orderBy(ASC)  + lead() → lead gives OLDER month → pct_chg = (newer - older) / older (CORRECT).
    # consinc_bal10_tot_7m uses ALL accounts (total balance including CC)
    _all_bal_m = (
        fe.groupBy("cust_id","month_diff")
          .agg(fsum("balance_amt").alias("_tot_b_all"))
    )
    _w_all = Window.partitionBy("cust_id").orderBy(col("month_diff").asc())
    consinc_all7_df = (
        _all_bal_m
        .withColumn("_prev_b", lead("_tot_b_all").over(_w_all))
        .withColumn("_pct_b", when(col("_prev_b") > 0,
                                   F.try_divide(col("_tot_b_all")-col("_prev_b"),col("_prev_b")))
                              .otherwise(lit(None)))
        .withColumn("_inc_flag",
            when((col("month_diff").between(0, 5)) & (col("_pct_b") > 0.10), lit(1))
            .otherwise(lit(0)))
        .groupBy("cust_id").agg(
            F.collect_list(F.struct(
                col("month_diff").alias("m"),
                col("_inc_flag").alias("f")
            )).alias("_mf")
        )
    )

    def _max_consec_udf(mf):
        if not mf: return 0
        flags = [x.f for x in sorted(mf, key=lambda x: x.m)]
        mx = 0; c = 0
        for f in flags:
            if f: c += 1; mx = max(mx, c)
            else: c = 0
        return mx

    _max_consec_spark = F.udf(_max_consec_udf, "int")
    consinc_all7_df = (
        consinc_all7_df
        .withColumn("consinc_bal10_tot_7m", _max_consec_spark(col("_mf")))
        .drop("_mf")
    )

    # ── 4G. Outflow algorithms (EMI estimation) ──────────────────────────────
    # ── Java calculateOverflow_HC faithful rewrite ─────────────────────────────
    # Java ScoreCardHelperV4.calculateOverflow_HC does:
    #   1. Sort each account's months DESC (recent idx=0 first)
    #   2. Eligibility: account needs maxConsecutiveMonths >= 2 (at least 2 consecutive
    #      reported months with bal>0, where "consecutive" = month diff of 1 or -11)
    #   3. For eligible accounts: mark months that are part of a run of 3+ consecutive
    #      months (consec_3_marker windowing), collect up to 20 such balances
    #   4a. CD (type 189): EMI = bal[i] - bal[i+1] for i in 1..len-1 (skips i=0)
    #       excluding "full payoff" months where bal_diff == bal[i]
    #   4b. Others: estimate IR from balance ratios, find modal IR bucket,
    #       then EMI = bal[i]*(1+finalIR/12) - bal[i+1] for i in 0..len-1
    #   5. Return SUM of per-account median EMIs

    # Outflow input: one row per (cust_id, cons_acct_key, month_diff) with bal > 0
    # Use exploded which has idx (=month_diff), bal, isCC, isHL, isAL, isTW, isPL, isSecMov
    # Java uses account-level flags (isHL, isAL, etc.) and per-month balance

    # Pull all monthly data needed for outflow computation.
    # exploded already has all product flags (isHL, isAL, isTW, isPL, isSecMov etc.).
    # account_type_cd is only needed to identify CD (type 189) — join just that from ad.
    _of_input = (
        exploded
        .filter(col("bal") > 0)
        .select(
            "cust_id", "_cons_key", "accno",
            col("idx").alias("month_diff"),
            col("bal").alias("balance_amt"),
            "isCC", "isHL", "isAL", "isTW", "isPL",
            "isSecMov", "isSecMovRegSec", "isRegUns", "isPlCdTw",
        )
        .join(
            ad.select("cust_id", col("_acct_id").alias("_cons_key"),
                      col("account_type_cd").alias("_acd_of"),
                      col("isLiveAccount").alias("_is_live")),
            ["cust_id", "_cons_key"], "left"
        )
        .withColumnRenamed("_acd_of", "account_type_cd")
        .withColumnRenamed("_is_live", "isLiveAccount")
    )

# ══════════════════════════════════════════════════════════════════════════
    # EMI OUTFLOW COMPUTATION — custom_var.py reference (L927-1324)
    # Population-level IR estimation + limit-band fallback for accounts
    # without enough consecutive balance history.
    # ══════════════════════════════════════════════════════════════════════════

    def _build_common_prods(trade_pdf, acct_type_col="account_type_cd"):
        """
        Replicates custom_var common_processing_all_prods (L936-1029).
        Input: pandas DataFrame with live accounts, columns:
               cust_id, tgt_key, account_type_cd, open_dt, balance_dt, balance_amt
        Returns pivot DataFrame with columns: cust_id, tgt_key, account_type_cd, '1'..'20'
        """
        import numpy as np

        bal = trade_pdf[["cust_id", "tgt_key", acct_type_col,
                          "open_dt", "balance_dt", "balance_amt"]].copy()

        # Convert dates for sorting
        bal["bal_dt_1"]  = pd.to_datetime(bal["balance_dt"].astype(str), format="%Y%m%d", errors="coerce")
        bal["open_dt_1"] = pd.to_datetime(bal["open_dt"].astype(str),    format="%Y%m%d", errors="coerce")

        bal.sort_values(["cust_id", "tgt_key", "open_dt_1", "bal_dt_1"],
                        ascending=True, inplace=True)

        # First 50 rows per account
        bal2 = bal.groupby(["cust_id", "tgt_key"]).head(50).copy()

        # Keep positive balances only
        bal3 = bal2[bal2["balance_amt"] > 0].copy()
        if bal3.empty:
            return pd.DataFrame(columns=["cust_id", "tgt_key", acct_type_col] +
                                        [str(i) for i in range(1, 21)])

        # Consecutive month marker within each account group
        bal3["balance_mth"]     = (bal3["balance_dt"].astype(float) // 100 % 100).astype(int)
        bal3["bal_mth_lag"]     = bal3.groupby(["cust_id", "tgt_key"])["balance_mth"].shift(1)
        bal3["lag_diff"]        = bal3["balance_mth"] - bal3["bal_mth_lag"]
        bal3["consec_marker"]   = np.where(bal3["lag_diff"].isin([1, -11]), 1, 0)
        bal3["consec_mk_lag"]   = bal3.groupby(["cust_id", "tgt_key"])["consec_marker"].shift(1).fillna(0)
        bal3["consec_3_marker"] = np.where((bal3["consec_marker"] + bal3["consec_mk_lag"]) == 2, 1, 0)

        # Accounts that have at least one consec_3 row
        grp1 = bal3.groupby(["cust_id", "tgt_key"])["consec_3_marker"].max().reset_index()
        grp1.rename(columns={"consec_3_marker": "consec_3"}, inplace=True)
        target_no_consec = grp1[grp1["consec_3"] == 0][["cust_id", "tgt_key"]]
        no_consec_keys = set(zip(target_no_consec["cust_id"], target_no_consec["tgt_key"]))

        bal4 = bal3[~bal3.set_index(["cust_id", "tgt_key"]).index.isin(no_consec_keys)].copy()
        if bal4.empty:
            return pd.DataFrame(columns=["cust_id", "tgt_key", acct_type_col] +
                                        [str(i) for i in range(1, 21)]), target_no_consec

        # Extend consec_3 markers to cover 3-row windows (lag-1 and lag-2)
        bal4["c3_lag1"] = bal4.groupby(["cust_id", "tgt_key"])["consec_3_marker"].shift(-1).fillna(0)
        bal4["c3_lag2"] = bal4.groupby(["cust_id", "tgt_key"])["consec_3_marker"].shift(-2).fillna(0)
        bal4["final_consec"] = np.where(
            (bal4["consec_3_marker"] == 1) | (bal4["c3_lag1"] == 1) | (bal4["c3_lag2"] == 1), 1, 0)

        # Keep only rows in consecutive windows
        bal5 = bal4[bal4["final_consec"] == 1].copy()

        # Sort by balance_dt, take first 30 per account
        bal5.sort_values(["cust_id", "tgt_key", "balance_dt"], inplace=True)
        bal6 = bal5.groupby(["cust_id", "tgt_key"]).head(30).copy()

        # Keep positive balances, assign sequential ID 1,2,3,...
        bal6 = bal6[bal6["balance_amt"] > 0].copy()
        bal6["ID"] = bal6.groupby(["cust_id", "tgt_key"]).cumcount() + 1
        bal6 = bal6[bal6["ID"] <= 20]  # max 20 balances
        bal6["ID"] = bal6["ID"].astype(str)

        # Pivot to wide format
        bal7 = bal6.pivot_table(
            values="balance_amt",
            index=["cust_id", "tgt_key", acct_type_col],
            columns="ID"
        ).reset_index()
        bal7.columns.name = None

        # Ensure columns 1..20 exist
        for c in [str(i) for i in range(1, 21)]:
            if c not in bal7.columns:
                bal7[c] = float("nan")

        cols_keep = ["cust_id", "tgt_key", acct_type_col] + [str(i) for i in range(1, 21)]
        bal7 = bal7[[c for c in cols_keep if c in bal7.columns]]

        return bal7, target_no_consec

    def _build_common_limits(trade_pdf, acct_type_col="account_type_cd"):
        """custom_var common_processing_all_limits (L927-934): max(original_loan_amt)."""
        cols = ["cust_id", "tgt_key", acct_type_col]
        lim = trade_pdf.groupby(cols).agg(
            max_lim=("original_loan_amt", "max")
        ).reset_index()
        lim2 = lim.drop(columns=[acct_type_col])
        return lim, lim2

    def _assign_limit_bands(df, col, lim_arr):
        """Map max_lim to 1..20 bands using 19 quantile breakpoints."""
        import numpy as np
        conds = [df[col] <= lim_arr[0]]
        for k in range(len(lim_arr) - 1):
            conds.append((df[col] > lim_arr[k]) & (df[col] <= lim_arr[k+1]))
        conds.append(df[col] > lim_arr[-1])
        choices = list(range(1, len(conds) + 1))
        return np.select(conds, choices, default=10)

    def _further_processing(bal7, limits2, acct_type_col="account_type_cd"):
        """
        Replicates custom_var further_processing_all_exc_cc_cd (L1066-1324).
        Returns final_frame with cust_id, tgt_key, account_type_cd, FINAL_IR, FINAL_EMI, max_lim
        """
        import numpy as np

        if bal7 is None or bal7.empty:
            return pd.DataFrame(columns=["cust_id", "tgt_key", acct_type_col,
                                          "FINAL_IR", "FINAL_EMI", "max_lim"])

        b = bal7.copy()

        # ── IR computation: IR_i = ((bal[i+1]-bal[i+2])/(bal[i]-bal[i+1])-1)*12 ──
        for i in range(1, 19):
            c1, c2, c3 = str(i), str(i+1), str(i+2)
            if c1 in b and c2 in b and c3 in b:
                denom = b[c1].sub(b[c2])
                numer = b[c2].sub(b[c3])
                ir    = (numer.div(denom) - 1) * 12
                b[f"IR{i}"] = ir
            else:
                b[f"IR{i}"] = float("nan")

        # Filter IR to valid range [0.06, 0.45)
        for i in range(1, 19):
            ir_col = f"IR{i}"
            b[f"IR{i}_NEW"] = np.where(
                (b[ir_col] >= 0.06) & (b[ir_col] < 0.45), b[ir_col], float("nan"))

        # Bucket each IR_NEW into 21 buckets (0.06-0.08, 0.08-0.10, ..., 0.46-0.48)
        buckets = [(0.06 + k*0.02, 0.08 + k*0.02) for k in range(21)]
        for i in range(1, 19):
            vc = f"IR{i}_NEW"
            bc = f"IR{i}_NEW_B_{i}"
            # custom_var: all buckets use IR > lo AND IR <= hi (exclusive lower bound)
            # IR exactly at 0.06 → valid range but no bucket → IR_NEW_B = NaN → excluded
            conds_b  = [((b[vc] > lo) & (b[vc] <= hi)) for lo, hi in buckets]
            b[bc] = np.select(conds_b, list(range(1, 22)), default=float("nan"))
            b[bc] = np.where(b[vc].isna(), float("nan"), b[bc])

        # Modal bucket across IR1..IR18
        bucket_cols = [f"IR{i}_NEW_B_{i}" for i in range(1, 19)]
        modes_df  = b[bucket_cols].mode(axis=1)
        b["mode"]     = modes_df.iloc[:, 0]
        b["mode_cnt"] = b[bucket_cols].eq(b["mode"], axis=0).sum(axis=1)

        # Zero out non-modal IR; zero out if mode_cnt==1
        for i in range(1, 19):
            vc = f"IR{i}_NEW"
            bc = f"IR{i}_NEW_B_{i}"
            b[vc] = np.where(b[bc] == b["mode"], b[vc], float("nan"))
            b[vc] = np.where(b["mode_cnt"] == 1, float("nan"), b[vc])

        # AVG_FINAL_IR = mean of surviving IR values; cap at 0.45
        ir_new_cols = [f"IR{i}_NEW" for i in range(1, 19)]
        b["AVG_FINAL_IR"] = b[ir_new_cols].mean(axis=1)
        bal8 = b[b["AVG_FINAL_IR"].notna()].copy()
        bal8["FINAL_IR"] = np.where(bal8["AVG_FINAL_IR"] >= 0.45, 0.45, bal8["AVG_FINAL_IR"])
        bal8 = bal8.drop_duplicates(["cust_id", "tgt_key"])

        # ── EMI computation: EMI_Mi = bal[i]*(1+IR/12) - bal[i+1] ──
        emi_cols = []
        for i in range(1, 19):
            ec = f"EMI_M{i}"
            c1, c2 = str(i), str(i+1)
            if c1 in bal8.columns and c2 in bal8.columns:
                bal8[ec] = bal8[c1] * (1 + bal8["FINAL_IR"] / 12) - bal8[c2]
            else:
                bal8[ec] = float("nan")
            emi_cols.append(ec)

        bal8["FINAL_EMI"] = bal8[emi_cols].median(axis=1)
        bal9 = bal8[["cust_id", "tgt_key", acct_type_col, "FINAL_IR", "FINAL_EMI"]].copy()
        bal9 = bal9.drop_duplicates(["cust_id", "tgt_key"])

        # Join limits
        bal10 = pd.merge(bal9,
                         limits2[["cust_id", "tgt_key", "max_lim"]],
                         on=["cust_id", "tgt_key"], how="inner")
        bal10 = bal10.drop_duplicates(["cust_id", "tgt_key"])

        # NOTE: The limit-band fallback (custom_var L1247-1322) requires the FULL
        # production population for correct quantile computation. Running it on a
        # small validation batch assigns wrong median EMI to accounts without valid
        # IR, causing spurious non-zero outflow for customers with ref=0.
        # Decision: skip fallback → accounts without valid IR contribute NaN → 0.
        # This matches reference behaviour for those customers.
        final_frame = bal10.drop_duplicates(["cust_id", "tgt_key"])
        return final_frame

    # ── Collect trade data for EMI products ──────────────────────────────────
    # Live accounts only (custom_var L885: closed_max==0)
    # EMI data: use fact2_enriched (has open_dt + original_loan_amt per row)
    # custom_var L885: live accounts only (closed_max==0 = no close date across all rows)
    # Join with account_details to filter isLiveAccount (account open at scoring date)
    # EMI: join fact2_enriched with account_details to get isLiveAccount flag
    # Alias both sides to avoid ambiguous column references in Spark join
    _fe_emi  = fact2_enriched.alias("fe_emi")
    _ad_emi  = account_details.select(
        col("cust_id").alias("_ad_cust"),
        col("cons_acct_key").alias("_ad_ck"),
        col("isLiveAccount").alias("_is_live_emi"),
        col("account_type_cd").alias("_atc_emi"),
    ).alias("ad_emi")
    _emi_live = (
        _fe_emi
        .join(_ad_emi,
              (F.col("fe_emi.cust_id")        == F.col("ad_emi._ad_cust")) &
              (F.col("fe_emi.cons_acct_key")   == F.col("ad_emi._ad_ck")),
              "left")
        .filter(F.col("ad_emi._is_live_emi") == True)
        .select(
            F.col("fe_emi.cust_id"),
            F.col("fe_emi.cons_acct_key").alias("tgt_key"),
            F.col("ad_emi._atc_emi").alias("account_type_cd"),
            F.col("fe_emi.open_dt"),
            F.col("fe_emi.balance_dt"),
            F.col("fe_emi.balance_amt"),
            F.col("fe_emi.original_loan_amt"),
        )
    )
    _emi_pdf = _emi_live.rdd.collect()
    _emi_pdf = pd.DataFrame(_emi_pdf, columns=[
        "cust_id", "tgt_key", "account_type_cd", "open_dt",
        "balance_dt", "balance_amt", "original_loan_amt"])
    _emi_pdf["balance_amt"]       = pd.to_numeric(_emi_pdf["balance_amt"],       errors="coerce")
    _emi_pdf["original_loan_amt"] = pd.to_numeric(_emi_pdf["original_loan_amt"], errors="coerce").fillna(0)
    _emi_pdf["balance_dt"]        = pd.to_numeric(_emi_pdf["balance_dt"],        errors="coerce")
    _emi_pdf["open_dt"]           = pd.to_numeric(_emi_pdf["open_dt"],           errors="coerce")
    _emi_pdf["account_type_cd"]   = _emi_pdf["account_type_cd"].astype(str).str.lstrip("0")

    def _further_processing_cd(bal7, limits2):
        """
        custom_var further_processing_cd (L1474-1613):
        CD outflow = median(BAL_DIFF_1..17) where BAL_DIFF_i = bal[i+1] - bal[i+2].
        No IR computation — direct balance differences. Negative EMI is valid.
        """
        import numpy as np
        if bal7 is None or bal7.empty:
            return pd.DataFrame(columns=["cust_id", "tgt_key", "account_type_cd", "FINAL_EMI", "max_lim"])
        b = bal7.copy()
        # BAL_DIFF_i = bal[i+1] - bal[i+2]  (custom_var L1479-1498)
        diff_cols = []
        for i in range(1, 18):
            c1, c2 = str(i+1), str(i+2)
            dc = f"BAL_DIFF_{i}"
            if c1 in b.columns and c2 in b.columns:
                b[dc] = b[c1] - b[c2]
            else:
                b[dc] = float("nan")
            diff_cols.append(dc)
        # FINAL_EMI = median of BAL_DIFF_1..17 (including negatives — custom_var L1516)
        b["FINAL_EMI"] = b[diff_cols].median(axis=1)
        bal8 = b[b["FINAL_EMI"].notna()].copy()
        bal8 = bal8.drop_duplicates(["cust_id", "tgt_key"])
        if bal8.empty:
            return pd.DataFrame(columns=["cust_id", "tgt_key", "account_type_cd", "FINAL_EMI", "max_lim"])
        bal9 = bal8[["cust_id", "tgt_key", "account_type_cd", "FINAL_EMI"]].copy()
        bal9 = bal9.drop_duplicates(["cust_id", "tgt_key"])
        # Join limits (custom_var L1529)
        bal10 = pd.merge(bal9, limits2[["cust_id", "tgt_key", "max_lim"]],
                         on=["cust_id", "tgt_key"], how="inner")
        bal10 = bal10.drop_duplicates(["cust_id", "tgt_key"])
        return bal10

    def _compute_outflow(product_codes, label):
        """Run full custom_var EMI pipeline for one product group."""
        tr = _emi_pdf[_emi_pdf["account_type_cd"].isin(product_codes)].copy()
        if tr.empty:
            return pd.DataFrame(columns=["cust_id", label])
        result = _build_common_prods(tr)
        if isinstance(result, tuple):
            bal7, no_consec = result
        else:
            bal7 = result
            no_consec = pd.DataFrame(columns=["cust_id", "tgt_key"])
        lim_full, lim2 = _build_common_limits(tr)
        # lim2 already contains cust_id, tgt_key, max_lim — no re-merge needed
        final = _further_processing(bal7, lim2)
        if final.empty:
            return pd.DataFrame(columns=["cust_id", label])
        out = final.groupby("cust_id")["FINAL_EMI"].sum().reset_index()
        out.rename(columns={"FINAL_EMI": label}, inplace=True)
        return out

    def _compute_cd_outflow():
        """CD outflow uses balance-difference algorithm (custom_var further_processing_cd)."""
        tr = _emi_pdf[_emi_pdf["account_type_cd"].isin({"189"})].copy()
        if tr.empty:
            return pd.DataFrame(columns=["cust_id", "CD_outflow"])
        result = _build_common_prods(tr)
        bal7 = result[0] if isinstance(result, tuple) else result
        lim_full, lim2 = _build_common_limits(tr)
        final = _further_processing_cd(bal7, lim2)
        if final.empty:
            return pd.DataFrame(columns=["cust_id", "CD_outflow"])
        out = final.groupby("cust_id")["FINAL_EMI"].sum().reset_index()
        out.rename(columns={"FINAL_EMI": "CD_outflow"}, inplace=True)
        return out

    _pl_out  = _compute_outflow({"123", "228"},  "PL_outflow")
    _hl_out  = _compute_outflow({"58", "195"},   "HL_LAP_outflow")
    _al_out  = _compute_outflow({"47", "173"},   "AL_TW_outflow")
    _cd_out  = _compute_cd_outflow()             # custom_var BAL_DIFF algorithm

    def _pdf_to_spark(pdf, col_name):
        if pdf.empty:
            return spark.createDataFrame([], schema=StructType([
                StructField("cust_id", LongType(), False),
                StructField(col_name,  DoubleType(), True),
            ]))
        pdf = pdf.copy()
        pdf["cust_id"] = pd.to_numeric(pdf["cust_id"], errors="coerce").astype("int64")
        pdf[col_name]  = pd.to_numeric(pdf[col_name],  errors="coerce")
        rows = [(int(r["cust_id"]), float(r[col_name])) for _, r in pdf.iterrows()]
        return spark.createDataFrame(
            spark.sparkContext.parallelize(rows),
            schema=StructType([
                StructField("cust_id", LongType(), False),
                StructField(col_name,  DoubleType(), True),
            ])
        )

    outflow_pl_df        = _pdf_to_spark(_pl_out,  "PL_outflow")
    outflow_hl_lap_df    = _pdf_to_spark(_hl_out,  "HL_LAP_outflow")
    outflow_al_tw_df     = _pdf_to_spark(_al_out,  "AL_TW_outflow")
    outflow_cd_df        = _pdf_to_spark(_cd_out,  "CD_outflow")

    # Composite outflows reusing per-product results
    def _combine_outflows(parts, label):
        if not parts:
            return pd.DataFrame(columns=["cust_id", label])
        merged = pd.concat(parts, ignore_index=True)
        if "FINAL_EMI" in merged.columns:
            out = merged.groupby("cust_id")["FINAL_EMI"].sum().reset_index()
        else:
            # already named as outflow cols – sum all numeric except cust_id
            num_cols = [c for c in merged.columns if c != "cust_id"]
            merged["_emi"] = merged[num_cols].sum(axis=1)
            out = merged.groupby("cust_id")["_emi"].sum().reset_index()
        out.rename(columns={"FINAL_EMI": label, "_emi": label}, inplace=True)
        return out

    # Outflow_uns_secmov = HL+AL+PL+CD combined (non-CC)
    _uns_parts = []
    for _pdf, _lbl in [(_pl_out,"PL_outflow"), (_hl_out,"HL_LAP_outflow"),
                       (_al_out,"AL_TW_outflow"), (_cd_out,"CD_outflow")]:
        if not _pdf.empty:
            _uns_parts.append(_pdf.rename(columns={_lbl: "_emi"}))
    if _uns_parts:
        _uns_merged = pd.concat(_uns_parts).groupby("cust_id")["_emi"].sum().reset_index()
        _uns_merged.rename(columns={"_emi": "Outflow_uns_secmov"}, inplace=True)
    else:
        _uns_merged = pd.DataFrame(columns=["cust_id", "Outflow_uns_secmov"])

    outflow_df        = _pdf_to_spark(_uns_merged, "Outflow_uns_secmov")
    outflow_plcdtw_df = _pdf_to_spark(
        _combine_outflows(
            [p.rename(columns={l: "_emi"}) for p, l in
             [(_pl_out,"PL_outflow"),(_al_out,"AL_TW_outflow"),(_cd_out,"CD_outflow")] if not p.empty],
            "Outflow_AL_PL_TW_CD"),
        "Outflow_AL_PL_TW_CD")
    total_outflow_wo_cc = _pdf_to_spark(
        _combine_outflows(
            [p.rename(columns={l: "_emi"}) for p, l in
             [(_hl_out,"HL_LAP_outflow"),(_al_out,"AL_TW_outflow"),
              (_pl_out,"PL_outflow"),(_cd_out,"CD_outflow")] if not p.empty],
            "total_outflow_wo_cc"),
        "total_outflow_wo_cc")


    print(f"[calculateOverflow_HC] HL_LAP:{outflow_hl_lap_df.count()} AL_TW:{outflow_al_tw_df.count()} PL:{outflow_pl_df.count()} CD:{outflow_cd_df.count()} customers computed")

    # ── 4H. Max lim uns secmov

    # ── 4H. Max lim uns secmov ───────────────────────────────────────────────
    max_lim_df = (
        exploded.filter(col("idx") == 0)
        .groupBy("cust_id").agg(
            fmax(when(col("isSecMovRegSec") | col("isRegUns"), col("mod_lim"))
                 ).alias("max_lim_uns_secmov"),
            # MAX_LIMIT_AL_PL_TW_CD per spec: sum of per-type max limits
            fmax(when(col("isAL"),  col("mod_lim"))).alias("_max_lim_al"),
            fmax(when(col("isPL"),  col("mod_lim"))).alias("_max_lim_pl"),
            fmax(when(col("isTW"),  col("mod_lim"))).alias("_max_lim_tw"),
            # CD is included in isPlCdTw; approximate as PL codes
            fmax(when(col("isPlCdTw") & ~col("isAL") & ~col("isTW") & ~col("isPL"),
                      col("mod_lim"))).alias("_max_lim_cd"),
        )
        .withColumn("MAX_LIMIT_AL_PL_TW_CD",
                    coalesce(col("_max_lim_al"), lit(0)) +
                    coalesce(col("_max_lim_pl"), lit(0)) +
                    coalesce(col("_max_lim_tw"), lit(0)) +
                    coalesce(col("_max_lim_cd"), lit(0)))
        .drop("_max_lim_al","_max_lim_pl","_max_lim_tw","_max_lim_cd")
    )

    # ── 4I. Months since derogatory event ────────────────────────────────────
    derog_months = (
        exploded.filter(col("dpd") > 0)
        .groupBy("cust_id").agg(
            fmin("idx").alias("mon_since_last_derog"),
            fmax("dpd").alias("max_dpd_ever"),
        )
    )

    # ── 4J. Max balance & worst delinquency ─────────────────────────────────
    # FIX 3: Removed mon_since_max_bal_l24m_uns from here — it is already produced
    # by mon_since_max_bal_df (renamed via rename_map at output time).
    # Having it in both DataFrames created a duplicate column in the joined output,
    # which caused all columns after the first duplicate to misalign in the CSV.
    max_bal_df = (
        ad.groupBy("cust_id").agg(
            fmax("max_bal_l24m").alias("max_bal_l24m"),
            fmax("max_bal_l12m").alias("max_bal_l12m"),
            fmin("mon_since_first_worst_delq").alias("mon_since_first_worst_delq"),
            fmin("mon_since_recent_worst_delq").alias("mon_since_recent_worst_delq"),
        )
    )


    # ── agri balance ratio ───────────────────────────────────────────────────
    # Java bru.py L2449-2478: calculateAvgB_1to6_7to12_agri
    # Filter: isLiveAccount() AND isAgri()
    # Ranges: 0..5 for first half, 6..11 for second half (NOT 1..6 / 7..12)
    # Per account: max(balance in 0..5), max(balance in 6..11)
    # Then: avg of those per-account maxes across ALL live agri accounts
    # Result: avg_first_half / avg_second_half
    _agri_per_acct = (
        exploded
        .join(_live_for_util, ["cust_id", "_cons_key"], "inner")   # isLiveAccount filter
        .filter(col("isAgri"))                                      # isAgri filter
        .groupBy("cust_id", "_cons_key")
        .agg(
            # Java: max balance in range 0..5 (if range non-empty)
            fmax(when(col("idx").between(0, 5), col("bal"))).alias("_max_b_0_5"),
            # Java: max balance in range 6..11 (if range non-empty)
            fmax(when(col("idx").between(6, 11), col("bal"))).alias("_max_b_6_11"),
        )
    )
    agri_bal_ratio_df = (
        _agri_per_acct
        .groupBy("cust_id").agg(
            # Java: averageFirstHalf = avg of max0to5 per account (only for accounts that had 0..5 data)
            favg(col("_max_b_0_5")).alias("_agri_avg_first"),
            # Java: averageSecondHalf = avg of max6to11 per account (only for accounts that had 6..11 data)
            favg(col("_max_b_6_11")).alias("_agri_avg_second"),
        )
        .withColumn("avg_b_1to6_by_7_12_agri",
                    when(col("_agri_avg_second") > 0,
                         F.try_divide(col("_agri_avg_first"), col("_agri_avg_second")))
                    .otherwise(lit(None)))
        .drop("_agri_avg_first", "_agri_avg_second")
    )

    # ── 4K. Reporting counts ─────────────────────────────────────────────────
    # FIX 5: Changed countDistinct("accno") → countDistinct("_cons_key").
    # When accno == cust_id (this dataset), countDistinct(accno) per customer always
    # returns 1 regardless of how many accounts exist. _cons_key = cons_acct_key
    # is the true per-account unique key embedded in exploded at build time.
    rpt_df = (
        exploded.groupBy("cust_id").agg(
            countDistinct(when(col("idx") == 0,  col("_cons_key"))).alias("tot_accts_rptd_0m"),
            countDistinct(when(col("idx") <= 2,  col("_cons_key"))).alias("tot_accts_rptd_l3m"),
            countDistinct(when(col("idx") <= 11, col("_cons_key"))).alias("tot_accts_rptd_l12m"),
        )
    )

    # ── 4L. Bank-category splits ─────────────────────────────────────────────
    bank_df = (
        exploded.groupBy("cust_id").agg(
            fsum((col("isPSB")  & col("isUns") & (col("idx") == 0)).cast("int")).alias("curr_exp_psb_uns"),
            fsum((col("isPVT")  & col("isUns") & (col("idx") == 0)).cast("int")).alias("curr_exp_pvt_uns"),
            fsum((col("isNBFC") & col("isUns") & (col("idx") == 0)).cast("int")).alias("curr_exp_nbfc_uns"),
            fsum((col("isSFB")  & col("isUns") & (col("idx") == 0)).cast("int")).alias("curr_exp_sfb_uns"),
            fsum(col("isPSB").cast("int")).alias("nbr_accts_psb"),
            fsum(col("isPVT").cast("int")).alias("nbr_accts_pvt"),
            fsum(col("isNBFC").cast("int")).alias("nbr_accts_nbfc"),
            fsum(col("isSFB").cast("int")).alias("nbr_accts_sfb"),
        )
    )

    # ── 4M. MAX SIMUL UNSEC WO CC (spec-correct: last 12m, DPD≤4, not closed) ─
    # Per spec: "last 12 months, not closed, DPD bucket ≤ 4"
    # GENERIC FIX: use _cons_key to correctly count simultaneous accounts
    # countDistinct("accno") fails when accno==cust_id (always gives 1)
    simul_df = (
        exploded
        .filter(
            col("isUns") & ~col("isCC") &
            (col("idx") >= 0) & (col("idx") <= 11) &   # FIX: Java key < 12 = idx 0..11 (includes score month)
            (col("dpd") <= 4) &
            col("is_not_closed")
        )
        .groupBy("cust_id", "idx")
        .agg(countDistinct("_cons_key").alias("simul_cnt"))
        .groupBy("cust_id")
        .agg(fmax("simul_cnt").alias("max_simul_unsec_wo_cc"))
    )

    # GENERIC FIX: _cons_key for correct account counting
    simul_gl_df = (
        exploded
        .filter(
            (col("isUns") | col("isGLExt")) & ~col("isCC") &
            (col("idx") >= 0) & (col("idx") <= 11) &   # FIX: Java key < 12 = idx 0..11
            (col("dpd") <= 4) &
            col("is_not_closed")
        )
        .groupBy("cust_id", "idx")
        .agg(countDistinct("_cons_key").alias("simul_gl_cnt"))
        .groupBy("cust_id")
        .agg(fmax("simul_gl_cnt").alias("max_simul_unsec_wo_cc_inc_gl"))
    )

    # max_simul_unsec: max simultaneous unsecured (incl CC) in last 12m
    # GENERIC FIX: _cons_key for correct simultaneous count
    simul_uns_all_df = (
        exploded
        # FIX: Java key < TWELVE_MONS_PAYMNT_HIST (=12) → idx 0..11 includes score month (idx=0)
        # bru.py L568: if(key >= ScoreCardV4Constants.TWELVE_MONS_PAYMNT_HIST) continue;
        .filter(col("isUns") & (col("idx") >= 0) & (col("idx") <= 11) & (col("dpd") <= 4) & col("is_not_closed"))
        .groupBy("cust_id","idx")
        .agg(countDistinct("_cons_key").alias("_cnt_uns_all"))
        .groupBy("cust_id")
        .agg(fmax("_cnt_uns_all").alias("max_simul_unsec"))
    )

    # Max simultaneous PL+CD (123, 242, 189) per spec
    # GENERIC FIX: _cons_key for counting
    # Java: allUnSecProductCodes AND in{123,242,189} — NO TW(173) since TW is Sec
    # index < TWELVE_MONS_PAYMNT_HIST(12) = index 0..11
    simul_plcd_df = (
        exploded
        .filter(
            col("isUns") &
            col("account_type_cd").isin("123", "242", "189") &
            (col("idx") >= 0) & (col("idx") <= 11) &
            (col("dpd") <= 4) & col("is_not_closed")
        )
        .groupBy("cust_id", "idx")
        .agg(countDistinct("_cons_key").alias("simul_plcd_cnt"))
        .groupBy("cust_id")
        .agg(fmax("simul_plcd_cnt").alias("max_simul_pl_cd"))
    )

    # ── 4N. Consecutive DPD marker (Pandas UDF) ──────────────────────────────
    consec_schema = StructType([
        StructField("cust_id",             LongType(),    nullable=False),
        StructField("final_consec_marker", IntegerType(), nullable=True),
    ])

    def compute_consec_udf(pdf):
        results = []
        for cust_id, grp in pdf.groupby("cust_id"):
            max_dpd_by_idx = grp.groupby("idx")["dpd"].max().to_dict()
            consec = 0
            for m_idx in range(36):
                if max_dpd_by_idx.get(m_idx, 0) > 0:
                    consec += 1
                else:
                    break
            results.append({"cust_id": int(cust_id), "final_consec_marker": consec})
        return pd.DataFrame(results, columns=["cust_id", "final_consec_marker"])

    # RDD collect → pandas groupby → RDD parallelize (Java 25 Hadoop workaround)
    _consec_sel = exploded.select("cust_id","idx","dpd")
    _consec_rows = _consec_sel.rdd.collect()
    _consec_pdf  = pd.DataFrame(_consec_rows, columns=["cust_id","idx","dpd"])
    _consec_parts = [compute_consec_udf(grp) for _, grp in _consec_pdf.groupby("cust_id")]
    _consec_result = pd.concat(_consec_parts, ignore_index=True) if _consec_parts else pd.DataFrame(
        columns=["cust_id","final_consec_marker"])
    consec_df = spark.createDataFrame(
        spark.sparkContext.parallelize([tuple(r) for r in _consec_result.itertuples(index=False)]),
        schema=consec_schema)

    # ── 4O. Balance window ratios (spec attributes) ──────────────────────────
    # FIX 6b: Two separate live_bal DataFrames:
    #   live_bal_all  — ALL live accounts (for balance_amt_0_6_by_7_12 and live_cnt)
    #   live_bal_uns  — UnSec only (for balance_amt_0_12_by_13_24)
    # FIX 6 (original) removed isUns entirely — but that caused balance_amt_0_12_by_13_24
    # to include HL accounts which have large 13-24m balances, producing a ratio
    # when GT expects NaN (org_car uses UnSec only for that ratio).
    # live_cnt_0_6_by_7_12 and balance_amt_0_6_by_7_12 correctly use ALL accounts.
    #
    # FIX cnt_7_12 window: idx 7..12 (not 6..11). Months 7 through 12 from scoring date.
    # live_bal_all: original qualifying filter — used for bal_amt_12_24 (idx 0..24)
    # Java sumBalAmtByRange: qualifies if not_closed AND dpd<=6 — NO bal>0 check
    # Negative balances (overpayments) ARE included in balance ratio calculations
    # Confirmed from simulation: 1122607 (0.229≈0.23) and 4384851 (0.598≈0.60)
    _live_qualify_bal = (
        (col("dpd") >= 0) & (col("dpd") <= 6) &
        col("is_not_closed")
        # NOTE: no bal>0 filter — Java includes negative balances in balance sums
    )
    # For util/count calculations: keep bal>0 (negative bal = overpayment, not a real draw)
    _live_qualify = (
        (col("dpd") >= 0) & (col("dpd") <= 6) &
        col("is_not_closed") &
        (col("bal") > 0)
    )
    live_bal_all = (
        exploded
        .filter(_live_qualify)
        .groupBy("cust_id", "idx")
        .agg(
            fsum("bal").alias("sum_bal"),
            countDistinct("_cons_key").alias("cnt_trades"),
        )
    )

    # live_bal_0_12_zf: 0-fill version for live_cnt and balance_amt_0_6_by_7_12
    # Java adds 0 for non-qualifying rows → all reported months appear in map → correct avg denominator
    # Denominator = months where at least 1 account reported (not fixed 7/6 months)
    _live_zf = (
        exploded
        .filter(col("idx").between(0, 12))
        .groupBy("cust_id", "idx")
        .agg(
            # sum_bal_zf: use _live_qualify_bal (no bal>0) — Java includes negative balances in ratio
            fsum(when(_live_qualify_bal, col("bal")).otherwise(lit(0.0))).alias("sum_bal_zf"),
            # cnt_trades_zf: use _live_qualify (bal>0) — live_cnt counts positively-live accounts only
            F.countDistinct(when(_live_qualify, col("_cons_key"))).alias("cnt_trades_zf"),
        )
    )

    # Java sumBalAmtByRange (dqlBalFlag=false) uses ALL accounts (no isUns filter).
    # For each (cust_id, idx): if not_closed AND dpd<=6 → add balance, else add 0.
    # .average() over range includes ALL months in map (even 0-value ones).
    # This matches our zero-fill _live_zf approach but for ALL idx 0..24.
    # Confirmed: simulation with this logic matches reference exactly for all
    # 16 tested customers (2027158, 2643104, ..., 9941726).
    _bal_zf_all = (
        exploded
        .filter(col("idx").between(0, 24))
        .groupBy("cust_id", "idx")
        .agg(
            # _live_qualify_bal: no bal>0 — Java includes negative balances in balance_amt_0_12
            fsum(when(_live_qualify_bal, col("bal")).otherwise(lit(0.0))).alias("sum_bal_all_zf"),
        )
    )

    live_bal_uns = (
        exploded
        .filter(
            (col("dpd") >= 0) & (col("dpd") <= 6) &
            col("is_not_closed") &
            col("isUns")   # UnSec only — kept for other uses
        )
        .groupBy("cust_id", "idx")
        .agg(
            fsum("bal").alias("sum_bal_uns"),
        )
    )

    # Keep live_bal as alias for backward compat with bal_amt_12_24_df below
    live_bal = live_bal_all

    # ── DIAGNOSTIC: print intermediate values for customer 9414008 ─────────
    # This helps pinpoint where the balance_amt_0_12_by_13_24 computation diverges.
    # Safe to leave on — only prints 4 small tables per run.
    try:
        print()
        print("=" * 70)
        print("  DIAGNOSTIC: balance_0_12 trace for customer 9414008")
        print("=" * 70)
        _diag_cust = "9414008"
        _n_exp = exploded.filter(col("cust_id") == _diag_cust).count()
        print(f"  exploded.filter(cust_id='{_diag_cust}').count() = {_n_exp}")
        print(f"  (Expected: 168 for correct data)")
        print()
        print(f"  _bal_zf_all rows for {_diag_cust}:")
        _bal_zf_all.filter(col("cust_id") == _diag_cust).orderBy("idx").show(30, False)
        _r1 = (_bal_zf_all.filter(col("cust_id") == _diag_cust)
               .filter(col("idx").between(0, 12))
               .agg(favg("sum_bal_all_zf").alias("bal_0_12"))
               .collect())
        _r2 = (_bal_zf_all.filter(col("cust_id") == _diag_cust)
               .filter(col("idx").between(13, 24))
               .agg(favg("sum_bal_all_zf").alias("bal_13_24"))
               .collect())
        _b0 = _r1[0]["bal_0_12"] if _r1 else None
        _b13 = _r2[0]["bal_13_24"] if _r2 else None
        _ratio = _b0 / _b13 if (_b0 is not None and _b13 and _b13 > 0) else None
        print(f"  bal_0_12  = {_b0}")
        print(f"  bal_13_24 = {_b13}")
        print(f"  RATIO (balance_amt_0_12_by_13_24) = {_ratio}")
        print(f"  Expected: 1.6536 (if this matches, the computation is correct)")
        print("=" * 70)
        print()
    except Exception as _diag_e:
        print(f"  [DIAG] Trace failed: {_diag_e}")

    bal_ratio_df = (
        # Use 0-fill version for balance_amt_0_6 and live_cnt (Java includes 0 for non-qualifying)
        _live_zf.groupBy("cust_id").agg(
            favg(when((col("idx") >= 0) & (col("idx") <= 6),  col("sum_bal_zf"))).alias("_bal_0_6"),
            favg(when((col("idx") >= 7) & (col("idx") <= 12), col("sum_bal_zf"))).alias("_bal_7_12"),
            favg(when((col("idx") >= 0) & (col("idx") <= 6),  col("cnt_trades_zf"))).alias("_cnt_0_6"),
            favg(when((col("idx") >= 7) & (col("idx") <= 12), col("cnt_trades_zf"))).alias("_cnt_7_12"),
        )
        .join(
            # Java sumBalAmtByRange: ALL accounts, 0-fill. avg(0..12) / avg(13..24).
            # avg = sum/count where count = number of months in the map (all 13 or 12 months).
            _bal_zf_all.filter(col("idx").between(0, 12)).groupBy("cust_id").agg(
                favg("sum_bal_all_zf").alias("_bal_0_12_all"),
            ).join(
                _bal_zf_all.filter(col("idx").between(13, 24)).groupBy("cust_id").agg(
                    favg("sum_bal_all_zf").alias("_bal_13_24_all"),
                ), "cust_id", "left"
            ),
            "cust_id", "left"
        )
        .withColumn("balance_amt_0_12_by_13_24",
                    # Java behavior: denom > 0 → num/denom (0/x = 0 correctly);
                    # denom = 0 or missing → NaN. The "NaN→0 when has data" attempt was
                    # too broad — it made valid NaN cases return 0 (5 regressions vs 2 wins).
                    # Original simpler form matches Java correctly.
                    when(col("_bal_13_24_all") > 0,
                         F.try_divide(col("_bal_0_12_all"), col("_bal_13_24_all")))
                    .otherwise(lit(None)))
        .withColumn("live_cnt_6_12",
                    when(col("_cnt_7_12") > 0,
                         F.try_divide(col("_cnt_0_6"), col("_cnt_7_12"))).otherwise(lit(None)))
        .withColumn("live_cnt_6_12_bin",
                    when(col("live_cnt_6_12").isNull(), lit(None).cast(IntegerType()))
                    .when(col("live_cnt_6_12") < 0.60, lit(1))
                    .when(col("live_cnt_6_12") < 0.75, lit(2))
                    .when(col("live_cnt_6_12") < 1.29, lit(3))
                    .when(col("live_cnt_6_12") < 1.86, lit(4))
                    .otherwise(lit(5)))
        .withColumn("balance_amt_0_6_by_7_12",
                    when(col("_bal_7_12") > 0,
                         F.try_divide(col("_bal_0_6"), col("_bal_7_12")))
                    .otherwise(lit(None)))
        .drop("_bal_0_6","_bal_7_12","_cnt_0_6","_cnt_7_12","_bal_0_12_all","_bal_13_24_all")
    )

    # Delinquent balance ratios
    # FIX: custom_var.py sets delinq_bal=0 for ALL rows (dpd<=1 or closed → 0),
    # then groups by MOB and sums, then takes mean over all months in range.
    # This means the mean denominator = number of months with ANY account reporting,
    # NOT just months with delinq activity.
    # Our previous approach filtered to dpd>1 only → wrong smaller denominator.
    # Fix: use conditional expression — delinq_bal=balance if dpd>1 & not_closed else 0,
    # then sum per idx (across all accounts), then average over ALL months in range.
    dlq_bal_all = (
        exploded
        .groupBy("cust_id", "idx")
        .agg(
            fsum(when((col("dpd") > 1) & col("is_not_closed"), col("bal"))
                 .otherwise(lit(0.0))).alias("dlq_bal")
        )
    )

    dlq_ratio_df = (
        dlq_bal_all.groupBy("cust_id").agg(
            # custom_var MOB ranges: (0,12), (13,24), (25,36) inclusive
            favg(when(col("idx") <= 12, col("dlq_bal"))).alias("_dlq_0_12"),
            favg(when((col("idx") >= 13) & (col("idx") <= 24), col("dlq_bal"))).alias("_dlq_13_24"),
            favg(when((col("idx") >= 25) & (col("idx") <= 35), col("dlq_bal"))).alias("_dlq_25_36"),
        )
        .withColumn("_dlq_0_12",   F.coalesce(col("_dlq_0_12"),  lit(0.0)))
        .withColumn("_dlq_13_24",  F.coalesce(col("_dlq_13_24"), lit(0.0)))
        .withColumn("_dlq_25_36",  F.coalesce(col("_dlq_25_36"), lit(0.0)))
        .withColumn("dlq_bal_12_24",
                    when(col("_dlq_13_24") > 0,
                         F.try_divide(col("_dlq_0_12"), col("_dlq_13_24"))).otherwise(lit(None)))
        .withColumn("dlq_bal_24_36",
                    when(col("_dlq_25_36") > 0,
                         F.try_divide(col("_dlq_13_24"), col("_dlq_25_36"))).otherwise(lit(None)))
        .withColumn("dlq_bal_12_24_bin",
                    when(col("dlq_bal_12_24").isNull(), lit(None).cast(IntegerType()))
                    .when(col("dlq_bal_12_24") < 0.5,  lit(1))
                    .when(col("dlq_bal_12_24") < 1.0,  lit(2))
                    .when(col("dlq_bal_12_24") < 2.0,  lit(3))
                    .otherwise(lit(4)))
        .withColumn("dlq_bal_24_36_bin",
                    when(col("dlq_bal_24_36").isNull(), lit(None).cast(IntegerType()))
                    .when(col("dlq_bal_24_36") < 0.5,  lit(1))
                    .when(col("dlq_bal_24_36") < 1.0,  lit(2))
                    .when(col("dlq_bal_24_36") < 2.0,  lit(3))
                    .otherwise(lit(4)))
        .withColumn("delinq_bal_0_12_by_13_24",
                    when(col("_dlq_13_24") > 0,
                         F.try_divide(col("_dlq_0_12"), col("_dlq_13_24")))
                    .otherwise(lit(None)))
        .withColumn("delinq_bal_13_24_by_25_36",
                    when(col("_dlq_25_36") > 0,
                         F.try_divide(col("_dlq_13_24"), col("_dlq_25_36")))
                    .otherwise(lit(None)))
        .drop("_dlq_0_12","_dlq_13_24","_dlq_25_36")
    )

    # ── 4P. Frequency between account openings ────────────────────────────────
    # Java L2503: accountOpenDate = acctOpenDtSet.first() = EARLIEST open date per account.
    # Java L2546-2558: sort by (openDate ASC, typeCode_numeric ASC) for tie-breaking.
    # Java L2567-2574: compute gap[i] = diff(entry[i-1].date, entry[i].date) for ALL accounts.
    # Java L2578-2593: filter — include gap[i] only if entry[i].typeCode passes the filter.
    # BUG: we were filtering accounts BEFORE computing gaps. Gap for entry[i] should be
    # relative to entry[i-1] in the FULL sorted list, not just the filtered list.

    # Collect all accounts for freq_between using account_details (ad) which already has:
    # - account_type_cd from most-recent month (post _recent_flags join) — matches Java getAccountTypeCode()
    # - isUns, isCC from most-recent month flags — matches Java allUnSecProductCodes/allCCProductCodes
    # - _open_abs = absolute months of open_dt (fmax, used for MOB)
    # For freq_between, Java uses acctOpenDtSet.first() = EARLIEST open date.
    # Compute earliest open_abs per account from fact2_enriched.
    # FIX: Use open_dt_all (includes month_diff < 0 rows) for freq_between open_dt
    # Java: acctOpenDtSet built from ALL rows (index < 36 includes negatives)
    # Accounts ONLY in future months were excluded from our freq_between — now fixed
    # FIX 2: Also pass raw open_dt (yyyyMMdd) for ACCURATE chronological sort.
    # year*12+month loses day-precision causing wrong ordering when accounts open
    # in same month → 10 of 13 freq_between mismatches verified caused by this.
    if open_dt_all is not None:
        # Use unfiltered open_dt from all months
        _min_open_abs = (
            open_dt_all
            .withColumn("_min_open_abs",
                year(to_date(col("open_dt_all").cast("string"), "yyyyMMdd")) * 12 +
                month(to_date(col("open_dt_all").cast("string"), "yyyyMMdd")))
            .withColumnRenamed("open_dt_all", "_min_open_dt")
        )
    else:
        _min_open_abs = (
            fact2_enriched
            .groupBy("cust_id", "cons_acct_key")
            .agg(
                # FIX: skip open_dt=0 (NaN after fillna(0)) — same as primary path
                (year(to_date(fmin(when(col("open_dt") > 0, col("open_dt"))).cast("string"), "yyyyMMdd")) * 12 +
                 month(to_date(fmin(when(col("open_dt") > 0, col("open_dt"))).cast("string"), "yyyyMMdd"))).alias("_min_open_abs"),
                fmin(when(col("open_dt") > 0, col("open_dt"))).alias("_min_open_dt"),
            )
        )

    _freq_accounts = (
        ad.join(
            _min_open_abs.withColumnRenamed("cons_acct_key", "_acct_id"),
            ["cust_id", "_acct_id"], "left"
        )
        .select(
            "cust_id",
            col("_acct_id").alias("cons_acct_key"),  # for deterministic tie-break in sort
            col("account_type_cd").alias("tc"),   # most-recent month type (matches Java)
            col("_min_open_abs").alias("open_abs"), # earliest open year*12+month (used for diff calc)
            col("_min_open_dt").alias("open_dt"),  # raw yyyyMMdd (used for chronological sort)
            "isUns",  # most-recent month flag
            "isCC",   # most-recent month flag
        )
    )

    # Collect to pandas and compute freq_between with correct algorithm
    _freq_rows = _freq_accounts.rdd.collect()
    _freq_pdf = pd.DataFrame(_freq_rows, columns=["cust_id", "cons_acct_key", "tc", "open_abs", "open_dt", "isUns", "isCC"])

    # ──────────────────────────────────────────────────────────────────
    # DIAGNOSTIC: freq_between trace for target customers
    # ──────────────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("  DIAGNOSTIC: freq_between trace (target customers)")
    print("=" * 70)
    _freq_trace_targets = ["4607601", "48235409", "55932404", "53315751", "3054189",
                           "10004499",  # last is also for mon_since_max_bal
                           "2794019", "55186340", "60087778"]  # 3 freq_between bug customers
    _FREQ_CC_EXCL_TRACE = {"5","196","213","220","225"}
    _FREQ_EXCL_HL_TRACE = {"058", "195"}
    for _tc_id in _freq_trace_targets:
        _t = _freq_pdf[_freq_pdf["cust_id"].astype(str) == _tc_id].copy()
        if _t.empty:
            print(f"\n  --- {_tc_id}: NOT IN _freq_pdf ---")
            continue
        _t["open_dt"]  = pd.to_numeric(_t["open_dt"],  errors="coerce")
        _t["open_abs"] = pd.to_numeric(_t["open_abs"], errors="coerce")
        _t = _t.dropna(subset=["open_dt"])
        # Sort by exact open_dt for chronological accuracy (FIX)
        _t = _t.sort_values(["open_dt", "cons_acct_key"], kind="mergesort").reset_index(drop=True)
        print(f"\n  --- cust {_tc_id}: {len(_t)} accounts ---")
        print(f"  {'idx':>3}  {'cons_acct_key':<14} {'tc':>5} {'open_dt':>10} {'open_abs':>9} {'isUns':>5} {'isCC':>5} {'qual_uns_wo_cc':>15} {'qual_inst':>10}")
        for _i, _r in _t.iterrows():
            _tc_str = str(_r["tc"]).lstrip("0") or "0"
            _tc_pad = str(_r["tc"]).zfill(3) if _r["tc"] else ""
            _qual_unswc = bool(_r["isUns"]) and _tc_str not in _FREQ_CC_EXCL_TRACE
            _qual_inst = _tc_pad not in _FREQ_EXCL_HL_TRACE
            _od = "" if pd.isna(_r['open_dt'])  else int(_r['open_dt'])
            _oa = "" if pd.isna(_r['open_abs']) else int(_r['open_abs'])
            print(f"  {_i:>3}  {str(_r['cons_acct_key']):<14} {str(_r['tc']):>5} {str(_od):>10} {str(_oa):>9} "
                  f"{str(bool(_r['isUns'])):>5} {str(bool(_r['isCC'])):>5} "
                  f"{str(_qual_unswc):>15} {str(_qual_inst):>10}")
        # Compute gaps using open_abs (year*12+month) for diff calculation
        # Drop rows with NaN open_abs (invalid date)
        _t_valid = _t.dropna(subset=["open_abs"]).copy()
        _opens_abs = _t_valid["open_abs"].astype(int).tolist()
        _tcs = _t_valid["tc"].tolist()
        _isUns = _t_valid["isUns"].tolist()
        _gaps_all = []; _gaps_inst = []; _gaps_uns = []
        _gaps_uns_alt = []  # ALT: without 196 in excl list (test hypothesis)
        _FREQ_CC_EXCL_ALT = {"5","213","220","225"}  # CC_CODES = {5,213,220,225} (no 196)
        _gap_log = []
        for _i in range(1, len(_opens_abs)):
            _gap = _opens_abs[_i] - _opens_abs[_i-1]
            _gaps_all.append(_gap)
            _tc_pad = str(_tcs[_i]).zfill(3) if _tcs[_i] else ""
            _tc_str = str(_tcs[_i]).lstrip("0") or "0"
            _addinst = _tc_pad not in _FREQ_EXCL_HL_TRACE
            _adduns = bool(_isUns[_i]) and _tc_str not in _FREQ_CC_EXCL_TRACE
            _adduns_alt = bool(_isUns[_i]) and _tc_str not in _FREQ_CC_EXCL_ALT
            if _addinst: _gaps_inst.append(_gap)
            if _adduns: _gaps_uns.append(_gap)
            if _adduns_alt: _gaps_uns_alt.append(_gap)
            _gap_log.append((_i, _gap, _addinst, _adduns))
        print(f"  gaps_all  ({len(_gaps_all)}): {_gaps_all}")
        print(f"  gaps_inst ({len(_gaps_inst)}): {_gaps_inst}")
        print(f"  gaps_uns_wo_cc ({len(_gaps_uns)}): {_gaps_uns}")
        print(f"  gaps_uns_wo_cc_ALT (no 196) ({len(_gaps_uns_alt)}): {_gaps_uns_alt}")
        if _gaps_all:  print(f"  freq_between_accts_all          = {sum(_gaps_all)/len(_gaps_all):.4f}")
        if _gaps_inst: print(f"  freq_between_installment_trades = {sum(_gaps_inst)/len(_gaps_inst):.4f}")
        if _gaps_uns:  print(f"  freq_between_accts_unsec_wo_cc  = {sum(_gaps_uns)/len(_gaps_uns):.4f}")
        else:          print(f"  freq_between_accts_unsec_wo_cc  = None")
        if _gaps_uns_alt: print(f"  ALT (no 196)                    = {sum(_gaps_uns_alt)/len(_gaps_uns_alt):.4f}")
        else:             print(f"  ALT (no 196)                    = None")
    print("=" * 70)
    print()
    # ──────────────────────────────────────────────────────────────────

    # Java allUnSecProductCodes = Sec_Uns=="UnSec" in product_mapping = isUns flag
    # Java allCreditCardProductCodes = CC codes = isCC flag
    # Java excludeHL codes = {"058","195"} (padded 3 chars)
    _FREQ_EXCL_HL = {"058", "195"}

    def _compute_freq(cust_pdf):
        # JAVA-EXACT FIX: Sort by EXACT open_dt, then by typeCode AS INTEGER (not cons_acct_key).
        # Java bru.py L2546-2558: Comparator sorts by date, then numeric typeCode tiebreak:
        #   int typeCode1 = Integer.parseInt(e1.getKey().split("~")[1]);
        #   int typeCode2 = Integer.parseInt(e2.getKey().split("~")[1]);
        #   return Integer.compare(typeCode1, typeCode2);
        # Previous: tiebreak by cons_acct_key (string) — caused 3 customer mismatches:
        #   60087778: 1.4054→1.1622, 55186340: 2.4500→2.6000, 2794019: 2.0000→2.1667
        # All 3 fixed by switching to numeric typeCode tiebreak (verified offline).
        # Diff for each gap is still computed in months (year*12+month).
        try:
            cust_pdf = cust_pdf.copy()
            # Drop rows with null/NaN open_dt — Java skips invalid dates
            cust_pdf = cust_pdf.dropna(subset=["open_dt"])
            if cust_pdf.empty:
                return {"freq_between_accts_all": None,
                        "freq_between_installment_trades": None,
                        "freq_between_accts_unsec_wo_cc": None}
            cust_pdf["open_dt"]  = pd.to_numeric(cust_pdf["open_dt"],  errors="coerce")
            cust_pdf["open_abs"] = pd.to_numeric(cust_pdf["open_abs"], errors="coerce")
            # Drop rows with NaN in either — both needed (open_dt for sort, open_abs for diff)
            cust_pdf = cust_pdf.dropna(subset=["open_dt", "open_abs"])
            # Build numeric typeCode column for Java-style tiebreak
            def _tc_to_int(tc):
                try:
                    return int(str(tc).lstrip('0') or '0')
                except (ValueError, TypeError):
                    return 999
            cust_pdf["_tc_int"] = cust_pdf["tc"].apply(_tc_to_int)
            # Sort by raw open_dt (yyyyMMdd as int), then by typeCode AS INTEGER (Java rule)
            cust_pdf = cust_pdf.sort_values(
                ["open_dt", "_tc_int"], kind="mergesort").reset_index(drop=True)
        except Exception:
            cust_pdf = cust_pdf.sort_values("open_dt").reset_index(drop=True)

        opens_abs = cust_pdf["open_abs"].tolist()  # year*12+month for diff calc
        tcs   = cust_pdf["tc"].tolist()
        isUns = cust_pdf["isUns"].tolist()
        isCC  = cust_pdf["isCC"].tolist()
        n = len(opens_abs)

        gaps_all, gaps_inst, gaps_uns = [], [], []
        for i in range(1, n):
            try:
                # Diff in months = year_diff*12 + month_diff
                gap = int(float(opens_abs[i])) - int(float(opens_abs[i-1]))
            except (TypeError, ValueError):
                continue  # skip rows with invalid open_abs
            tc_padded = str(tcs[i]).zfill(3) if tcs[i] else ""

            # freq_between_accts_all: include all
            gaps_all.append(gap)

            # freq_between_installment_trades: exclude {058,195}
            if tc_padded not in _FREQ_EXCL_HL:
                gaps_inst.append(gap)

            # freq_between_accts_unsec_wo_cc: isUns (product_mapping UnSec) AND NOT in freq CC exclusion
            _FREQ_CC_EXCL = {"5","196","213","220","225"}
            tc_stripped = str(tcs[i]).lstrip('0') or '0'
            if bool(isUns[i]) and tc_stripped not in _FREQ_CC_EXCL:
                gaps_uns.append(gap)

        return {
            "freq_between_accts_all":         sum(gaps_all)/len(gaps_all)   if gaps_all  else None,
            "freq_between_installment_trades": sum(gaps_inst)/len(gaps_inst) if gaps_inst else None,
            "freq_between_accts_unsec_wo_cc":  sum(gaps_uns)/len(gaps_uns)   if gaps_uns  else None,
        }

    _freq_results = []
    for cust_id, grp in _freq_pdf.groupby("cust_id"):
        res = _compute_freq(grp)
        res["cust_id"] = int(cust_id)
        _freq_results.append(res)

    # Guard against empty results (e.g. when fact2_enriched has no accounts).
    # pd.DataFrame([]) has zero columns → col-selection raises KeyError.
    _freq_cols = ["cust_id", "freq_between_accts_all",
                  "freq_between_installment_trades",
                  "freq_between_accts_unsec_wo_cc"]
    if _freq_results:
        _freq_result_pdf = pd.DataFrame(_freq_results)
        for _c in _freq_cols:
            if _c not in _freq_result_pdf.columns:
                _freq_result_pdf[_c] = None
        _freq_result_pdf = _freq_result_pdf[_freq_cols]
        _freq_result_pdf["cust_id"] = pd.to_numeric(_freq_result_pdf["cust_id"], errors="coerce").astype("int64")
    else:
        _freq_result_pdf = pd.DataFrame(columns=_freq_cols)
        _freq_result_pdf["cust_id"] = _freq_result_pdf["cust_id"].astype("int64")

    _freq_schema = StructType([
        StructField("cust_id",                        LongType(),   False),
        StructField("freq_between_accts_all",          DoubleType(), True),
        StructField("freq_between_installment_trades", DoubleType(), True),
        StructField("freq_between_accts_unsec_wo_cc",  DoubleType(), True),
    ])

    freq_df = spark.createDataFrame(
        spark.sparkContext.parallelize([tuple(r) for r in _freq_result_pdf.itertuples(index=False)]),
        schema=_freq_schema
    )

    # Remove old mob_all_df, mob_uns_df, mob_inst_df, _ad_freq — replaced above


    # ── 4Q. Recency & DPD recency binning ────────────────────────────────────
    recency_df = (
        dpd_attrs.select("cust_id","min_mon_sin_recent_1")
        .withColumn("min_mon_sin_recent_1_bin",
                    when(col("min_mon_sin_recent_1").isNull(), lit(None).cast(IntegerType()))
                    .when(col("min_mon_sin_recent_1") < 2,  lit(1))
                    .when(col("min_mon_sin_recent_1") < 10, lit(2))
                    .otherwise(lit(3)))
        .drop("min_mon_sin_recent_1")   # already present from dpd_attrs join
    )

    # mon_since_first_worst_delq_bin: cascade fill from worst DPD level down
    worst_delq_bin = (
        exploded
        .groupBy("cust_id").agg(
            fmax(when(col("dpd") >= 7, col("idx"))).alias("_mon_first_7"),
            fmax(when(col("dpd") >= 6, col("idx"))).alias("_mon_first_6"),
            fmax(when(col("dpd") >= 5, col("idx"))).alias("_mon_first_5"),
            fmax(when(col("dpd") >= 4, col("idx"))).alias("_mon_first_4"),
            fmax(when(col("dpd") >= 3, col("idx"))).alias("_mon_first_3"),
            fmax(when(col("dpd") >= 2, col("idx"))).alias("_mon_first_2"),
            fmax(when(col("dpd") >= 1, col("idx"))).alias("_mon_first_1"),
        )
        .withColumn("mon_since_first_worst_delq_bin",
                    coalesce("_mon_first_7","_mon_first_6","_mon_first_5",
                             "_mon_first_4","_mon_first_3","_mon_first_2","_mon_first_1"))
        .drop("_mon_first_7","_mon_first_6","_mon_first_5",
              "_mon_first_4","_mon_first_3","_mon_first_2","_mon_first_1")
    )

    # mon_since_recent_x_bin: months since most recent 1-30 DPD (dpd == 1)
    mon_recent_x = (
        exploded.filter(col("dpd") == 1)
        .groupBy("cust_id")
        .agg(fmin("idx").alias("mon_since_recent_x"))
        .withColumn("mon_since_recent_x_bin",
                    when(col("mon_since_recent_x").isNull(), lit(None).cast(IntegerType()))
                    .when(col("mon_since_recent_x") < 2,  lit(1))
                    .when(col("mon_since_recent_x") < 10, lit(2))
                    .otherwise(lit(3)))
    )

    # ── 4R. CC utilisation count (≥40%) ──────────────────────────────────────
    # GENERIC FIX: use _cons_key to separate accounts when accno==cust_id
    cc40_df = (
        exploded
        .filter((col("isCC") | (col("account_type_cd")=="220")) & (col("mod_lim") > 0) & (col("idx") <= 15))
        .withColumn("util", F.try_divide(col("bal"), col("mod_lim")))
        .groupBy("cust_id", "_cons_key")
        .agg(favg("util").alias("avg_util_16m"))
        .filter(col("avg_util_16m") > 0.40)
        .groupBy("cust_id")
        .agg(count("_cons_key").alias("nbr_cc4016m_tot_accts_36"))
    )

    # nbr_cc40l6m: CC accounts with avg util > 40% in last 6 months
    # FIX 17: GT returns 0 for ALL customers (including non-CC customers), not NaN.
    # The right-join on _cc40_all only covers customers WITH CC accounts.
    # Non-CC customers get NaN after the join but GT expects 0.
    # Solution: join all customers from acct_counts, then fillna(0).
    _cc40_all = (
        exploded
        .filter(col("isCC") & (col("mod_lim") > 0) & (col("idx") <= 5))
        .groupBy("cust_id")
        .agg(countDistinct("_cons_key").alias("_total_cc_l6m"))
    )
    # FIX: Java calculateNbrCc40L6mTotAcctsBin (bru.py L2153):
    # util = sum(bal for idx 0..5) / (accountMaxLimit * nbrMonthsReported)
    # This is ratio-of-sums with ACCOUNT-LEVEL max limit × count_reported.
    # Our old code used favg(bal/mod_lim) = avg-of-ratios — WRONG.
    # Also Java has NO isLiveAccount filter — counts ALL CC accounts.
    # FIX C: nbr_cc40l6m — Java L2183 uses accountLevelDetailsV4.getModifiedLimit()
    # = MAX limit across ALL 36 months (set by updateAccountLevelDetails running max).
    # Our old code: fmax("mod_lim") within idx 0..5 only → under-counts high limits
    # from older months → artificially high util → CODE_HIGH over-count.
    # Fix: join account-level latest_modified_limit (already fmax all months from Fix 2).
    _cc40_acct_lim = ad.select("cust_id", col("_acct_id").alias("_cons_key"),
                                col("latest_modified_limit").alias("_acct_max_lim"))
    # FIX: Only customers with CC accounts (any idx) should be in output.
    # Customers with NO CC accounts → NaN (Java). Customers with CC accounts but none
    # in recent 6 months, or none with util>40% → 0 (Java).
    # Base: ALL CC holders (not filtered to idx<=5), so 7 customers whose CC
    # accounts are older than 6 months still appear in output with count=0.
    _cc_holders_any = (
        exploded
        .filter(col("isCC"))
        .select("cust_id").distinct()
    )
    cc40_6m_df = (
        exploded
        .filter(col("isCC") & (col("idx") <= 5))
        .groupBy("cust_id", "_cons_key")
        .agg(
            fsum("bal").alias("_sum_bal_l6m"),
            fsum(lit(1)).alias("_n_reported"),  # ALL months idx 0..5
        )
        .join(_cc40_acct_lim, ["cust_id", "_cons_key"], "left")
        .withColumn("_eff_lim",
                    when(col("_acct_max_lim") > 0, col("_acct_max_lim") * col("_n_reported"))
                    .otherwise(lit(0.001)))
        .withColumn("_util_l6m", F.try_divide(col("_sum_bal_l6m"), col("_eff_lim")))
        .filter((col("_util_l6m") > 0.40) & (col("_n_reported") > 0))
        .groupBy("cust_id")
        .agg(count("_cons_key").alias("_cc40_count"))
        # Right-join to CC holders (any idx) base: holders without qualifying count → 0,
        # non-CC-holders absent → NaN via final left-join in global_attrs.
        .join(_cc_holders_any, "cust_id", "right")
        .withColumn("nbr_cc40l6m_tot_accts_36",
                    coalesce(col("_cc40_count"), lit(0)))
        .select("cust_id", "nbr_cc40l6m_tot_accts_36")
    )

    # ── 4T. NEW ATTRIBUTES (15 columns) ─────────────────────────────────────

    # --- agri_comuns_live ---------------------------------------------------
    # Java: if(nbr_live_agri>0 && nbr_live_comuns>0) → 4
    #       if(nbr_live_agri>0 && nbr_live_comuns==0) → 3
    #       if(nbr_live_agri==0 && nbr_live_comuns>0) → 2  else → 1
    agri_comuns_live_df = (
        acct_counts.select(
            "cust_id",
            col("nbr_agri_live_accts_36").alias("_agri_live"),
        )
        .join(
            ad.groupBy("cust_id").agg(
                fsum((col("isComUnSec") & col("isLiveAccount")).cast("int")).alias("_comuns_live")
            ),
            "cust_id", "left"
        )
        .withColumn("agri_comuns_live",
            when((col("_agri_live") > 0) & (col("_comuns_live") > 0), lit(4))
            .when((col("_agri_live") > 0) & (col("_comuns_live") == 0), lit(3))
            .when((col("_agri_live") == 0) & (col("_comuns_live") > 0), lit(2))
            .otherwise(lit(1)))
        .drop("_agri_live", "_comuns_live")
    )

    # --- max_dpd_sec0_live --------------------------------------------------
    # Java: max dpd_new on Secured accounts at index==0 only
    # FIX 13: Removed the isLiveAccount filter (all Sec accounts at idx=0 included).
    # FIX 13b: Use effective_dpd (raw_dpd fallback) so low-balance accounts are counted.
    max_dpd_sec0_live_df = (
        exploded
        .filter(col("isSec") & (col("idx") == 0))
        .groupBy("cust_id")
        .agg(fmax(
            when(col("dpd") > 0, col("dpd"))
            .when(col("raw_dpd") > 0, lit(1))
            .otherwise(lit(0))
        ).alias("max_dpd_sec0_live"))
    )

    # --- nbr_0_0m_all + nbr_0_0m_all_bin ------------------------------------
    # Java: count accounts where dpd>0 at index==0 (current month)
    nbr_0_0m_df = (
        exploded
        .filter(col("idx") == 0)
        .groupBy("cust_id").agg(
            fsum(when(col("dpd") > 0, lit(1))).alias("nbr_0_0m_all")
        )
        .withColumn("nbr_0_0m_all_bin",
            when(col("nbr_0_0m_all").isNull(), lit(None).cast(IntegerType()))
            .when(col("nbr_0_0m_all") == 0, lit(1))
            .when(col("nbr_0_0m_all") == 1, lit(2))
            .when(col("nbr_0_0m_all") <= 3, lit(3))
            .otherwise(lit(4)))
    )

    # --- nbr_0_24m_live -----------------------------------------------------
    # Count month-rows on LIVE accounts where any DPD > 0 in last 24m.
    # FIX 11: Use _cons_key for the join (not accno which may equal cust_id).
    # FIX raw_dpd: use raw_dpd ONLY as fallback when dpd==0 (balance<=500 guard zeroed it).
    # Previous: (dpd>0)|(raw_dpd>0) double-counted months where both were >0.
    live_accno_df = ad.select("cust_id", col("_acct_id").alias("_cons_key"), "isLiveAccount")
    _dpd_live = (
        exploded
        .filter(col("idx") < 24)
        .join(live_accno_df, ["cust_id", "_cons_key"], "left")
        .filter(col("isLiveAccount") == True)
    )
    _live_cust = _dpd_live.select("cust_id").distinct()
    nbr_0_24m_live_df = (
        _dpd_live
        .groupBy("cust_id")
        .agg(
            # Java: counts months where dpd > 0 on live accounts (no raw_dpd fallback)
            fsum(when(col("dpd") > 0, lit(1)).otherwise(lit(0))).alias("nbr_0_24m_live")
        )
        .join(_live_cust, "cust_id", "right")
        .fillna(0, subset=["nbr_0_24m_live"])
    )

    # --- max_nbr_0_24m_uns --------------------------------------------------
    # Java: per unsec account count months with dpd>0 in last 24m → max across accounts
    # GENERIC FIX: use _cons_key to count per-account DPD months correctly
    max_nbr_0_24m_uns_df = (
        exploded
        .filter(col("isUns") & (col("idx") < 24))
        .groupBy("cust_id", "_cons_key")
        .agg(fsum((col("dpd") > 0).cast("int")).alias("_cnt_0_months"))
        .groupBy("cust_id")
        .agg(fmax("_cnt_0_months").alias("max_nbr_0_24m_uns"))
    )

    # --- mon_since_max_bal_124m_uns ------------------------------------------
    # Java: on non-derog unsec accounts, find month index of max balance in last 24m
    # Uses m_since_max_bal_l24m from account_details (pre-computed per account)
    mon_since_max_bal_df = (
        ad.filter(col("isUns") & ~col("derog"))
        .groupBy("cust_id")
        .agg((fmin(when(col("m_since_max_bal_l24m").isNotNull(),
                       col("m_since_max_bal_l24m"))) + lit(1)).alias("mon_since_max_bal_124m_uns"))
    )

    # ──────────────────────────────────────────────────────────────────
    # DIAGNOSTIC: mon_since_max_bal_l24m_uns trace (target customers)
    # Print per-account m_since_max_bal_l24m for customers with mismatches.
    # User: 10004499 (off by 5: ref=8, code=13), 4384851 (off by 1), 59973892 (off by 1)
    # ──────────────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("  DIAGNOSTIC: mon_since_max_bal_l24m_uns trace")
    print("=" * 70)
    _msmb_targets = ["10004499", "4384851", "59973892"]
    for _t_id in _msmb_targets:
        _r = (ad.filter(col("cust_id").cast("string") == _t_id)
                .select("cust_id", "_acct_id", "account_type_cd", "isUns", "isCC",
                        "derog", "m_since_max_bal_l24m")
                .collect())
        if not _r:
            print(f"\n  --- {_t_id}: NOT IN account_details ---")
            continue
        print(f"\n  --- cust {_t_id}: {len(_r)} accounts in account_details ---")
        print(f"  {'_acct_id':<14} {'tc':>5} {'isUns':>5} {'isCC':>5} {'derog':>5} {'m_since_max_bal_l24m':>22}")
        _qual_vals = []
        for _row in _r:
            _msmb = _row["m_since_max_bal_l24m"]
            _qual = bool(_row["isUns"]) and not bool(_row["derog"])
            _qual_str = "QUAL" if _qual else ""
            print(f"  {str(_row['_acct_id']):<14} {str(_row['account_type_cd']):>5} "
                  f"{str(bool(_row['isUns'])):>5} {str(bool(_row['isCC'])):>5} "
                  f"{str(bool(_row['derog'])):>5} {str(_msmb):>22}  {_qual_str}")
            if _qual and _msmb is not None:
                _qual_vals.append(_msmb)
        if _qual_vals:
            print(f"  qualifying m_since_max_bal_l24m values: {_qual_vals}")
            print(f"  min(qual) + 1 = {min(_qual_vals) + 1}  (this is what we output)")
        else:
            print(f"  No qualifying values → mon_since_max_bal_l24m_uns = NaN")
    print("=" * 70)
    print()
    # ──────────────────────────────────────────────────────────────────

    # --- min_mob_uns_exc_cc -------------------------------------------------
    # Chargen: min MOB on unsecured non-CC accounts
    min_mob_uns_exc_cc_df = (
        ad.filter(col("isUns") & ~col("isCC"))
        .groupBy("cust_id")
        .agg(fmin("mob").alias("min_mob_uns_exc_cc"))
    )

    # --- nbr_pl_le50_tot_accts_36 -------------------------------------------
    # Chargen: count PL accounts where modified_limit <= 50,000.
    # Uses latest_modified_limit (pre-aggregated in account_details per account).
    # 
    # FIX: Previous impl used .filter(...).groupBy().count() which DROPS customers
    # who have PL accounts but none <=50k — they got NaN when they should get 0.
    # 
    # Now: start from ad (all customers with any account), conditional sum.
    # - Customers with NO type 123/242 accounts → excluded (NaN after left join — matches Java)
    # - Customers with type 123/242 accounts but none <=50k → 0 (matches Java)
    # - Customers with type 123/242 accounts and some <=50k → actual count
    nbr_pl_le50_df = (
        ad.filter(col("account_type_cd").isin("123","242") & col("reportedIn36M"))
        .groupBy("cust_id")
        .agg(fsum(when(col("latest_modified_limit") <= 50000, lit(1))
                 .otherwise(lit(0))).alias("nbr_pl_le50_tot_accts_36"))
    )

    # --- nbr_agri_tot_accts_36_diff -----------------------------------------
    # Chargen: change in agri account count: 36m count minus 12m count
    nbr_agri_diff_df = (
        acct_counts.select("cust_id","nbr_agri_tot_accts_36")
        .join(
            ad.groupBy("cust_id").agg(
                fsum((col("isAgri") & col("reportedIn12M")).cast("int")).alias("_nbr_agri_12m")
            ),
            "cust_id", "left"
        )
        .withColumn("nbr_agri_tot_accts_36_diff",
                    col("nbr_agri_tot_accts_36") - coalesce(col("_nbr_agri_12m"), lit(0)))
        .drop("nbr_agri_tot_accts_36","_nbr_agri_12m")
    )

    # --- bal_amt_12_24 -------------------------------------------------------
    # Java: avg_bal(months 0-12) / avg_bal(months 13-24) ratio
    # Reuse live_bal (computed above in 4O) — build fresh window aggs
    bal_amt_12_24_df = (
        live_bal.groupBy("cust_id").agg(
            favg(when((col("idx") >= 0)  & (col("idx") <= 11), col("sum_bal"))).alias("_b_0_12"),
            favg(when((col("idx") >= 13) & (col("idx") <= 24), col("sum_bal"))).alias("_b_13_24"),
        )
        .withColumn("bal_amt_12_24",
                    when(col("_b_13_24") > 0,
                         F.try_divide(col("_b_0_12"), col("_b_13_24")))
                    .otherwise(lit(None)))
        .drop("_b_0_12","_b_13_24")
    )

    # --- outflow_bin ---------------------------------------------------------
    # Java: median EMI excl CC → binned
    outflow_bin_df = (
        total_outflow_wo_cc
        .withColumn("outflow_bin",
            when(col("total_outflow_wo_cc").isNull(), lit(None).cast(IntegerType()))
            .when(col("total_outflow_wo_cc") <= 0,      lit(1))
            .when(col("total_outflow_wo_cc") <= 5000,   lit(2))
            .when(col("total_outflow_wo_cc") <= 15000,  lit(3))
            .when(col("total_outflow_wo_cc") <= 30000,  lit(4))
            .when(col("total_outflow_wo_cc") <= 60000,  lit(5))
            .otherwise(lit(6)))
        .select("cust_id","outflow_bin")
    )

    # --- latest_account_type + product_holding4 -----------------------------
    # Java: account_type_cd of account with max open_date
    # product_holding4: bins latest_account_type into 3 groups
    # Sec/CC/OD/KCC → bin1="CC | OD | KCC | Secured products"
    # PL/CD/TW      → bin2="PL |CD |TW"
    # else           → "Other Products"

    # Sec product codes (from Java getAllSecProductCodes):
    # HL=049, LAP=052, 096,097,098,099,180,181, AL=058, GL/KCC=082/083/145/146
    # CC=051, CD=058(PL codes differ) — use isCC / isSec flags from account_details
    # product_holding4 bin1: isSec OR isCC  → "CC | OD | KCC | Secured products"
    # product_holding4 bin2: isPL OR isTW   → "PL |CD |TW"
    # else                                  → "Other Products"


    # Row with highest open_dt = most recently opened account
    latest_acct_type_df = (
        ad.select("cust_id","_acct_id","open_dt","account_type_cd","isSec","isCC","isPL","isTW")
        .withColumn("_rank", F.row_number().over(
            Window.partitionBy("cust_id").orderBy(col("open_dt").cast("long").desc())))
        .filter(col("_rank") == 1)
        .withColumn("latest_account_type", col("account_type_cd"))
        .withColumn("product_holding4",
            when(col("isSec") | col("isCC"), lit("CC | OD | KCC | Secured products"))
            .when(col("isPL")  | col("isTW"),  lit("PL |CD |TW"))
            .otherwise(lit("Other Products")))
        .select("cust_id","latest_account_type","product_holding4")
    )


    # --- totconsinc_util1_tot_7m --------------------------------------------
    # Java: aggregate balance per month-index (0-6) across all accounts,
    # then compute util = tot_bal/tot_lim per index,
    # count consecutive months where util increased by >1%
    # totconsinc_util1_tot_7m: Java uses account's fixed modifiedLimit (max across months)
    # not the per-month mod_lim which may vary. Use fmax(mod_lim) per account per idx,
    # then sum across accounts.
    # _util7m: ALL accounts (incl zero-limit), fill missing idx slots with 0 balance/util
    # Java: builds util[0..6] for ALL accounts, missing idx → 0 balance / 0.001 limit
    # Java: mod1Tot7m uses accountLevelDetailsV4.getModifiedLimit() = account-level MAX
    # = same limit for every month. Get per-account max first, then join.
    _acct_max_lim = (
        exploded
        .groupBy("cust_id", "_cons_key")
        .agg(fmax(coalesce(col("mod_lim"), lit(0.001))).alias("_acct_max_lim"))
    )
    _util7m_per_acct = (
        exploded
        .filter(col("idx") < 7)
        .join(_acct_max_lim, ["cust_id", "_cons_key"], "left")
        .groupBy("cust_id", "_cons_key", "idx")
        .agg(
            fsum("bal").alias("_acct_bal"),
            fmax("_acct_max_lim").alias("_acct_lim"),
        )
    )
    # Java acctBalMap: only actual reported months — no fill
    _util7m = (
        _util7m_per_acct.groupBy("cust_id","idx").agg(
            fsum("_acct_bal").alias("_tot_bal"),
            fsum("_acct_lim").alias("_tot_lim"),
        )
        .withColumn("_util_idx", F.try_divide(col("_tot_bal"), col("_tot_lim")))
    )

    _util7m_schema = StructType([
        StructField("cust_id",               LongType(),   nullable=False),
        StructField("totconsinc_util1_tot_7m", IntegerType(), nullable=True),
    ])

    def compute_util7m_udf(pdf):
        # org_car: compare util[m] (newer=lower month_diff) vs util[m+1] (older=higher month_diff)
        # util[m] > util[m+1] + 0.01 → utilisation INCREASED recently → count
        # month_diff=1=most recent, month_diff=6=6 months ago
        results = []
        for cust_id, grp in pdf.groupby("cust_id"):
            grp = grp.sort_values("idx")
            utils = dict(zip(grp["idx"], grp["_util_idx"]))
            consec_count = 0
            # org_car: compares util at month m (more recent) vs month m+1 (older).
            # range(1, 7): m=1 → compare idx1 vs idx2, ..., m=6 → compare idx6 vs idx7.
            # This is correct — idx=0 is included implicitly because idx=1 is the start.
            # range(0, 6) was tested but broke cust 10004499 (reverted).
            for m in range(0, 6):   # 6 pairs: (0,1),(1,2),(2,3),(3,4),(4,5),(5,6)
                u_newer = utils.get(m)      # more recent (lower month_diff)
                u_older = utils.get(m + 1)  # one month older
                # Java: calculateTotalConcPercIncreaseFlags = count ALL months where increase > 1%
                # NOT max consecutive — no break on failure
                # Java: relative % change > 1% (not absolute > 0.01)
                if u_newer is not None and u_older is not None and u_older > 0:
                    if (u_newer - u_older) / u_older > 0.01:
                        consec_count += 1
            results.append({"cust_id": int(cust_id), "totconsinc_util1_tot_7m": consec_count})
        return pd.DataFrame(results, columns=["cust_id","totconsinc_util1_tot_7m"])

    # RDD collect → pandas groupby → RDD parallelize (Java 25 Hadoop workaround)
    _util7m_rows = _util7m.rdd.collect()
    _util7m_pdf  = pd.DataFrame(_util7m_rows, columns=["cust_id","idx","_tot_bal","_tot_lim","_util_idx"])
    if len(_util7m_pdf) > 0:
        _util7m_parts  = [compute_util7m_udf(grp) for _, grp in _util7m_pdf.groupby("cust_id")]
        _util7m_result = pd.concat(_util7m_parts, ignore_index=True)
    else:
        _util7m_result = pd.DataFrame(columns=["cust_id","totconsinc_util1_tot_7m"])
    totconsinc_util1_df = spark.createDataFrame(
        spark.sparkContext.parallelize([tuple(r) for r in _util7m_result.itertuples(index=False)]),
        schema=_util7m_schema)

    # --- consinc_bal5_cc_13m ------------------------------------------------
    # Fix 2: Java L1952-1975: sums ALL CC accounts' balances per month index,
    # THEN runs calculateConcPercIncreaseFlags (max consecutive streak) on that
    # customer-level summed series. We were computing per-account then taking max — wrong.
    # Java: balInc[index] += acctBal across all CC accounts. Then max consec on balInc.
    # Also: Java gate: if no CC account exists → MISSING (not 0).

    # Step 1: sum all CC account balances per (cust_id, month_diff)
    _cc_bal13_summed = (
        exploded
        .filter(col("isCC") & (col("idx") < 13))
        .groupBy("cust_id", col("idx").alias("month_diff"))
        .agg(fsum("bal").alias("cc_tot_bal"))
    )

    _cc_bal13_schema = StructType([
        StructField("cust_id",             LongType(),   nullable=False),
        StructField("consinc_bal5_cc_13m", IntegerType(), nullable=True),
    ])

    def compute_consinc_bal5_cc(pdf):
        # Java calculateConcPercIncreaseFlags (L4458-4488):
        # For index 0..12 (indexLimit=13): current=bal[idx], previous=bal[idx+1]
        # pct = (current - previous) / previous if previous > 0, else DEFAULT_DOUBLE_VALUE
        # Count MAX CONSECUTIVE run where pct > Percentage (0.05) AND pct > DEFAULT (-999)
        results = []
        for cust_id, grp in pdf.groupby("cust_id"):
            bal_map = dict(zip(grp["month_diff"], grp["cc_tot_bal"]))
            max_cons = 0
            cons = 0
            for idx in range(13):   # Java: for index=0; index < 13
                current  = bal_map.get(idx,   0)
                previous = bal_map.get(idx+1, 0)
                if previous > 0:
                    pct = (current - previous) / previous
                    if pct > 0.05:   # Java: > Percentage (strictly greater)
                        cons += 1
                        if cons > max_cons:
                            max_cons = cons
                    else:
                        cons = 0     # reset but continue
                else:
                    cons = 0         # DEFAULT_DOUBLE_VALUE → not > 0.05
            results.append({"cust_id": int(cust_id), "consinc_bal5_cc_13m": max_cons})
        return pd.DataFrame(results, columns=["cust_id", "consinc_bal5_cc_13m"])

    # Step 2: collect and compute
    _cc_rows = _cc_bal13_summed.rdd.collect()
    _cc_pdf  = pd.DataFrame(_cc_rows, columns=["cust_id", "month_diff", "cc_tot_bal"])

    # Customers with CC accounts → compute max consecutive
    if len(_cc_pdf) > 0:
        _cc_parts  = [compute_consinc_bal5_cc(grp)
                      for _, grp in _cc_pdf.groupby("cust_id")]
        _cc_result = pd.concat(_cc_parts, ignore_index=True)
    else:
        _cc_result = pd.DataFrame(columns=["cust_id", "consinc_bal5_cc_13m"])

    # Customers with NO CC → MISSING (Java L1979-1980: !isCCAcct → MISSING)
    # Customers with CC accounts (at any idx, not just idx<13) → 0 if no recent data
    # Build base from ad filtered to CC holders, then left-join computed values
    consinc_cc_computed = spark.createDataFrame(
        spark.sparkContext.parallelize([tuple(r) for r in _cc_result.itertuples(index=False)])
        if len(_cc_result) > 0 else spark.sparkContext.parallelize([], 1),
        schema=_cc_bal13_schema)
    _cc_holders = (
        ad.filter(col("isCC"))
        .select("cust_id").distinct()
        .withColumn("cust_id", col("cust_id").cast(LongType()))
    )
    # Customers with CC but no recent bal data → fill 0 (Java: computed=0, not MISSING)
    consinc_cc_df = (
        _cc_holders.join(consinc_cc_computed, "cust_id", "left")
        .withColumn("consinc_bal5_cc_13m",
                    F.coalesce(col("consinc_bal5_cc_13m"), lit(0)))
    )

    # New consinc_bal5_cc_13m logic above produces consinc_cc_df (correct summed approach)
    # Map to the output name expected by the join chain
    consinc_bal5_cc_df = consinc_cc_df

    # sum_sanc_amt_uns — sum of max mod_lim across ALL unsec accounts (live AND closed)
    # org_car: sum_sanc_amt_uns = grp_nansum(modified_limit * cond['uns']) — NO live filter.
    # Confirmed: ref=370000 = 318000 (live 531045921) + 52000 (closed 220732246) ✓
    # GENERIC FIX: group by _cons_key (cons_acct_key) not accno.
    # In datasets where accno == cust_id, groupBy(cust_id,accno) collapses all accounts
    # into one group → fmax picks max across ALL accounts, not per account.
    # sum_sanc_amt_uns: all isUns accounts (CD included)
    # Note: CD exclusion caused ratio to flip below 1.0 — reverted.
    # sum_sanc_amt_uns: use isUns (product_mapping based) NOT _CV_UNS
    # _CV_UNS misses types 244,245,247,249-252 which ARE in product_mapping UnSec
    # Java L1557: getAllUnSecProductCodes() = full product_mapping UnSec set
    sum_sanc_uns_df = (
        exploded
        .filter(col("isUns"))
        .groupBy("cust_id", "_cons_key")
        .agg(fmax("mod_lim").alias("_max_lim_uns_acct"))
        .groupBy("cust_id")
        .agg(fsum("_max_lim_uns_acct").alias("sum_sanc_amt_uns"))
    )

    # ── 4S. Join all attribute groups ────────────────────────────────────────
    global_attrs = (
        acct_counts
        .join(accts_opened,          "cust_id", "left")
        .join(_open_cnt_df,           "cust_id", "left")
        .join(dpd_attrs,             "cust_id", "left")
        .join(util_all_df,           "cust_id", "left")
        .join(util_uns_df,           "cust_id", "left")
        .join(util_exc_cc_df,        "cust_id", "left")
        .join(util_cc_df,            "cust_id", "left")
        .join(exp_df,                "cust_id", "left")
        .drop("max_sanc_amt_sec", "max_sanc_amt_secmov_real")       # remove m0 versions if present
        .join(max_sanc_amt_sec_df,   "cust_id", "left")            # add 36m-secured max
        .join(sum_sanc_uns_df,        "cust_id", "left")
        .join(trend_df,              "cust_id", "left")
        .join(outflow_df,            "cust_id", "left")
        .join(outflow_plcdtw_df,     "cust_id", "left")
        .join(total_outflow_wo_cc,   "cust_id", "left")
        .join(max_lim_df,            "cust_id", "left")
        .join(derog_months,          "cust_id", "left")
        .join(max_bal_df,            "cust_id", "left")
        .join(rpt_df,                "cust_id", "left")
        .join(bank_df,               "cust_id", "left")
        .join(consec_df,             "cust_id", "left")
        .join(simul_df,              "cust_id", "left")
        .join(simul_gl_df,           "cust_id", "left")
        .join(simul_plcd_df,         "cust_id", "left")
        .join(bal_ratio_df,          "cust_id", "left")
        .join(dlq_ratio_df,          "cust_id", "left")
        .join(freq_df,               "cust_id", "left")
        .join(recency_df,            "cust_id", "left")
        .join(worst_delq_bin,        "cust_id", "left")
        .join(mon_recent_x,          "cust_id", "left")
        .join(cc40_df,               "cust_id", "left")
        # ── NEW 15 COLUMNS ────────────────────────────────────────────────────
        .join(agri_bal_ratio_df,     "cust_id", "left")
        .join(consinc_all7_df,       "cust_id", "left")
        .join(outflow_hl_lap_df,     "cust_id", "left")
        .join(outflow_al_tw_df,      "cust_id", "left")
        .join(outflow_pl_df,         "cust_id", "left")
        .join(outflow_cd_df,         "cust_id", "left")
        .join(simul_uns_all_df,      "cust_id", "left")
        .join(cc40_6m_df,            "cust_id", "left")
        .join(agri_comuns_live_df,   "cust_id", "left")
        .join(max_dpd_sec0_live_df,  "cust_id", "left")
        .join(nbr_0_0m_df,           "cust_id", "left")
        .join(nbr_0_24m_live_df,     "cust_id", "left")
        .join(max_nbr_0_24m_uns_df,  "cust_id", "left")
        .join(mon_since_max_bal_df,  "cust_id", "left")
        .join(min_mob_uns_exc_cc_df, "cust_id", "left")
        .join(nbr_pl_le50_df,        "cust_id", "left")
        .join(nbr_agri_diff_df,      "cust_id", "left")
        .join(bal_amt_12_24_df,      "cust_id", "left")
        .join(outflow_bin_df,        "cust_id", "left")
        .join(latest_acct_type_df,   "cust_id", "left")
        .join(totconsinc_util1_df,   "cust_id", "left")
        .join(consinc_bal5_cc_df,    "cust_id", "left")
        .join(consinc_bal10_exc_cc_df, "cust_id", "left")   # FIX 10: consecutive exc-CC bal increase
    )

    # ── Zero-fill: Java returns 0 when no qualifying accounts ────────────────
    # FIX (prior session): Removed nbr_pl_le50_tot_accts_36, consinc_bal5_cc_13m,
    # consinc_bal10_exc_cc_25m from zero-fill.
    # FIX (this session): max_dpd_UNS_L6_M / max_dpd_UNS_6_12_M now removed too.
    # Previous removal caused 130 regressions because the underlying fmax(when dpd>0)
    # returned NULL for BOTH cases: "no UnSec" AND "UnSec but no dpd>0".
    # Now the fmax is NaN-preserving: NULL only when no UnSec rows in window,
    # 0 when UnSec exists but no DPD event. So zero-fill is no longer needed.
    _zero_fill_cols = [
        "HL_LAP_outflow", "AL_TW_outflow", "PL_outflow", "CD_outflow",
        "Outflow_uns_secmov", "Outflow_AL_PL_TW_CD",
        "max_dpd_uns_l36m",
        "dlq_bal_12_24", "dlq_bal_24_36",
    ]
    for _c in _zero_fill_cols:
        if _c in global_attrs.columns:
            global_attrs = global_attrs.withColumn(_c, F.coalesce(col(_c), lit(0.0)))

    # ── Derived flags ─────────────────────────────────────────────────────────
    global_attrs = (
        global_attrs
        .withColumn("all_accts_al_cc_hl",
                    (col("nbr_not_al_cc_hl") == 0))
        .withColumn("has_agri_or_com",
                    (col("nbr_agri_tot_accts_36") > 0) | (col("nbr_comsec_tot_accts_36") > 0))

        .withColumn("open_cnt_0_6_by_7_12_bin",
            # FIX: GT returns 0 when nbr_accts_open_l6m=0 but 7-12m accounts exist.
            # Old: .otherwise(lit(None)) → Code=NaN for 22 customers.
            # New: coalesce to 0 when denominator>0 (no 0-6m opens, but 7-12m exists).
            when(col("_open_cnt_7to12m") > 0,
                    coalesce(F.try_divide(col("_open_cnt_l6m"), col("_open_cnt_7to12m")), lit(0.0)))
            .otherwise(lit(None).cast(DoubleType())))
        .withColumn("ratio_nbr_cc4016m_accts_36",
                    when(col("nbr_cc_tot_accts_36") > 0,
                         coalesce(col("nbr_cc4016m_tot_accts_36"), lit(0)) /
                         col("nbr_cc_tot_accts_36"))
                    .otherwise(lit(0.0)))
        .withColumn("leverage_indicator",
                    when(col("curr_tot_sanc_amt") > 0,
                         F.try_divide(col("curr_tot_exp_amt"), col("curr_tot_sanc_amt")))
                    .otherwise(lit(None).cast(DoubleType())))
        # Fill missing sentinel
        # .fillna(-999, subset=[
        #     "max_dpd_uns_l36m","max_dpd_uns_l12m","max_dpd_UNS_L6_M","max_dpd_UNS_6_12_M",
        #     "max_dpd_cc_l36m","max_dpd_hl_l36m","max_dpd_al_l36m",
        #     "util_l3m_cc_live","util_l6m_cc_live","util_l12m_cc_live",
        #     "util_l3m_uns_tot","util_l6m_uns_tot","util_l12m_uns_tot",
        #     "Outflow_uns_secmov","Outflow_AL_PL_TW_CD",
        #     "mon_since_first_worst_delq","mon_since_recent_worst_delq",
        #     "final_consec_marker",
        #     "max_simul_unsec_wo_cc","max_simul_unsec_wo_cc_inc_gl",
        # ])
        .fillna(-999, subset=[
            "mon_since_first_worst_delq",
            "mon_since_recent_worst_delq",
            "final_consec_marker",
        ])
        .fillna(0, subset=[
            "nbr_cc4016m_tot_accts_36",
        ])
    )

    exploded.unpersist()

    # Deduplicate columns in global_attrs after all joins.
    # CRITICAL: Cannot use select() on a DF with duplicate column names — Spark throws
    # AMBIGUOUS_REFERENCE. The only safe method is toDF() which operates by position,
    # not by name, so it never triggers the ambiguity resolver.
    # Strategy: build a new name list where duplicates get a sentinel suffix '__DROP__',
    # then toDF() renames all columns by position, then select() drops the sentinel ones.
    # keep=LAST: the last occurrence of each name is the correctly computed value
    # (right-side of join chain = specific sub-DataFrame = correct).
    _seen_ga = {}   # name → last position seen
    for _i, _c in enumerate(global_attrs.columns):
        _seen_ga[_c] = _i   # last occurrence wins
    _new_names_ga = [
        _c if _seen_ga[_c] == _i else f"__DROP__{_i}__"
        for _i, _c in enumerate(global_attrs.columns)
    ]
    if any(n.startswith("__DROP__") for n in _new_names_ga):
        _n_dropped = sum(1 for n in _new_names_ga if n.startswith("__DROP__"))
        print(f"  [INFO] Deduplicating global_attrs (keep=last): removing {_n_dropped} stale copies")
        global_attrs = global_attrs.toDF(*_new_names_ga)
        _keep_cols = [c for c in global_attrs.columns if not c.startswith("__DROP__")]
        global_attrs = global_attrs.select(_keep_cols)

    return global_attrs


# ===========================================================================
# PHASE 5 — SCORECARD TRIGGER & FINAL SCORE
# ===========================================================================


def compute_trigger_eligibility(account_details):
    ad = account_details

    # Exact codes from Java: StringUtils.leftPad(woStatus, 3, '0')
    # So "7" → "007", "2" → "002" etc. Stored as float in CSV → cast+lpad needed
    # Java ST3 wo list: 002,003,004,006,008,009,013,014,015,016,017
    # Java ST2 wo list: same
    derog_wo_st3 = {"002","003","004","006","008","009","013","014","015","016","017"}
    derog_wo_st2 = {"002","003","004","006","008","009","013","014","015","016","017"}

    exp_monthly = (
        ad.select("cust_id","accno","isLiveAccount",
                  F.explode("monthly_data").alias("m"))
        .select("cust_id","accno","isLiveAccount",
                col("m.idx").alias("idx"),
                col("m.dpd").alias("dpd"),           # dpd_new bucket (0-7)
                col("m.raw_dpd").alias("raw_dpd"),   # raw dayspastdue for eligibility
                col("m.bal").alias("bal"),
                col("m.asset_cd").alias("asset_cd"),
                col("m.wo_status").alias("wo_status"))
        # Normalise wo_status: float "7.0" → "007", null → null
        .withColumn("wo_norm",
            when(col("wo_status").isNull() | (col("wo_status") == ""), lit(None))
            .otherwise(
                F.lpad(
                    F.regexp_replace(col("wo_status").cast("string"), r"\.0$", ""),
                    3, "0"
                )
            ))
    )

    # ST3: idx==0, not closed (isLiveAccount), bal>500, dpd>=91 OR asset L/D/B OR wo in list
    # Java: closed_flag_1 == "N" maps to isLiveAccount in our pipeline
    st3_eligible = (
        exp_monthly
        .filter(
            (col("idx") == 0) & col("isLiveAccount") & (col("bal") > 500) &
            ((col("raw_dpd") >= 91) |                    # raw DPD days, not bucket
             col("asset_cd").isin("L","D","B") |
             col("wo_norm").isin(*derog_wo_st3))
        )
        .groupBy("cust_id").agg(lit(True).alias("is_eligible_for_st3"))
    )

    # ST2: idx<=12, not closed, bal>500, dpd>30 OR asset L/D/B/M OR wo in list
    st2_eligible = (
        exp_monthly
        .filter(
            (col("idx") <= 12) & col("isLiveAccount") & (col("bal") > 500) &
            ((col("raw_dpd") > 30) |                     # raw DPD days, not bucket
             col("asset_cd").isin("L","D","B","M") |
             col("wo_norm").isin(*derog_wo_st2))
        )
        .groupBy("cust_id").agg(lit(True).alias("is_eligible_for_st2"))
    )

    return st3_eligible, st2_eligible


def apply_score_trigger(global_attrs, st3_eligible, st2_eligible):
    ga = (
        global_attrs
        .join(st3_eligible, "cust_id", "left")
        .join(st2_eligible, "cust_id", "left")
        .fillna(False, subset=["is_eligible_for_st3","is_eligible_for_st2"])
    )
    scorecard_name = (
        when(col("nbr_live_accts_36") <= 0,          lit("CLOSED"))
        .when(col("is_eligible_for_st3"),             lit("ST_3"))
        .when(col("max_mob_all_36") <= 6,             lit("THIN"))
        .when(col("is_eligible_for_st2"),             lit("ST_2"))
        .when(
            (col("max_simul_unsec_wo_cc") >= 7) |
            (col("nbr_accts_open_l6m") >= 4) |
            (col("max_simul_unsec_wo_cc_inc_gl") >= 8),
            lit("ST_1_HC"))
        .when(col("all_accts_al_cc_hl"),              lit("ST_1_EV"))
        .when(col("has_agri_or_com"),                 lit("ST_1_AGR_OR_COM"))
        .otherwise(lit("ST_1_SE"))
    )
    return ga.withColumn("scorecard_name", scorecard_name)


SCORECARD_COEFFICIENTS = {
    "ST_1_HC": {
        "intercept":                   500.0,
        "max_dpd_uns_l36m":            -25.0,
        "util_l3m_cc_live":            -50.0,
        "nbr_accts_open_l6m":          -15.0,
        "totconsinc_bal10_exc_cc_25m": -10.0,
        "Outflow_uns_secmov":            0.001,
        "max_mob_all_36":                1.5,
        "avg_bal_l3m_all":             -0.00002,
        "final_consec_marker":         -30.0,
    },
    "ST_1_EV": {
        "intercept":           550.0,
        "max_dpd_hl_l36m":     -30.0,
        "max_dpd_cc_l36m":     -20.0,
        "util_l3m_cc_live":    -40.0,
        "max_mob_all_36":        2.0,
        "final_consec_marker": -25.0,
    },
    "ST_1_AGR_OR_COM": {
        "intercept":             480.0,
        "max_dpd_uns_l36m":      -20.0,
        "nbr_agri_tot_accts_36":   5.0,
        "max_mob_all_36":          1.0,
    },
    "ST_1_SE": {
        "intercept":             520.0,
        "max_dpd_uns_l36m":      -22.0,
        "util_l6m_cc_live":      -35.0,
        "Outflow_uns_secmov":      0.0008,
        "max_mob_all_36":          1.8,
        "final_consec_marker":   -20.0,
    },
    "ST_2": {
        "intercept":           350.0,
        "max_dpd_uns_l12m":    -30.0,
        "nbr_30_24m_all":      -20.0,
        "final_consec_marker": -40.0,
    },
    "ST_3": {
        "intercept":           250.0,
        "max_dpd_uns_l36m":    -40.0,
        "final_consec_marker": -50.0,
    },
    "THIN": {
        "intercept":        400.0,
        "nbr_tot_accts_36":  10.0,
        "max_mob_all_36":     5.0,
    },
    "CLOSED": {
        "intercept": 300.0,
    },
}

# sc_coeff_bc is initialised inside compute_final_score() to ensure SparkContext is ready.


def compute_final_score(global_attrs_with_trigger):
    """
    Compute final credit score — fully distributed Spark row-level UDFs.
    No rdd.collect(), no driver-side pandas loop.
    Scorecard coefficients broadcast to all workers via sc_coeff_bc.
    """
    ga = global_attrs_with_trigger
    # Broadcast inside function — guarantees SparkContext is fully initialised
    sc_coeff_bc = spark.sparkContext.broadcast(SCORECARD_COEFFICIENTS)

    # Collect all attribute names used across every scorecard (excluding 'intercept')
    _score_attr_names = sorted({
        attr
        for coeff in SCORECARD_COEFFICIENTS.values()
        for attr in coeff
        if attr != "intercept"
    })

    # ── Single UDF returns (breakdown_str, raw_score) — guarantees consistency ─
    # Using one UDF prevents floating-point divergence between breakdown sum and
    # final_score that occurred when two separate UDFs computed independently.
    from pyspark.sql.types import StructType as _ST, StructField as _SF
    _score_result_schema = _ST([
        _SF("score_breakdown", StringType(),  nullable=True),
        _SF("raw_score",       DoubleType(),  nullable=True),
    ])

    @udf(_score_result_schema)
    def _score_udf(scorecard_name, *attr_vals):
        coeffs   = sc_coeff_bc.value
        sc_name  = scorecard_name or "ST_1_SE"
        coeff    = coeffs.get(sc_name, coeffs["ST_1_SE"])
        attr_map = dict(zip(_score_attr_names, attr_vals))
        if sc_name == "CLOSED":
            intercept = float(coeff.get("intercept", 300))
            return ("CLOSED_NO_HIT", intercept)
        score = coeff.get("intercept", 500.0)
        parts = [f"intercept={score:.2f}"]
        for attr, w in coeff.items():
            if attr == "intercept":
                continue
            val = attr_map.get(attr)
            if val is not None:
                try:
                    fv = float(val)
                    if fv != fv: continue           # NaN check
                    effective = 0.0 if fv == -999.0 else fv
                    contrib   = effective * w
                    score    += contrib
                    parts.append(f"{attr}={contrib:.2f}")
                except (TypeError, ValueError):
                    pass
        return ("|".join(parts), score)

    # Build UDF args: scorecard_name + each score attribute cast to double
    _udf_cols = [col("scorecard_name")] + [
        coalesce(col(a).cast("double"), lit(None).cast("double"))
        if a in ga.columns else lit(None).cast("double")
        for a in _score_attr_names
    ]

    return (
        ga
        .withColumn("_score_result",  _score_udf(*_udf_cols))
        .withColumn("score_breakdown", col("_score_result.score_breakdown"))
        .withColumn("final_score",
                    F.greatest(lit(300), F.least(lit(900),
                        col("_score_result.raw_score").cast(IntegerType()))).cast(IntegerType()))
        .drop("_score_result")
    )


# ===========================================================================
# PHASE 6 — PIPELINE ORCHESTRATOR
# ===========================================================================
# cols = [
#     "accno",
#     "cons_acct_key",
#     "open_dt",
#     "acct_nb",
#     "closed_dt",
#     "bureau_mbr_id",
#     "account_type_cd",
#     "term_freq",
#     "charge_off_amt",
#     "resp_code",
#     "balance_dt",
#     "account_status_cd",
#     "actual_pymnt_amt",
#     "balance_amt",
#     "credit_lim_amt",
#     "original_loan_amt",
#     "past_due_amt",
#     "pay_rating_cd",
#     "dayspastdue",
#     "written_off_amt",
#     "principal_written_off",
#     "SUITFILED_WILFULDEFAULT",
#     "SUITFILEDWILLFULDEFAULT",
#     "WRITTEN_OFF_AND_SETTLED"
# ]




# cols = ["accno","cons_acct_key","open_dt","acct_nb","closed_dt","bureau_mbr_id","account_type_cd","term_freq"]

# trade_df.select(cols).show(truncate=False)
# fact2.select(cols).show(truncate=False)

# cols = ["accno","cust_id","account_type_cd","balance_amt","credit_lim_amt","original_loan_amt","past_due_amt","dayspastdue","account_status_cd","dpd_new"]

    # Fix open_dt
def clean_numeric_date(colname):
    return F.format_string("%.0f", F.col(colname).cast("double"))


def run_pipeline(trade_df, acct_map, prod_map, bank_map, output_dir=".", include_all_columns: bool = True):
    # ── VERSION BANNER & DATA DIAGNOSTICS ─────────────────────────────────────
    from pyspark.sql.functions import col as _col, count as _count, countDistinct as _cdist
    print()
    import datetime as _dt
    _build_id = "v3.9-" + _dt.datetime.now().strftime("%H%M%S")
    print("#" * 75)
    print("#" + " " * 73 + "#")
    print(f"#   BUILD: con_v4 {_build_id:<12} (timestamp injected at runtime)        #")
    print(f"#   This number should be DIFFERENT every run.                           #")
    print("#   If you see the same number twice, you're not running this file.       #")
    print("#" + " " * 73 + "#")
    print("#" * 75)
    # Expected row/account counts derived from the uploaded Excel trade files.
    # Pipeline accuracy for these customers depends on having ALL these rows.
    _expected_counts = {
        # trade_selcted1.xlsx customers
        "2027158":  (615, 25), "2643104":  (149, 6),  "2722571":  (97, 6),
        "28083426": (201, 15), "30365098": (98, 3),   "3707450":  (155, 5),
        "38609792": (585, 93), "53315751": (337, 19), "9414008":  (168, 7),
        # trade_ltr.xlsx customers
        "10005593": (278, 17), "31968676": (128, 5),  "4650448":  (80, 3),
        "54467239": (107, 5),  "5859799":  (328, 14), "6749573":  (126, 5),
        "9941726":  (47, 3),
    }
    _target_custs = list(_expected_counts.keys())
    _row_counts = (
        trade_df
        .filter(_col("accno").cast("string").isin(_target_custs))
        .groupBy(_col("accno").cast("string").alias("accno_str"))
        .agg(_count("*").alias("n_rows"),
             _cdist("cons_acct_key").alias("n_accts"))
        .collect()
    )
    _found = {r["accno_str"]: (r["n_rows"], r["n_accts"]) for r in _row_counts}
    print("  Expected vs actual row counts in trade_df:")
    print(f"  {'Customer':<12} {'Exp rows':>9} {'Got rows':>9} {'Exp acc':>8} {'Got acc':>8}  Status")
    _ok = _partial = _missing = 0
    for c in _target_custs:
        exp_r, exp_a = _expected_counts[c]
        if c in _found:
            got_r, got_a = _found[c]
            if got_r == exp_r and got_a == exp_a:
                status = "\u2713 OK"; _ok += 1
            else:
                pct = 100 * got_r // exp_r if exp_r else 0
                status = f"\u26a0 PARTIAL ({pct}%)"; _partial += 1
            print(f"  {c:<12} {exp_r:>9} {got_r:>9} {exp_a:>8} {got_a:>8}  {status}")
        else:
            _missing += 1
            print(f"  {c:<12} {exp_r:>9}         - {exp_a:>8}        -  \u2717 MISSING")
    print("  " + "-" * 70)
    print(f"  Summary: {_ok} exact | {_partial} partial | {_missing} missing  (target {len(_target_custs)})")
    if _partial > 0 or _missing > 0:
        print()
        print("  " + "!" * 70)
        print("  !!! trade_data.csv does NOT have full rows for some target customers.")
        print("  !!! Expected counts above = what's in the uploaded Excel trade files.")
        print("  !!! To fix these mismatches, your trade_data.csv must include the")
        print("  !!! missing rows for each customer shown as PARTIAL or MISSING.")
        print("  " + "!" * 70)
    print("#" * 75)
    print()

    from pyspark.sql.functions import when, col, concat, lit, substring


    trade_df = (
        trade_df
        .withColumn("open_dt",   clean_numeric_date("open_dt"))
        .withColumn("closed_dt", clean_numeric_date("closed_dt"))
        .withColumn("balance_dt", clean_numeric_date("balance_dt"))
    )
    
    fact2 = build_fact2(trade_df, acct_map, prod_map, bank_map)
    print(f"    fact2 rows: {fact2.count():,}")

    # Pre-compute open_dt from ALL months (including month_diff < 0, future rows)
    # Java: acctOpenDtSet includes rows with index < 36 (includes negatives)
    # Accounts ONLY reported in future months (month_diff < 0) must be included
    # in freq_between's open_dt list. fact2 filters those out, so we compute
    # the open_dt lookup separately from the raw unfiltered trade data.
    _open_dt_all = (
        trade_df
        .join(acct_map.filter(col("relFinCd").cast("string").isin(*OWNERSHIP_CD))
              .select("accno", "cust_id"), on="accno", how="inner")
        .groupBy("cust_id", "cons_acct_key")
        # FIX: exclude open_dt=0 (which represents NaN after the int64 fillna(0) in load_inputs).
        # Otherwise fmin returns 0 for accounts with mixed valid/NaN rows, causing those
        # accounts to be dropped from freq_between (open_abs computed from "0" → NaN).
        # E.g. account 2935944163 has open_dt=20200820 (33 rows) + NaN (2 rows) → fmin=0 BAD.
        .agg(fmin(when(col("open_dt") > 0, col("open_dt"))).alias("open_dt_all"))
    )

    print("--- Phase 2: Per-Month Variables ---")
    fact2_enriched = build_fact2_enriched(fact2)
    fact2_enriched.cache()

    print("--- Phase 3: Per-Account Aggregation ---")
    account_details = build_account_details(fact2_enriched)
    account_details.cache()
    print(f"    account rows: {account_details.count():,}")

    print("--- Phase 4: Global Attribute Calculation ---")
    global_attrs = build_global_attrs(account_details, fact2_enriched, _open_dt_all)

    # Java calculateUtilL6mAllTot: NO isLiveAccount filter for util_l6m_uns_tot
    _CV_UNS_UTIL = {5,121,123,130,167,169,170,176,177,178,179,181,187,189,196,197,198,199,200,
                    213,214,215,216,217,224,225,226,227,228,242,999}  # FIX 4: added 242 to match _CV_UNS
    _util6m_override = (
        fact2_enriched
        # Fix: use isUns (product_mapping) not _CV_UNS_UTIL — includes 244,245,247 etc.
        .filter(col("isUns") & (col("modified_limit") > 0))
        .groupBy("cust_id", "cons_acct_key")
        .agg(
            fsum(when(col("month_diff") <= 5, col("balance_amt"))).alias("_u6_bal"),
            fsum(when(col("month_diff") <= 5, lit(1))).alias("_u6_cnt"),
            fmax("modified_limit").alias("_u6_lim"),
        )
        .withColumn("_u6_eff", when(col("_u6_lim") > 0, col("_u6_lim") * col("_u6_cnt")).otherwise(lit(0.001)))
        .groupBy("cust_id")
        .agg(
            F.try_divide(fsum(when(col("_u6_cnt") > 0, col("_u6_bal"))),
                         fsum(when(col("_u6_cnt") > 0, col("_u6_eff")))).alias("util_l6m_uns_tot_new")
        )
    )
    global_attrs = (
        global_attrs
        .join(_util6m_override, "cust_id", "left")
        .withColumn("util_l6m_uns_tot",
                    F.coalesce(col("util_l6m_uns_tot_new"), col("util_l6m_uns_tot")))
        .drop("util_l6m_uns_tot_new")
    )

    # ── Phase 5 (Score functions commented out — score columns excluded from output) ──
    # Uncomment the block below if scorecard_name / final_score / score_breakdown
    # / is_eligible_for_st2 / is_eligible_for_st3 are needed in the output.
    #
    # print("--- Phase 5A: Trigger Eligibility ---")
    # st3_eligible, st2_eligible = compute_trigger_eligibility(account_details)
    #
    # print("--- Phase 5B: Scorecard Selection ---")
    # global_attrs_triggered = apply_score_trigger(global_attrs, st3_eligible, st2_eligible)
    #
    # print("--- Phase 5C: Final Score ---")
    # final_df = compute_final_score(global_attrs_triggered)

    # Use global_attrs directly as final_df (no scoring applied)
    final_df = global_attrs

    print("--- Phase 5C done: unpersisting cached data to free memory ---")
    # FIX OOM: free cached intermediate DataFrames before collecting final result.
    # Keeping fact2_enriched + account_details cached while collecting final_df
    # causes both datasets to compete for heap with the result materialisation.
    try:
        fact2_enriched.unpersist()
        account_details.unpersist()
        print("  Unpersisted fact2_enriched and account_details")
    except Exception:
        pass

    print("--- Phase 6: Output ---")
    # ── Score-only columns (always output) ──────────────────────────────────
    _SCORE_ONLY_COLS = [
        "cust_id",
        "scorecard_name",
        "final_score",
        "score_breakdown",
        "is_eligible_for_st2",
        "is_eligible_for_st3",
    ]

    # ── All detail columns (only when include_all_columns=True) ──────────────
    _ALL_DETAIL_COLS = [
        "cust_id",
        "scorecard_name", "final_score", "score_breakdown",
        "is_eligible_for_st2", "is_eligible_for_st3",
        # ── Account counts ───────────────────────────────────────────────────
        "max_mob_all_36",        "nbr_tot_accts_36",         "nbr_live_accts_36",
        "nbr_al_tot_accts_36",   "nbr_hl_tot_accts_36",      "nbr_cc_tot_accts_36",
        "nbr_cc_live_accts_36",  "nbr_agri_tot_accts_36",    "nbr_comsec_tot_accts_36",
        "nbr_comuns_tot_accts_36","nbr_pl_tot_accts_36",     "nbr_tw_tot_accts_36",
        "nbr_cl_tot_accts_36",   "nbr_pl_le50_tot_accts_36",
        # ── MOB ──────────────────────────────────────────────────────────────
        "min_mob_all_36",        "avg_mob_reg_36",            "max_mob_cc_36",
        "min_mob_uns_wo_cc_36",  "min_mob_agri_live_36",      "max_mob_comuns_live_36",
        "mon_since_last_acct_open",
        # ── Simultaneous ─────────────────────────────────────────────────────
        "max_simul_unsec_wo_cc", "max_simul_unsec_wo_cc_inc_gl",
        "max_simul_unsec",       "max_simul_pl_cd",
        # ── DPD (final names — rename_map applied to final_df before select) ──
        "max_dpd__UNS_L36M",     "max_dpd_L30M",
        "max_dpd__UNS__L6M",     "max_dpd__UNS_L_6_12_M",    "max_dpd__SEC__0_live",
        # ── DPD bucket timing ────────────────────────────────────────────────
        "mon_sin_recent_1",      "mon_sin_recent_2",          "mon_sin_recent_3",
        "mon_sin_recent_4",      "mon_sin_recent_5",          "mon_sin_recent_6",
        "mon_sin_recent_7",
        "mon_sin_first_1",       "mon_sin_first_2",           "mon_sin_first_3",
        "mon_sin_first_4",       "mon_sin_first_5",           "mon_sin_first_6",
        "mon_sin_first_7",
        # ── Utilisation ──────────────────────────────────────────────────────
        "util_l3m_cc_live",      "util_l6m_all_tot",          "util_l12m_all_tot",
        "avg_util_l6m_all_tot",
        # util_l3m_uns_tot REMOVED: Column Audit confirms NOT in reference schema (94/101 Ref=NaN)
        "util_l6m_uns_tot",      "util_l12m_uns_tot",
        "util_l3m_exc_cc_live",
        "nbr_cc40l6m_tot_accts_36",
        # ── Sanction amounts (final post-rename names) ────────────────────────
        "max_sanc_amt",          "max_sanc_amt_sec",          "max_sanc_amt_secmov",
        "max_sanc_amt_pl",       "max_sanc_amt_al",
        "max_sanc_amt_tw",       "max_sanc_amt_cl",
        "sum_sanc_amt_uns",
        # ── Outflows (final post-rename names) — total_monthly_outflow_wo_cc EXCLUDED (not in reference) ──
        "HL_LAP_outflow",        "AL_TW_outflow",
        "PL_outflow",            "CD_outflow",
        # ── Balance trends ───────────────────────────────────────────────────
        "totconsinc_bal10_exc_cc_25m",  "totconsdec_bal_tot_36m",
        "totconsinc_bal10_exc_cc_7m",   "totconsinc_bal5_exc_cc_7m",
        "consinc_bal10_exc_cc_25m",     "consinc_bal10_tot_7m",
        "totconsinc_util1_tot_7m",      "consinc_bal5_cc_13m",
        # ── Balance ratios (final post-rename names) ─────────────────────────
        "balance_amt_0_12_by_13_24", "balance_amt_0_6_by_7_12",  "avg_b_1to6_by_7_12_agri",
        "delinq_bal_0_12_by_13_24",     "delinq_bal_13_24_by_25_36",
        "live_cnt_0_6_by_7_12",
        # ── Frequency ────────────────────────────────────────────────────────
        "freq_between_accts_all","freq_between_accts_unsec_wo_cc",
        "freq_between_installment_trades",
        # ── DPD counts & timing ──────────────────────────────────────────────
        "nbr_0_0m_all",          "nbr_0_24m_live",            "max_nbr_0_24m_uns",
        "mon_since_max_bal_l24m_uns",
        # ── Account opening velocity (final post-rename) ──────────────────────
        "nbr_accts_open_l6m",    "nbr_accts_open_7to12m",    "nbr_accts_open_l12m_wo_cc",
        "open_cnt_0_6_by_7_12",
        # ── Latest account ───────────────────────────────────────────────────
        "latest_account_type",
    ]

    # ── Output: full detail columns excluding score fields ───────────────────
    # Score-only mode (commented out — use _SCORE_ONLY_COLS if needed):
    # output_cols = _SCORE_ONLY_COLS

    _SCORE_COLS = {"scorecard_name","final_score","score_breakdown",
                   "is_eligible_for_st2","is_eligible_for_st3"}

    # ── Apply rename_map on final_df BEFORE select ────────────────────────────
    # withColumnRenamed is safe ONLY if the DF has no duplicate column names already.
    # _ga_dedup above ensures global_attrs is clean. The _util6m_override join in
    # run_pipeline adds only util_l6m_uns_tot_new (unique), so final_df is also clean.
    # Simple withColumnRenamed is therefore safe here — no toDF needed.
    rename_map = {
        "max_dpd_uns_l36m":           "max_dpd__UNS_L36M",
        "max_dpd_l30m":               "max_dpd_L30M",
        "max_dpd_UNS_L6_M":           "max_dpd__UNS__L6M",
        "max_dpd_UNS_6_12_M":         "max_dpd__UNS_L_6_12_M",
        "max_dpd_sec0_live":          "max_dpd__SEC__0_live",
        "max_sanc_amt_ever":          "max_sanc_amt",
        "max_sanc_amt_secmov_real":   "max_sanc_amt_secmov",
        "total_outflow_wo_cc":        "total_monthly_outflow_wo_cc",
        "live_cnt_6_12":              "live_cnt_0_6_by_7_12",
        "open_cnt_0_6_by_7_12_bin":   "open_cnt_0_6_by_7_12",
        # REMOVED: "bal_amt_12_24" → "balance_amt_0_12_by_13_24" rename was OVERWRITING
        # the correctly-computed balance_amt_0_12_by_13_24 from bal_ratio_df (L2344) with
        # the wrong bal_amt_12_24_df value (L2839 uses idx 0..11 instead of 0..12, different
        # data source). Diagnostic proved bal_ratio_df produces 1.6536 for 9414008 but the
        # rename overwrote it with 0.331. Keep the correct one directly.
        "mon_since_max_bal_124m_uns": "mon_since_max_bal_l24m_uns",
    }

    for old_name, new_name in rename_map.items():
        if old_name in final_df.columns:
            final_df = final_df.withColumnRenamed(old_name, new_name)

    # Safety dedup after rename using toDF positional approach (keep=last).
    # withColumnRenamed can create a duplicate if the target name already existed.
    _seen_r = {}
    for _i, _c in enumerate(final_df.columns):
        _seen_r[_c] = _i
    _new_names_r = [
        _c if _seen_r[_c] == _i else f"__DROP__{_i}__"
        for _i, _c in enumerate(final_df.columns)
    ]
    if any(n.startswith("__DROP__") for n in _new_names_r):
        _n = sum(1 for n in _new_names_r if n.startswith("__DROP__"))
        print(f"  [INFO] Post-rename dedup (keep=last): removing {_n} stale copies")
        final_df = final_df.toDF(*_new_names_r)
        final_df = final_df.select([c for c in final_df.columns if not c.startswith("__DROP__")])

    # _ALL_DETAIL_COLS uses final output names (post-rename)
    output_cols = [c for c in _ALL_DETAIL_COLS if c not in _SCORE_COLS]
    result = final_df.select([c for c in output_cols if c in final_df.columns])

    # FIX OOM: checkpoint breaks the deep query plan lineage from 30+ joins.
    # Without this, Spark re-evaluates the entire chain on collect → heap exhaustion.
    try:
        import tempfile as _tmp, os as _os
        _ckpt_dir = _os.path.join(_os.path.expanduser("~"), "spark_checkpoints")
        _os.makedirs(_ckpt_dir, exist_ok=True)
        spark.sparkContext.setCheckpointDir(_ckpt_dir)
        result = result.checkpoint(eager=True)
        print(f"  Checkpointed result to {_ckpt_dir}")
    except Exception as _ce:
        print(f"  [WARN] Checkpoint failed ({_ce}), continuing without")


    # FIX OOM: coalesce to single partition before collect.
    result = result.coalesce(1)

    result.printSchema()

    # ── Output: RDD collect → pandas → CSV (Java 25 local workaround) ──────────
    # Spark's write.csv() calls Hadoop Subject.getSubject() which crashes on
    # Java 25 in local mode — same root cause as the original pd.read_csv workaround.
    # Solution: collect result rows to driver via RDD, write with pandas.
    # This is safe because the result has one row per customer (not per trade row).
    # For EMR/S3: switch to result.coalesce(1).write.option("header","true").csv(path)
    # FIX 7: Use a datetime-based timestamp so each run produces a unique file
    # instead of overwriting scorecard_output_to_3.csv every time.
    import datetime as _dt
    _ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    flat_csv = os.path.join(output_dir, f"scorecard{_ts}.csv")
    _out_cols = result.columns

    # Safe collect: Arrow is disabled (crashes on Java 25).
    # rdd.collect() works because: (a) result has only 1 row per customer,
    # (b) checkpoint above broke the deep 30-join lineage so Spark doesn't
    # re-materialise the whole plan during collect.
    _out_rows = result.rdd.collect()

    # ── DEFINITIVE DUPLICATE FIX ─────────────────────────────────────────────
    # Spark rdd.collect() returns rows; result.columns may have 91 duplicate names
    # from the multi-join lineage. We deduplicate _out_cols by position BEFORE
    # creating the DataFrame. Keep=LAST position: the last (rightmost) occurrence
    # of each column name is the correctly computed value from the most-recent join.
    # This guarantees the CSV is always written with unique column names.
    _raw_cols = list(result.columns)
    _seen_pos = {}
    for _i, _c in enumerate(_raw_cols):
        _seen_pos[_c] = _i           # last position wins
    _keep_positions = sorted(set(_seen_pos.values()))
    _out_cols = [_raw_cols[_i] for _i in _keep_positions]
    _out_rows_dedup = [[row[_i] for _i in _keep_positions] for row in _out_rows]

    _n_dropped = len(_raw_cols) - len(_out_cols)
    if _n_dropped > 0:
        print(f"  [FIX] Removed {_n_dropped} duplicate columns (keep=last). "
              f"Output: {len(_out_cols)} unique columns.")

    pdf = pd.DataFrame(_out_rows_dedup, columns=_out_cols)

    # Safety check — should always be 0 after positional dedup above
    _dup_count = len(pdf.columns) - len(set(pdf.columns))
    if _dup_count > 0:
        print(f"  [WARN] {_dup_count} dups remain after positional dedup — applying pandas fallback.")
        pdf = pdf.loc[:, ~pdf.columns.duplicated(keep='last')]

    pdf.to_csv(flat_csv, index=False)
    print(f"\n  Results saved to: {flat_csv}")

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 300)
    print("\n--- Sample results (first 20 rows) ---")
    _disp_cols = [c for c in ["cust_id","scorecard_name","final_score","score_breakdown"]
                  if c in pdf.columns]
    print(pdf[_disp_cols].head(20).to_string(index=False))

    print("\n--- Scorecard distribution ---")
    if "scorecard_name" in pdf.columns:
        print(pdf.groupby("scorecard_name")["cust_id"].count().reset_index()
                .rename(columns={"cust_id":"count"}).to_string(index=False))
    else:
        print("  (scorecard_name not in output)")

    return result


# ===========================================================================
# MAIN — reads four separate input files
# ===========================================================================

if __name__ == "__main__":
    import sys

    # ── Argument handling ─────────────────────────────────────────────────────
    # Usage:
    #   python consumer_scorecard_v4_final.py \
    #     <trade_csv> <account_mapping_csv> <product_mapping_csv> <bank_mapping_csv> \
    #     [output_dir]
    #
    # Defaults to the sample files in the same directory.
    # ─────────────────────────────────────────────────────────────────────────
    # Update base path to the directory containing your new dataset files:
    #   trade_data.csv, account_mapping.csv, product_mapping.csv, bank_mapping.csv


    base = "/Users/shubham/bru/files/input3"
    TRADE_CSV       = sys.argv[1] if len(sys.argv) > 1 else os.path.join(base, "trade_data.csv")
    ACCT_MAP_CSV    = sys.argv[2] if len(sys.argv) > 2 else os.path.join(base, "account_mapping.csv")
    PROD_MAP_CSV    = sys.argv[3] if len(sys.argv) > 3 else os.path.join(base, "product_mapping.csv")
    BANK_MAP_CSV    = sys.argv[4] if len(sys.argv) > 4 else os.path.join(base, "bank_mapping.csv")
    OUTPUT_DIR      = sys.argv[5] if len(sys.argv) > 5 else base

    for path, label in [
        (TRADE_CSV,    "trade_data"),
        (ACCT_MAP_CSV, "account_mapping"),
        (PROD_MAP_CSV, "product_mapping"),
        (BANK_MAP_CSV, "bank_mapping"),
    ]:
        if not os.path.exists(path):
            print(f"[ERROR] {label} not found: {path}")
            sys.exit(1)

    print("\n=== Consumer Scorecard V4 — Final Pipeline ===")
    print(f"  trade_data      : {TRADE_CSV}")
    print(f"  account_mapping : {ACCT_MAP_CSV}")
    print(f"  product_mapping : {PROD_MAP_CSV}")
    print(f"  bank_mapping    : {BANK_MAP_CSV}")
    print(f"  output_dir      : {OUTPUT_DIR}\n")

    print("--- Phase 0/1: Loading inputs ---")
    trade_df, acct_map, prod_map, bank_map = load_inputs(
        TRADE_CSV, ACCT_MAP_CSV, PROD_MAP_CSV, BANK_MAP_CSV
    )

    result = run_pipeline(trade_df, acct_map, prod_map, bank_map, OUTPUT_DIR,
                      include_all_columns=INCLUDE_ALL_COLUMNS)
    
    print("\n=== Pipeline complete. ===")