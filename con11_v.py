# =============================================================================
# Consumer Scorecard V4  —  FINAL END-TO-END PIPELINE
# =============================================================================
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
# JAVA 25 COMPATIBILITY
# ─────────────────────────────────────────────────────────────────────────────
#   - Security Manager removed → no -Djava.security.manager flag
#   - CSV I/O via pandas to avoid Hadoop Subject.getSubject() crash
#   - All pipeline logic is native PySpark
#
# NEW ATTRIBUTES vs PREVIOUS VERSION (from spec document)
# ─────────────────────────────────────────────────────────────────────────────
#   max_simul_unsec_wo_cc       — rewritten: last 12m, DPD ≤4, not closed
#   max_simul_unsec_wo_cc_inc_gl— same but include GL (191, 243)
#   Outflow_uns_secmov          — RegUns + SecMov/RegSec; 20m balance rundown median
#   max_lim_uns_secmov          — max modified_limit on RegUns + SecMov/RegSec
#   balance_amt_0_12_by_13_24   — ratio of live balances (0-12m / 13-24m)
#   bal_amt_6_12                — ratio of live balances (0-6m / 7-12m)
#   dlq_bal_12_24_bin           — ratio of delinquent balances (0-12m / 13-24m) binned
#   dlq_bal_24_36_bin           — ratio of delinquent balances (13-24m / 25-36m) binned
#   live_cnt_6_12_bin           — ratio of live tradeline counts (0-6m / 7-12m) binned
#   freq_between_accts_all      — avg months between consecutive account opens (all)
#   freq_between_accts_unsec    — avg months between unsecured non-CC opens
#   freq_between_installment    — avg months between instalment opens (excl HL/LAP)
#   max_simul_pl_cd             — max simultaneous PL + CD (123, 242, 189)
#   mon_since_first_worst_delq_bin — binned months since first worst delinquency
#   mon_since_recent_x_bin      — binned months since 1-30 DPD
#   mon_since_last_acct_open    — min mob across all accounts
#   max_dpd_UNS_6_12_M          — max DPD unsecured in months 6-12
#   max_dpd_UNS_L6_M            — max DPD unsecured in last 6 months
#   max_dpd_l30m                — max DPD across all trades in last 30 months
#   Outflow_AL_PL_TW_CD         — outflow on AL/PL/TW/CD trades
#   MAX_LIMIT_AL_PL_TW_CD       — sum of max limits: AL + PL + TW + CD
#   min_mon_sin_recent_1_bin    — min months since any 1+ DPD event (binned)
# =============================================================================

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
CC_CODES        = {"220", "121", "213", "5", "214", "225"}
CL_CODES        = {"240", "242", "244", "245", "246", "247", "248", "249"}  # Consumer Loan
HL_CODES        = {"002", "058", "2", "58"}
AL_CODES        = {"001", "047", "1", "47"}
TWO_W_CODES     = {"013", "173", "13"}
GL_CODES        = {"191", "007", "7"}
PL_CODES        = {"123", "130", "172", "195", "196", "215", "216", "217", "219",
                   "221", "222", "243"}  # CL codes (240,242,244-249) removed — handled by CL_CODES
AGRI_CODES      = {"167", "177", "178", "179", "198", "199", "200", "223", "224", "226", "227"}
COM_SEC_CODES   = {"175", "176", "228", "241", "191", "243", "007"}  # B3 FIX: GL codes treated as commercial secured
COM_UNSEC_CODES = {"176", "177", "178", "179", "228"}
REGULAR_CODES   = {"047", "058", "47", "58", "121", "123", "130", "172", "173", "191", "195", "196",
                   "215", "216", "217", "219", "220", "221", "222", "225", "240", "241",
                   "242", "243", "244", "245", "246", "247", "248", "249"}
PL_CD_TW_CODES  = {"173", "013", "13", "123", "242", "189"}   # PL + CD + TW as per spec
LAP_HL_CODES    = {"058", "195", "58"}                        # HL + LAP (excluded from freq_installment)
GL_CODES_EXT    = {"191", "243"}                        # GL codes for max_simul_inc_gl

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
    # GENERIC FIX: if cons_acct_key IS in data and is genuinely different from accno
    # (i.e. not identical to accno for all rows), keep it. Otherwise fall back to accno.
    if "cons_acct_key" not in trade_pdf.columns:
        trade_pdf["cons_acct_key"] = trade_pdf["accno"]
    else:
        # If all cons_acct_key == accno, the column adds no information — keep as-is.
        # If they differ (real account-level keys), keep them — enables per-account grouping.
        pass  # cons_acct_key already present and correct

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
    """Normalised DPD bucket 0-7. Mirrors ScoreCardHelperV4.getDpd_new()."""
    bal = float(balance_amt) if balance_amt is not None else -1.0
    dpd = int(dayspastdue)   if dayspastdue  is not None else -1
    pr  = str(pay_rating_cd).strip() if pay_rating_cd is not None else ""

    def _clean(val):
        """Return empty string for missing/sentinel values, else return stripped string.
        Sentinel values in this dataset: None, NaN, '0', '-1', '0.0', '-1.0'
        WO/SFW integer 0 means 'not set', -1 means 'not applicable'.
        Neither should trigger any WO code rule — treat both as missing.
        """
        if val is None: return ""
        try:
            fv = float(val)
            if _math.isnan(fv): return ""
            # Integer 0 = "not set", -1 = "not applicable" — both are sentinels
            if fv <= 0: return ""
        except (ValueError, TypeError): pass
        s = str(val).strip()
        return "" if s.lower() in ("", "nan", "none", "null", "0", "-1", "0.0", "-1.0") else s

    wo_raw = _clean(wo_settled)
    # GENERIC FIX: sentinel value 99 = "not applicable" in Indian bureau data
    # Must not trigger any WO rule (same as -1 or 0)
    if wo_raw in {"99", "099", "999"}:
        wo_raw = ""
    wo     = wo_raw.zfill(3) if wo_raw else ""

    if bal <= 500:
        return 0
    if dpd > 720:
        return 7
    if pr == "L" and dpd == -1:
        return 7
    if wo and wo in {"002","003","004","005","006","007","008","009",
                     "010","011","012","013","014","015"}:
        return 7
    if 361 <= dpd <= 720:
        return 6
    if pr in {"L","D"} and dpd == -1:
        return 6
    if wo == "000" and bal > 500:
        return 6
    if 181 <= dpd <= 360:
        return 5
    if pr in {"D","B"} and dpd == -1 and wo in {"002","003"}:
        return 5
    if 91 <= dpd <= 180:
        return 4
    if pr in {"L","B","D"} and dpd == -1:
        return 4
    # pay_rating_cd="S" = Standard/Sub-Standard in Indian bureau.
    # org_car logic: "S" maps to main8 (dpd_new=0) — standard performing account.
    # Only L, D, B, M trigger delinquency when dayspastdue=-1.
    # If dpd>0 use the numeric dpd bucket directly.
    if pr == "S" and dpd == -1:
        return 0
    if pr == "S" and dpd > 0:
        return (1 if dpd<=30 else 2 if dpd<=60 else 3 if dpd<=90 else
                4 if dpd<=180 else 5 if dpd<=360 else 6 if dpd<=720 else 7)
    if 61 <= dpd <= 90:
        return 3
    if pr == "M" and dpd == -1:
        return 3
    if 31 <= dpd <= 60:
        return 2
    if 1 <= dpd <= 30:
        return 1
    return 0


@udf(DoubleType())
def modified_limit_udf(balance_amt, credit_lim_amt, original_loan_amt):
    """Returns max(balance, credit_limit, original_loan).
    Sentinel values: None, -1, '-1', '0' → treated as 0 (not a real limit).
    All values arrive as strings after _pdf_to_spark conversion.
    """
    _SENTINELS = {None, -1, "-1", "-1.0", "0", "0.0"}
    def _to_float(v):
        if v in _SENTINELS: return 0.0
        try:
            fv = float(v)
            return 0.0 if fv < 0 else fv   # negative values are sentinels
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
        .withColumn("isPSB",      col("Category") == "PSB")
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
            fmax("open_dt").alias("open_dt"),  # GENERIC FIX: fmax(open_dt) = most recent recorded open date
            first("closed_dt").alias("closed_dt"),
            first("score_dt").alias("score_dt"),
            first("bureau_mbr_id").alias("bureau_mbr_id"),
            # NOTE: cons_acct_key is already a groupBy key — do NOT repeat it in agg()
            # Adding first("cons_acct_key") here would create duplicate column → AMBIGUOUS_REFERENCE
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
            first("isCL").alias("isCL"),
            first("Sec_Mov").alias("Sec_Mov"),
            first("modified_limit", ignorenulls=True).alias("latest_modified_limit"),
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
            fmax(when(col("month_diff") <= 35, lit(True))).alias("reportedIn36M"),
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
        .drop("_score_abs", "_open_abs")
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
    max_bal_idx = (
        fact2_enriched.filter(col("month_diff") <= 23)
        .withColumn("_rn", row_number().over(
            Window.partitionBy("cust_id", "cons_acct_key")
                  .orderBy(col("balance_amt").desc(), col("month_diff").asc())))   # REVERTED: .desc() worsened all customers, original .asc() is correct
        .filter(col("_rn") == 1)
        .select("cust_id", "cons_acct_key", col("month_diff").alias("m_since_max_bal_l24m"))
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

def build_global_attrs(account_details, fact2_enriched):
    # Rename cons_acct_key to _acct_id to eliminate AMBIGUOUS_REFERENCE.
    # build_account_details groups by cons_acct_key and joins DataFrames that also
    # carry cons_acct_key — Spark sees the name twice in the column lineage.
    # Using a private alias throughout Phase 4 avoids this completely.
    ad = account_details.withColumnRenamed("cons_acct_key", "_acct_id")
    fe = fact2_enriched

    # ── Exploded monthly data (used by multiple sub-phases) ──────────────────
    # _cons_key = cons_acct_key (true unique account key).
    # Embedded once into exploded so every downstream groupBy uses it directly —
    # no per-DataFrame _acct_key_map joins needed (eliminates 11 repeated joins).
    exploded = (
        ad.select(
            "cust_id", "accno",
            col("_acct_id").alias("_cons_key"),   # ← unique account key baked in
            "isCC", "isHL", "isAL", "isTW", "isGL", "isGLExt", "isPL", "isCL",
            "isUns", "isSec", "isSecMov", "isSecMovRegSec", "isAgri",
            "isComSec", "isComUnSec", "isRegSec", "isRegUns", "isRegular",
            "isPlCdTw", "isLapHl", "isPSB", "isPVT", "isNBFC", "isSFB",
            F.explode("monthly_data").alias("m")
        )
        .select(
            "cust_id", "accno", "_cons_key",
            "isCC", "isHL", "isAL", "isTW", "isGL", "isGLExt", "isPL", "isCL",
            "isUns", "isSec", "isSecMov", "isSecMovRegSec", "isAgri",
            "isComSec", "isComUnSec", "isRegSec", "isRegUns", "isRegular",
            "isPlCdTw", "isLapHl", "isPSB", "isPVT", "isNBFC", "isSFB",
            col("m.idx").alias("idx"),
            col("m.dpd").alias("dpd"),
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

    # ── 4A. Account counts ───────────────────────────────────────────────────
    # BUG-FIX: count("cons_acct_key") to count distinct accounts (each has unique cons_acct_key)
    acct_counts = (
        ad.groupBy("cust_id").agg(
            count("_acct_id").alias("nbr_tot_accts_36"),
            fsum(col("isLiveAccount").cast("int")).alias("nbr_live_accts_36"),
            fsum(col("reportedIn12M").cast("int")).alias("nbr_tot_accts_12"),
            fsum((col("reportedIn12M") & col("isLiveAccount")).cast("int")).alias("nbr_live_accts_12"),
            fsum(when(col("isCC")     & col("reportedIn36M"), lit(1))).alias("nbr_cc_tot_accts_36"),
            fsum(when(col("isCC")     & col("isLiveAccount"), lit(1))).alias("nbr_cc_live_accts_36"),
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
            # FIX 4: avg_mob_reg_36 = avg mob across ALL accounts in 36m window
            # GT for 10004499: avg([87,87,121,248])=135.75 confirms all accounts, not just isRegular
            favg(col("mob")).alias("avg_mob_reg_36"),
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

    # ── 4B. Accounts opened by mob window ────────────────────────────────────
    accts_opened = (
        ad.groupBy("cust_id").agg(
            fsum(when((col("mob") <= 3) & (col("mob") > 0), lit(1))).alias("nbr_accts_open_l3m"),
            fsum(when((col("mob") <= 6)  & (col("mob") > 0), lit(1))).alias("nbr_accts_open_l6m"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0), lit(1))).alias("nbr_accts_open_l12m"),
            fsum(when((col("mob") <= 16) & (col("mob") > 0), lit(1))).alias("nbr_accts_open_l16m"),
            fsum(when((col("mob") <= 24) & (col("mob") > 0), lit(1))).alias("nbr_accts_open_l24m"),
            fsum(when((col("mob") >= 4) & (col("mob") <= 6), lit(1))).alias("nbr_accts_open_4to6m"),
            fsum(when((col("mob") >= 7) & (col("mob") <= 12), lit(1))).alias("nbr_accts_open_7to12m"),
            fsum(when((col("mob") >= 13) & (col("mob") <= 24), lit(1))).alias("nbr_accts_open_13to24m"),
            fsum(when((col("mob") <= 6) & (col("mob") > 0) & ~col("isCC"), lit(1))).alias("nbr_accts_open_l6m_wo_cc"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0) & ~col("isCC"), lit(1))).alias("nbr_accts_open_l12m_wo_cc"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0) & col("isCC"), lit(1))).alias("nbr_accts_open_l12m_cc"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0) & col("isHL"), lit(1))).alias("nbr_accts_open_l12m_hl"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0) & col("isAL"), lit(1))).alias("nbr_accts_open_l12m_al"),
            fsum(when((col("mob") <= 12) & (col("mob") > 0) & col("isAgri"), lit(1))).alias("nbr_accts_open_l12m_agri"),
            fmin("mob").alias("mon_since_last_acct_open"),
        )
    )

    # ── 4C. Max DPD attributes ───────────────────────────────────────────────
    dpd_attrs = (
        exploded.groupBy("cust_id").agg(
            fmax(col("dpd")).alias("max_dpd_all_l36m"),
            # NaN-preserving: only actual DPD events (matches org_car _rptd variant)
            fmax(when((col("idx") <= 29) & (col("dpd") > 0), col("dpd"))).alias("max_dpd_l30m"),
            # NaN-preserving: only non-zero DPD events count (matches org_car _rptd variant)
            fmax(when(col("isUns") & (col("dpd") > 0), col("dpd"))).alias("max_dpd_uns_l36m"),
            fmax(when(col("isUns") & (col("idx") <= 11) & (col("dpd") > 0), col("dpd"))).alias("max_dpd_uns_l12m"),
            fmax(when(col("isUns") & (col("idx") <= 5)  & (col("dpd") > 0), col("dpd"))).alias("max_dpd_UNS_L6_M"),
            fmax(when(col("isUns") & (col("idx") >= 6) & (col("idx") <= 12) & (col("dpd") > 0),
                      col("dpd"))).alias("max_dpd_UNS_6_12_M"),
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
            fmin(when(col("dpd") >= 2, col("idx"))).alias("mon_sin_recent_1"),
            fmin(when(col("dpd") == 2, col("idx"))).alias("mon_sin_recent_2"),
            fmin(when(col("dpd") == 3, col("idx"))).alias("mon_sin_recent_3"),
            fmin(when(col("dpd") == 4, col("idx"))).alias("mon_sin_recent_4"),
            fmin(when(col("dpd") == 5, col("idx"))).alias("mon_sin_recent_5"),
            fmin(when(col("dpd") == 6, col("idx"))).alias("mon_sin_recent_6"),
            fmin(when(col("dpd") == 7, col("idx"))).alias("mon_sin_recent_7"),
            # GENERIC FIX: first_1 also uses dpd>=2 to match org_car 30-day threshold
            fmax(when(col("dpd") >= 2, col("idx"))).alias("mon_sin_first_1"),
            fmax(when(col("dpd") == 2, col("idx"))).alias("mon_sin_first_2"),
            fmax(when(col("dpd") == 3, col("idx"))).alias("mon_sin_first_3"),
            fmax(when(col("dpd") == 4, col("idx"))).alias("mon_sin_first_4"),
            fmax(when(col("dpd") == 5, col("idx"))).alias("mon_sin_first_5"),
            fmax(when(col("dpd") == 6, col("idx"))).alias("mon_sin_first_6"),
            fmax(when(col("dpd") == 7, col("idx"))).alias("mon_sin_first_7"),
        )
    )

    
    # max_sanc_amt_sec: max sanction on ALL Secured accounts (for max_sanc_amt_sec output col)
    # max_sanc_amt_secmov: max sanction on SecMov accounts ONLY (Sec_Mov='SecMov')
    # These are separate: type-58 has Sec_Mov='Rest' → isSecMov=False → secmov should be NaN
    max_sanc_amt_sec_df = (
        exploded
        .filter(col("mod_lim").isNotNull())
        .groupBy("cust_id")
        .agg(
            fmax(when(col("isSec") & (col("idx") <= 35), col("mod_lim"))).alias("max_sanc_amt_sec"),
            fmax(when(col("isSecMov") & (col("idx") <= 35), col("mod_lim"))).alias("max_sanc_amt_secmov_real"),
        )
    )
    # ────────────────────────────────────────────────────────────────
    # 4D. Utilisation metrics (Correct final sequence)
    # ────────────────────────────────────────────────────────────────

    # 1. Live-account filter
    _live_for_util = (
        ad.select("cust_id", "accno", "isLiveAccount")
          .filter(col("isLiveAccount") == True)
    )

    # _cons_key is now embedded directly in exploded (see exploded build above)

    max_sanc_amt_sec_df = (
        exploded
        .filter(col("mod_lim").isNotNull())
        .groupBy("cust_id")
        .agg(
            fmax(when(col("isSec") & (col("idx") <= 35), col("mod_lim"))).alias("max_sanc_amt_sec"),
            fmax(when(col("isSecMov") & (col("idx") <= 35), col("mod_lim"))).alias("max_sanc_amt_secmov_real"),
        )
    )
    # ────────────────────────────────────────────────────────────────
    # 3. ALL-accounts per-account utilisation (ORG CAR avg-of-ratios)
    # ────────────────────────────────────────────────────────────────
    _per_acct_all_util = (
        exploded
        .filter(col("mod_lim") > 0)
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
        .withColumn("_u_l3m",  F.try_divide(col("_bal_l3m"),  col("_lim") * col("_n_l3m")))
        .withColumn("_u_l6m",  F.try_divide(col("_bal_l6m"),  col("_lim") * col("_n_l6m")))
        .withColumn("_u_l12m", F.try_divide(col("_bal_l12m"), col("_lim") * col("_n_l12m")))
    )

    # ────────────────────────────────────────────────────────────────
    # 4. UNS‑accounts only per-account utilisation
    # ────────────────────────────────────────────────────────────────
    _per_acct_uns_util = (
        exploded
        .join(_live_for_util, ["cust_id", "accno"], "inner")
        .filter(col("isUns") & (col("mod_lim") > 0))
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
        .withColumn("_u_l3m",  F.try_divide(col("_bal_l3m"),  col("_lim") * col("_n_l3m")))
        .withColumn("_u_l6m",  F.try_divide(col("_bal_l6m"),  col("_lim") * col("_n_l6m")))
        .withColumn("_u_l12m", F.try_divide(col("_bal_l12m"), col("_lim") * col("_n_l12m")))
    )

    # UNS aggregated (mean over accounts)
    util_uns_df = (
        _per_acct_uns_util
        .groupBy("cust_id")
        .agg(
            favg(when(col("_n_l3m") > 0, col("_u_l3m"))).alias("util_l3m_uns_tot"),
            favg(when(col("_n_l6m") > 0, col("_u_l6m"))).alias("util_l6m_uns_tot"),
            favg(when(col("_n_l12m") > 0, col("_u_l12m"))).alias("util_l12m_uns_tot"),
        )
    )

    # ────────────────────────────────────────────────────────────────
    # 5. Ratio‑of‑sums util (KEEP THIS)
    # ────────────────────────────────────────────────────────────────
    _ros_all_util = (
        exploded.filter(col("mod_lim") > 0)
        .groupBy("cust_id")
        .agg(
            F.try_divide(
                fsum(when(col("idx") <= 2, col("bal"))),
                fsum(when(col("idx") <= 2, col("mod_lim")))
            ).alias("util_l3m_all_tot"),
            F.try_divide(
                fsum(when(col("idx") <= 5, col("bal"))),
                fsum(when(col("idx") <= 5, col("mod_lim")))
            ).alias("util_l6m_all_tot"),
            F.try_divide(
                fsum(when(col("idx") <= 11, col("bal"))),
                fsum(when(col("idx") <= 11, col("mod_lim")))
            ).alias("util_l12m_all_tot"),
        )
    )

    # ────────────────────────────────────────────────────────────────
    # 6. Final ALL‑accounts util dataframe
    # ────────────────────────────────────────────────────────────────
    util_all_df = (
        _per_acct_all_util
        .groupBy("cust_id")
        .agg(
            favg(when(col("_n_l3m")  > 0, col("_u_l3m"))).alias("avg_util_l3m_all_tot"),
            favg(when(col("_n_l6m")  > 0, col("_u_l6m"))).alias("avg_util_l6m_all_tot"),
            favg(when(col("_n_l12m") > 0, col("_u_l12m"))).alias("avg_util_l12m_all_tot"),
        )
        .join(_ros_all_util, "cust_id", "left")
    )

    # ── util_exc_cc_df: ~isCC & isLive, ratio-of-sums
    # ROOT CAUSE FIX: util_l3m_exc_cc_live = sum(bal)/sum(lim) on ~isCC & isLive, idx<=2
    # Includes Sec accounts; uses ratio-of-sums NOT avg-of-ratios.
 
    # This replaces the util_l3m_uns_tot alias for the rename_map -> util_l3m_exc_cc_live.
    util_exc_cc_df = (
        exploded
        .join(_live_for_util, ["cust_id", "accno"], "inner")
        .filter(~col("isCC") & (col("mod_lim") > 0))
        .groupBy("cust_id").agg(
            F.try_divide(
                fsum(when(col("idx") <= 2,  col("bal"))),
                fsum(when(col("idx") <= 2,  col("mod_lim")))
            ).alias("util_l3m_exc_cc_live"),
        )
    )
    # ────────────────────────────────────────────────────────────────

    util_cc_df = (
        exploded.filter(col("isCC") & (col("mod_lim") > 0))
        .withColumn("util", F.try_divide(col("bal"), col("mod_lim")))
        .groupBy("cust_id").agg(
            favg(when(col("idx") <= 2,  col("util"))).alias("util_l3m_cc_live"),
            favg(when(col("idx") <= 5,  col("util"))).alias("util_l6m_cc_live"),
            favg(when(col("idx") <= 11, col("util"))).alias("util_l12m_cc_live"),
            fmax(when(col("idx") == 0,  col("util"))).alias("util_m0_cc"),
        )
    )

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
            fmax(when((col("idx") == 0) & col("isPL"), col("mod_lim"))).alias("max_sanc_amt_pl"),
            fmax(when((col("idx") == 0) & col("isAL"), col("mod_lim"))).alias("max_sanc_amt_al"),
            fmax(when((col("idx") == 0) & col("isTW"), col("mod_lim"))).alias("max_sanc_amt_tw"),
            fmax(when((col("idx") == 0) & col("isCL"), col("mod_lim"))).alias("max_sanc_amt_cl"),
            # fmax(when((col("idx") == 0) & col("account_type_cd").isin("240","242","244","245","246","247","248","249"),
            #           col("mod_lim"))).alias("max_sanc_amt_cl"),
        )
    )

    # ── 4F. Balance trend flags (exc CC) ────────────────────────────────────
    monthly_total_bal = (
        fe.filter(~col("isCC"))
        .groupBy("cust_id", "month_diff")
        .agg(fsum("balance_amt").alias("tot_bal"))
    )
    # WINDOW DIRECTION FIX: use ASC order so lead() gives the OLDER (higher month_diff) period
    # Old: orderBy(DESC) + lead() → lead gave NEWER month → pct_chg compared to future (wrong)
    # New: orderBy(ASC) + lead() → lead gives OLDER month → pct_chg = (current-older)/older ✓
    # totconsdec window: org_car uses month_diff 1..25 for 36m label (confirmed ref=24)
    w_trend = Window.partitionBy("cust_id").orderBy(col("month_diff").asc())
    trend_df = (
        monthly_total_bal
        .withColumn("prev_bal", lead("tot_bal").over(w_trend))  # lead = older month (ASC)
        .withColumn("pct_chg",
                    when(col("prev_bal") > 0,
                         F.try_divide(col("tot_bal") - col("prev_bal"), col("prev_bal")))
                    .otherwise(lit(None)))
        .groupBy("cust_id").agg(
            fsum(when((col("month_diff") <= 24) & (col("pct_chg") >= 0.10),
                      lit(1)).otherwise(lit(0))).alias("totconsinc_bal10_exc_cc_25m"),
            fsum(when((col("month_diff") <= 24) & (col("pct_chg") >= 0.05),
                      lit(1)).otherwise(lit(0))).alias("totconsinc_bal5_exc_cc_25m"),
            # GENERIC FIX: use full 1..35 window (not 1..25) for totconsdec.
            # 1..25 matched cust1 but undercounts for customers with older account history.
            fsum(when((col("month_diff") >= 1) & (col("month_diff") <= 35) & (col("pct_chg") < 0),
                      lit(1)).otherwise(lit(0))).alias("totconsdec_bal_tot_36m"),
            # 7m window trend
            fsum(when((col("month_diff") <= 6) & (col("pct_chg") >= 0.10),
                      lit(1)).otherwise(lit(0))).alias("totconsinc_bal10_exc_cc_7m"),
            fsum(when((col("month_diff") <= 6) & (col("pct_chg") >= 0.05),
                      lit(1)).otherwise(lit(0))).alias("totconsinc_bal5_exc_cc_7m"),
            fsum(when((col("month_diff") <= 24) & (col("pct_chg") >= 0.10),
                      lit(1)).otherwise(lit(0))).alias("consinc_bal10_exc_cc_25m"),
        )
    )

    # ── consinc_bal10_tot_7m: consecutive >10% total balance increase in 7m ──
    _all_bal_m = (
        fe.groupBy("cust_id","month_diff")
          .agg(fsum("balance_amt").alias("_tot_b_all"))
    )
    _w_all = Window.partitionBy("cust_id").orderBy(col("month_diff").desc())
    consinc_all7_df = (
        _all_bal_m
        .withColumn("_prev_b", lead("_tot_b_all").over(_w_all))
        .withColumn("_pct_b", when(col("_prev_b") > 0,
                                   F.try_divide(col("_tot_b_all")-col("_prev_b"),col("_prev_b")))
                              .otherwise(lit(None)))
        .groupBy("cust_id")
        .agg(fsum(when((col("month_diff") <= 6) & (col("_pct_b") >= 0.10),
                       lit(1)).otherwise(lit(0))).alias("consinc_bal10_tot_7m"))
    )

    # ── 4G. Outflow algorithms (EMI estimation) ──────────────────────────────
    IR_BUCKETS = [round(0.06 + i * 0.02, 2) for i in range(21)]

    emi_schema = StructType([
        StructField("cust_id",    LongType(),   nullable=False),
        StructField("accno",      LongType(),   nullable=False),
        StructField("emi_median", DoubleType(), nullable=True),
        StructField("is_plcdtw",  BooleanType(), nullable=True),
        StructField("is_cc",      BooleanType(), nullable=True),
        StructField("is_secmov_regsec", BooleanType(), nullable=True),
        StructField("is_reguns",  BooleanType(), nullable=True),
    ])

    def compute_emi_udf(pdf):
        # GENERIC FIX: use cons_acct_key (not accno) to separate accounts.
        # When accno == cust_id, groupby(cust_id, accno) collapses all accounts.
        from collections import Counter
        results = []
        # Use cons_acct_key if available, else fall back to accno
        grp_key = "cons_acct_key" if "cons_acct_key" in pdf.columns else "accno"
        for (cust_id, acct_key), grp in pdf.groupby(["cust_id", grp_key]):
            accno = grp["accno"].iloc[0]  # keep original accno for output
            grp   = grp.sort_values("month_diff")
            bals  = grp["balance_amt"].fillna(-1).tolist()
            idx   = grp["month_diff"].tolist()
            is_cc = bool(grp["isCC"].iloc[0])
            is_plcdtw = bool(grp["isPlCdTw"].iloc[0])
            is_secmov = bool(grp["isSecMovRegSec"].iloc[0])
            is_reguns = bool(grp["isRegUns"].iloc[0])
            emi_list  = []

            if is_cc:
                for i in range(len(idx) - 1):
                    if idx[i+1] - idx[i] == 1:
                        b0, b1 = bals[i], bals[i+1]
                        if b0 > 0 and b1 > 0 and b1 > b0:
                            emi_list.append(b1 - b0)
            else:
                ir_pairs = []
                for i in range(len(idx) - 1):
                    if idx[i+1] - idx[i] == 1:
                        b0, b1 = bals[i], bals[i+1]
                        # bals sorted ascending by month_diff: b0=newer, b1=older
                        # Drawdown (revolving/new loan): b0 > b1 (newer > older) → IR > 0
                        # Amortising (decreasing balance): b0 < b1 → skip (no EMI)
                        if b0 > 0 and b1 > 0 and b0 > b1:
                            ir_raw  = (b0 / b1) - 1.0  # always positive since b0>b1
                            nearest = min(IR_BUCKETS, key=lambda x: abs(x - ir_raw))
                            ir_pairs.append((i, nearest))
                if ir_pairs:
                    modal_ir = Counter(b for _, b in ir_pairs).most_common(1)[0][0]
                    for i in range(len(idx) - 1):
                        if idx[i+1] - idx[i] == 1:
                            b0, b1 = bals[i], bals[i+1]
                            # bals sorted ascending: b0=newer, b1=older
                            # Only compute EMI on drawdown pairs (newer > older = b0 > b1)
                            if b0 > 0 and b1 > 0 and b0 > b1:
                                emi_est = b1 * (1 + modal_ir / 12) - b0
                                if emi_est > 0:
                                    emi_list.append(emi_est)

            emi_med = float(np.median(emi_list)) if len(emi_list) >= 1 else None
            results.append({
                "cust_id":          int(cust_id),
                "accno":            int(accno),  # original accno for join
                "emi_median":       emi_med,
                "is_plcdtw":        is_plcdtw,
                "is_cc":            is_cc,
                "is_secmov_regsec": is_secmov,
                "is_reguns":        is_reguns,
            })
        return pd.DataFrame(results, columns=[
            "cust_id","accno","emi_median","is_plcdtw","is_cc","is_secmov_regsec","is_reguns"
        ])

    # BUG-FIX: account_details keyed on cons_acct_key now
    outflow_input = (
        fe.select("cust_id","accno","cons_acct_key","month_diff","balance_amt","isCC","isPlCdTw","isSecMovRegSec","isRegUns")
        .filter(col("balance_amt") > 500)
        .join(
            ad.select("cust_id",col("_acct_id").alias("cons_acct_key"),
                col("isLiveAccount").alias("_live"),
                col("isSecMovRegSec").alias("_secmov"),
                col("isRegUns").alias("_reguns"),
                col("isPlCdTw").alias("_plcdtw"),
            ),
            ["cust_id","cons_acct_key"], "left"
        )
        .filter(col("_live") == True)
        .drop("_live", "_secmov", "_reguns", "_plcdtw")
    )

    # RDD collect → pandas groupby → RDD parallelize
    # pandas_udf GROUPED_MAP and Spark write.csv both call Hadoop Subject.getSubject()
    # which crashes on Java 25 locally. The rdd.collect() path avoids Hadoop entirely.
    _emi_cols = outflow_input.columns
    _emi_rows = outflow_input.rdd.collect()
    _emi_pdf  = pd.DataFrame(_emi_rows, columns=_emi_cols)
    # GENERIC FIX: group by (cust_id, cons_acct_key) if available
    _emi_grp_key = ["cust_id","cons_acct_key"] if "cons_acct_key" in _emi_pdf.columns else ["cust_id","accno"]
    _emi_parts = [compute_emi_udf(grp) for _, grp in _emi_pdf.groupby(_emi_grp_key)]
    _emi_result = pd.concat(_emi_parts, ignore_index=True) if _emi_parts else pd.DataFrame(
        columns=["cust_id","accno","emi_median","is_plcdtw","is_cc","is_secmov_regsec","is_reguns"])
    account_emi = spark.createDataFrame(
        spark.sparkContext.parallelize([tuple(r) for r in _emi_result.itertuples(index=False)]),
        schema=emi_schema)
    account_emi.cache()

    # Outflow_uns_secmov: RegUns + SecMov/RegSec, LIVE, last 20 months
    outflow_df = (
        account_emi
        .filter(col("emi_median").isNotNull())
        .filter(col("is_reguns") | col("is_secmov_regsec"))
        .groupBy("cust_id")
        .agg(fsum("emi_median").alias("Outflow_uns_secmov"))
    )

    # Outflow_AL_PL_TW_CD
    outflow_plcdtw_df = (
        account_emi
        .filter(col("emi_median").isNotNull())
        .filter(col("is_plcdtw"))
        .groupBy("cust_id")
        .agg(fsum("emi_median").alias("Outflow_AL_PL_TW_CD"))
    )

    # Total outflow excl CC
    total_outflow_wo_cc = (
        account_emi
        .filter(col("emi_median").isNotNull())
        .filter(~col("is_cc"))                                         # REVERTED: ~is_secmov_regsec was too aggressive — broke customers with SecMov accounts
        .groupBy("cust_id")
        .agg(fsum("emi_median").alias("total_outflow_wo_cc"))
    )

    # ── Product-split outflows ────────────────────────────────────────────────
    # BUG-FIX: account_details keyed by cons_acct_key; keep accno for EMI join
    _acct_flags = (
        ad.select(
            "cust_id", "accno", "_acct_id", "isAL", "isTW", "isPL", "isHL",
            col("account_type_cd").alias("_acd"),
            col("isSecMov").alias("_isSecMov")
        )
    )
    _emi_flags = account_emi.join(_acct_flags, ["cust_id","accno"], "left")
    # FIX 15: type 58 (HL) has Sec_Mov="Rest" → isSecMov=False → was excluded.
    # HL_LAP_outflow must include both HL accounts (isHL) and LAP (isSecMov=True).
    outflow_hl_lap_df = (
        _emi_flags.filter(col("emi_median").isNotNull() & (col("isHL") | col("_isSecMov")))
        .groupBy("cust_id").agg(fsum("emi_median").alias("HL_LAP_outflow"))
    )
    outflow_al_tw_df = (
        _emi_flags.filter(col("emi_median").isNotNull() & (col("isAL") | col("isTW")))
        .groupBy("cust_id").agg(fsum("emi_median").alias("AL_TW_outflow"))
    )
    outflow_pl_df = (
        _emi_flags.filter(col("emi_median").isNotNull() & col("isPL"))
        .groupBy("cust_id").agg(fsum("emi_median").alias("PL_outflow"))
    )
    outflow_cd_df = (
        _emi_flags.filter(col("emi_median").isNotNull() & (col("_acd") == "189"))
        .groupBy("cust_id").agg(fsum("emi_median").alias("CD_outflow"))
    )

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

    # ── 4J. Max balance & mon-since-max-bal + worst delinquency ─────────────
    max_bal_df = (
        ad.groupBy("cust_id").agg(
            fmax("max_bal_l24m").alias("max_bal_l24m"),
            fmax("max_bal_l12m").alias("max_bal_l12m"),
            fmin(when(col("isUns"), col("m_since_max_bal_l24m"))).alias("mon_since_max_bal_l24m_uns"),
            fmin("mon_since_first_worst_delq").alias("mon_since_first_worst_delq"),
            fmin("mon_since_recent_worst_delq").alias("mon_since_recent_worst_delq"),
        )
    )


    # ── agri balance ratio ───────────────────────────────────────────────────
    agri_bal_ratio_df = (
        fe.filter(col("isAgri"))
        .groupBy("cust_id").agg(
            favg(when((col("month_diff") >= 1) & (col("month_diff") <= 6),
                      col("balance_amt"))).alias("_agri_b_1_6"),
            favg(when((col("month_diff") >= 7) & (col("month_diff") <= 12),
                      col("balance_amt"))).alias("_agri_b_7_12"),
        )
        .withColumn("avg_b_1to6_by_7_12_agri",
                    when(col("_agri_b_7_12") > 0,
                         F.try_divide(col("_agri_b_1_6"), col("_agri_b_7_12")))
                    .otherwise(lit(None)))
        .drop("_agri_b_1_6","_agri_b_7_12")
    )

    # ── 4K. Reporting counts ─────────────────────────────────────────────────
    rpt_df = (
        exploded.groupBy("cust_id").agg(
            countDistinct(when(col("idx") == 0,  col("accno"))).alias("tot_accts_rptd_0m"),
            countDistinct(when(col("idx") <= 2,  col("accno"))).alias("tot_accts_rptd_l3m"),
            countDistinct(when(col("idx") <= 11, col("accno"))).alias("tot_accts_rptd_l12m"),
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
            (col("idx") <= 11) &
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
            (col("idx") <= 11) &
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
        .filter(col("isUns") & (col("idx") <= 11) & (col("dpd") <= 4) & col("is_not_closed"))
        .groupBy("cust_id","idx")
        .agg(countDistinct("_cons_key").alias("_cnt_uns_all"))
        .groupBy("cust_id")
        .agg(fmax("_cnt_uns_all").alias("max_simul_unsec"))
    )

    # Max simultaneous PL+CD (123, 242, 189) per spec
    # GENERIC FIX: _cons_key for counting
    simul_plcd_df = (
        exploded
        .filter(
            col("isPlCdTw") & (col("idx") <= 11) &
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
    # Filter: DPD ≥0, ≤6, not closed (live trades)
    # GENERIC FIX: balance_amt_0_12_by_13_24 uses ONLY non-Sec (UnSec) live accounts.
    # Including Sec (HL) accounts causes ratio to be computed when ref expects NaN
    # (Sec accounts have balance in 13-24m window but org_car excludes them from this ratio).
    live_bal = (
        exploded
        .filter(
            (col("dpd") >= 0) & (col("dpd") <= 6) &
            col("is_not_closed") &
            col("isUns")   # GENERIC FIX: UnSec only — matches org_car bal_live filter
        )
        .groupBy("cust_id", "idx")
        .agg(
            fsum("bal").alias("sum_bal"),
            count("accno").alias("cnt_trades"),
        )
    )

    bal_ratio_df = (
        live_bal.groupBy("cust_id").agg(
            favg(when((col("idx") >= 0)  & (col("idx") <= 5),  col("sum_bal"))).alias("_bal_0_6"),
            favg(when((col("idx") >= 7)  & (col("idx") <= 12), col("sum_bal"))).alias("_bal_7_12"),
            favg(when((col("idx") >= 0)  & (col("idx") <= 11), col("sum_bal"))).alias("_bal_0_12"),
            favg(when((col("idx") >= 13) & (col("idx") <= 24), col("sum_bal"))).alias("_bal_13_24"),
            favg(when((col("idx") >= 0)  & (col("idx") <= 5),  col("cnt_trades"))).alias("_cnt_0_6"),
            # favg(when((col("idx") >= 7)  & (col("idx") <= 12), col("cnt_trades"))).alias("_cnt_7_12"),
            favg(when((col("idx") >= 6) & (col("idx") <= 11), col("cnt_trades"))).alias("_cnt_7_12"),
        )
        .withColumn("bal_amt_6_12",
                    when(col("_bal_7_12") > 0,
                         F.try_divide(col("_bal_0_6"), col("_bal_7_12"))).otherwise(lit(None)))
        .withColumn("balance_amt_0_12_by_13_24",
                    when(col("_bal_13_24") > 0,
                         F.try_divide(col("_bal_0_12"), col("_bal_13_24"))).otherwise(lit(None)))
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
        .drop("_bal_0_6","_bal_7_12","_bal_0_12","_bal_13_24","_cnt_0_6","_cnt_7_12")
    )

    # Delinquent balance ratios (DPD > 1, not closed)
    dlq_bal = (
        exploded
        .filter((col("dpd") > 1) & col("is_not_closed"))
        .groupBy("cust_id", "idx")
        .agg(fsum("bal").alias("dlq_bal"))
    )

    dlq_ratio_df = (
        dlq_bal.groupBy("cust_id").agg(
            favg(when((col("idx") <= 11), col("dlq_bal"))).alias("_dlq_0_12"),
            favg(when((col("idx") >= 13) & (col("idx") <= 24), col("dlq_bal"))).alias("_dlq_13_24"),
            favg(when((col("idx") >= 25) & (col("idx") <= 35), col("dlq_bal"))).alias("_dlq_25_36"),
        )
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
    # need to check with chargen
    @udf(DoubleType())
    def avg_gap_udf(arr):
        if arr is None or len(arr) == 0:
            return None
        if len(arr) == 1:
            return 0.0                                         # FIX 4: single account → gap = 0, not None
        gaps = [arr[i+1] - arr[i] for i in range(len(arr)-1) if arr[i+1] != arr[i]]
        return float(sum(gaps) / len(gaps)) if gaps else 0.0  # FIX 4: return 0 not None when all mobs equal
    

    mob_all_df = (
        ad.groupBy("cust_id")
        .agg(sort_array(collect_list(col("mob"))).alias("_mobs_all"))
        .withColumn("freq_between_accts_all", avg_gap_udf(col("_mobs_all")))
        .drop("_mobs_all")
    )

    mob_uns_df = (
        ad.filter(col("isUns") & ~col("isCC"))
        .groupBy("cust_id")
        .agg(sort_array(collect_list(col("mob"))).alias("_mobs_uns"))
        .withColumn("freq_between_accts_unsec_wo_cc", avg_gap_udf(col("_mobs_uns")))
        .drop("_mobs_uns")
    )

    mob_inst_df = (
        ad.filter(~col("isCC") & ~col("isLapHl"))    # excl CC, HL, LAP per spec
        .groupBy("cust_id")
        .agg(sort_array(collect_list(col("mob"))).alias("_mobs_inst"))
        .withColumn("freq_between_installment_trades", avg_gap_udf(col("_mobs_inst")))
        .drop("_mobs_inst")
    )

    freq_df = (
        mob_all_df
        .join(mob_uns_df,  "cust_id", "left")
        .join(mob_inst_df, "cust_id", "left")
    )

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
        .filter(col("isCC") & (col("mod_lim") > 0) & (col("idx") <= 15))
        .withColumn("util", F.try_divide(col("bal"), col("mod_lim")))
        .groupBy("cust_id", "_cons_key")
        .agg(favg("util").alias("avg_util_16m"))
        .filter(col("avg_util_16m") > 0.40)
        .groupBy("cust_id")
        .agg(count("_cons_key").alias("nbr_cc4016m_tot_accts_36"))
    )

    # nbr_cc40l6m: CC accounts with avg util > 40% in last 6 months
    # Returns 0 (not NaN) when customer has CC but none exceed 40% — org_car grp_nansum pattern
    # GENERIC FIX: count distinct _cons_key (accounts) not accno
    _cc40_all = (
        exploded
        .filter(col("isCC") & (col("mod_lim") > 0) & (col("idx") <= 5))
        .groupBy("cust_id")
        .agg(countDistinct("_cons_key").alias("_total_cc_l6m"))
    )
    # GENERIC FIX: use _cons_key
    cc40_6m_df = (
        exploded
        .filter(col("isCC") & (col("mod_lim") > 0) & (col("idx") <= 5))
        .withColumn("util", F.try_divide(col("bal"), col("mod_lim")))
        .groupBy("cust_id", "_cons_key")
        .agg(favg("util").alias("avg_util_l6m"))
        .filter(col("avg_util_l6m") > 0.40)
        .groupBy("cust_id")
        .agg(count("_cons_key").alias("_cc40_count"))
        .join(_cc40_all, "cust_id", "right")
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
    # BUG-FIX: account_details now keyed on cons_acct_key; join accordingly
    # Join exploded (has "accno") with ad (has "_acct_id") on accno
    # FIX 12: was joining exploded.accno (cust_id int) vs _acct_id (cons_acct_key) — never matched
    live_for_sec_df = ad.select("cust_id", "accno", "isLiveAccount")
    max_dpd_sec0_live_df = (
        exploded
        .filter(col("isSec") & (col("idx") == 0))
        .join(live_for_sec_df, ["cust_id", "accno"], "left")
        .filter(col("isLiveAccount") == True)
        .groupBy("cust_id")
        .agg(fmax("dpd").alias("max_dpd_sec0_live"))
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
    # Java: count LIVE accounts that had any dpd>0 in last 24m
    # BUG-FIX: countDistinct(accno) = distinct LIVE accounts with any DPD in 24m
    # Previously F.count("accno") counted row-events (one per DPD month), 
    # producing values much larger than nbr_live_accts_36
    # Join exploded (has "accno") with ad (has "_acct_id") — bridge via accno
    #     # FIX 13: count month-rows (not distinct accounts) with dpd>0 on live accounts in last 24m
    # GT=7 for 10003821: account 5680303167 has 7 months with dpd>0
    live_accno_df = ad.select("cust_id", "accno", "isLiveAccount")
    nbr_0_24m_live_df = (
        exploded
        .filter((col("idx") < 24) & (col("dpd") > 0))
        .join(live_accno_df, ["cust_id", "accno"], "left")
        .filter(col("isLiveAccount") == True)
        .groupBy("cust_id")
        # FIX 6: count month-rows with dpd>0 (not distinct accounts)
        # GT=7 for 10003821: account 5680303167 has 7 months dpd>0
        .agg(count("*").alias("nbr_0_24m_live"))
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
        .agg(fmin(when(col("m_since_max_bal_l24m").isNotNull(),
                       col("m_since_max_bal_l24m"))).alias("mon_since_max_bal_124m_uns"))
    )

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
    # Avoids ambiguous cust_id from cross-joining ad with exploded.
    nbr_pl_le50_df = (
        ad.filter(col("isPL") & col("reportedIn36M"))
        .filter(col("latest_modified_limit") <= 50000)
        .groupBy("cust_id")
        .agg(count("_acct_id").alias("nbr_pl_le50_tot_accts_36"))
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

    # BUG-FIX: account_details now has one row per (cust_id, cons_acct_key).
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
    _util7m = (
        exploded
        .filter((col("idx") < 7) & (coalesce(col("mod_lim"), lit(0)) > 0))  # B6 FIX: handle null mod_lim
        .groupBy("cust_id","idx")
        .agg(
            fsum("bal").alias("_tot_bal"),
            fsum("mod_lim").alias("_tot_lim"),
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
            for m in range(1, 7):
                u_newer = utils.get(m)      # more recent
                u_older = utils.get(m + 1)  # one month earlier
                if u_newer is not None and u_older is not None and (u_newer - u_older) > 0.01:
                    consec_count += 1
                else:
                    break
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
    # Java: for each CC account, count consecutive months where balance
    # increased >5% in last 13 months, then take max across CC accounts
    # GENERIC FIX: use _cons_key to keep accounts separate when accno == cust_id
    _cc_bal13 = (
        exploded
        .filter(col("isCC") & (col("idx") < 13))
        .select("cust_id", col("_cons_key").alias("accno"), "idx", "bal")
    )

    _cc_bal13_schema = StructType([
        StructField("cust_id",           LongType(),   nullable=False),
        StructField("consinc_bal5_cc_13m", IntegerType(), nullable=True),
    ])

    def compute_consinc_bal5_cc(pdf):
        # GENERIC FIX: iterate over consecutive AVAILABLE pairs (sorted by idx ascending).
        # Old code iterated m=1..12 assuming bals[0] exists — always broke at m=1
        # when the account starts reporting at month_diff=2 or later.
        # New: loop over sorted available (idx, bal) pairs and check consecutive months.
        # Each pair (idx[i], idx[i+1]) is consecutive only when idx[i+1] == idx[i]+1.
        # Compare bals[i] (newer) vs bals[i+1] (older): if newer > older * 1.05 → count.
        results = []
        for cust_id, grp in pdf.groupby("cust_id"):
            max_consec = 0
            for accno, acct_grp in grp.groupby("accno"):
                acct_grp = acct_grp.sort_values("idx")
                idxs = acct_grp["idx"].tolist()
                bals_list = acct_grp["bal"].tolist()
                consec = 0
                for i in range(len(idxs) - 1):
                    if idxs[i+1] - idxs[i] != 1:
                        break  # gap in reporting — reset
                    b_newer = bals_list[i]      # lower idx = more recent
                    b_older = bals_list[i+1]    # higher idx = older month
                    if b_newer and b_older and b_older > 0 and b_newer > 0:
                        if (b_newer - b_older) / b_older > 0.05:
                            consec += 1
                        else:
                            break
                    else:
                        break
                max_consec = max(max_consec, consec)
            results.append({"cust_id": int(cust_id), "consinc_bal5_cc_13m": max_consec})
        return pd.DataFrame(results, columns=["cust_id", "consinc_bal5_cc_13m"])

    # RDD collect → pandas groupby → RDD parallelize (Java 25 Hadoop workaround)
    _cc13_rows = _cc_bal13.rdd.collect()
    _cc13_pdf  = pd.DataFrame(_cc13_rows, columns=["cust_id","accno","idx","bal"])
    if len(_cc13_pdf) > 0:
        _cc13_parts  = [compute_consinc_bal5_cc(grp) for _, grp in _cc13_pdf.groupby("cust_id")]
        _cc13_result = pd.concat(_cc13_parts, ignore_index=True)
    else:
        _cc13_result = pd.DataFrame(columns=["cust_id","consinc_bal5_cc_13m"])
    consinc_bal5_cc_df = spark.createDataFrame(
        spark.sparkContext.parallelize([tuple(r) for r in _cc13_result.itertuples(index=False)]),
        schema=_cc_bal13_schema)

    # sum_sanc_amt_uns — sum of max mod_lim across ALL unsec accounts (live AND closed)
    # org_car: sum_sanc_amt_uns = grp_nansum(modified_limit * cond['uns']) — NO live filter.
    # Confirmed: ref=370000 = 318000 (live 531045921) + 52000 (closed 220732246) ✓
    # GENERIC FIX: group by _cons_key (cons_acct_key) not accno.
    # In datasets where accno == cust_id, groupBy(cust_id,accno) collapses all accounts
    # into one group → fmax picks max across ALL accounts, not per account.
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
    )

    # ── Derived flags ─────────────────────────────────────────────────────────
    global_attrs = (
        global_attrs
        .withColumn("all_accts_al_cc_hl",
                    (col("nbr_not_al_cc_hl") == 0))
        .withColumn("has_agri_or_com",
                    (col("nbr_agri_tot_accts_36") > 0) | (col("nbr_comsec_tot_accts_36") > 0))

        .withColumn("open_cnt_0_6_by_7_12_bin",
            when(col("nbr_accts_open_7to12m") > 0,
                    F.try_divide(col("nbr_accts_open_l6m"), col("nbr_accts_open_7to12m")))
            .otherwise(lit(None).cast(DoubleType())))  # NaN when no 7-12m opens (matches org_car)            
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
    account_emi.unpersist()

    # ============================================================
    #   NEW PATCH — ENSURE THESE ALWAYS REMAIN NULL (Like GT)
    # ============================================================

    global_attrs = (
        global_attrs
        .withColumn("max_simul_unsec_wo_cc",
            F.when(F.col("max_simul_unsec_wo_cc") == -999, F.lit(None))
             .otherwise(F.col("max_simul_unsec_wo_cc")))
        .withColumn("max_simul_unsec_wo_cc_inc_gl",
            F.when(F.col("max_simul_unsec_wo_cc_inc_gl") == -999, F.lit(None))
             .otherwise(F.col("max_simul_unsec_wo_cc_inc_gl")))
    )

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


def run_pipeline(trade_df, acct_map, prod_map, bank_map, output_dir="."):

    

    from pyspark.sql.functions import when, col, concat, lit, substring


    trade_df = (
        trade_df
        .withColumn("open_dt",   clean_numeric_date("open_dt"))
        .withColumn("closed_dt", clean_numeric_date("closed_dt"))
        .withColumn("balance_dt", clean_numeric_date("balance_dt"))
    )
    
    fact2 = build_fact2(trade_df, acct_map, prod_map, bank_map)
    print(f"    fact2 rows: {fact2.count():,}")

    print("--- Phase 2: Per-Month Variables ---")
    fact2_enriched = build_fact2_enriched(fact2)
    fact2_enriched.cache()

    print("--- Phase 3: Per-Account Aggregation ---")
    account_details = build_account_details(fact2_enriched)
    account_details.cache()
    print(f"    account rows: {account_details.count():,}")

    print("--- Phase 4: Global Attribute Calculation ---")
    global_attrs = build_global_attrs(account_details, fact2_enriched)

    print("--- Phase 5A: Trigger Eligibility ---")
    st3_eligible, st2_eligible = compute_trigger_eligibility(account_details)

    print("--- Phase 5B: Scorecard Selection ---")
    global_attrs_triggered = apply_score_trigger(global_attrs, st3_eligible, st2_eligible)

    print("--- Phase 5C: Final Score ---")
    final_df = compute_final_score(global_attrs_triggered)

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
    output_cols = [
        "cust_id",
        "scorecard_name", "final_score", "score_breakdown",   # ← ADD THIS LINE
        "is_eligible_for_st2", "is_eligible_for_st3",         # ← ADD THIS LINE
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
        # ── Account opening velocity ─────────────────────────────────────────
        "nbr_accts_open_l6m",    "nbr_accts_open_7to12m",    "nbr_accts_open_l12m_wo_cc",
        "open_cnt_0_6_by_7_12_bin",
        # ── DPD (pipeline alias — rename to final_list name in post-process) ─
        "max_dpd_uns_l36m",      "max_dpd_l30m",
        "max_dpd_UNS_L6_M",      "max_dpd_UNS_6_12_M",       "max_dpd_sec0_live",
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
        "util_l3m_uns_tot",      "util_l6m_uns_tot",          "util_l12m_uns_tot",
        "util_l3m_exc_cc_live",
        "nbr_cc40l6m_tot_accts_36",
        # ── Sanction amounts ─────────────────────────────────────────────────
        # max_sanc_amt_sec = all Secured accounts, max_sanc_amt_secmov_real = SecMov only
        "max_sanc_amt_ever",     "max_sanc_amt_sec",  "max_sanc_amt_secmov_real",
        "max_sanc_amt_pl",       "max_sanc_amt_al",
        "max_sanc_amt_tw",       "max_sanc_amt_cl",
        "sum_sanc_amt_uns",
        # ── Outflows ─────────────────────────────────────────────────────────
        "total_outflow_wo_cc",   "HL_LAP_outflow",            "AL_TW_outflow",
        "PL_outflow",            "CD_outflow",
        # ── Balance trends ───────────────────────────────────────────────────
        "totconsinc_bal10_exc_cc_25m",  "totconsdec_bal_tot_36m",
        "totconsinc_bal10_exc_cc_7m",   "totconsinc_bal5_exc_cc_7m",
        "consinc_bal10_exc_cc_25m",     "consinc_bal10_tot_7m",
        "totconsinc_util1_tot_7m",      "consinc_bal5_cc_13m",
        # ── Balance ratios ───────────────────────────────────────────────────
        "bal_amt_12_24",         "balance_amt_0_6_by_7_12",   "avg_b_1to6_by_7_12_agri",
        "delinq_bal_0_12_by_13_24",     "delinq_bal_13_24_by_25_36",
        "live_cnt_6_12",            # renamed to live_cnt_0_6_by_7_12 — raw double ratio matches GT
        # ── Frequency ────────────────────────────────────────────────────────
        "freq_between_accts_all","freq_between_accts_unsec_wo_cc",
        "freq_between_installment_trades",
        # ── DPD counts & timing ──────────────────────────────────────────────
        "nbr_0_0m_all",          "nbr_0_24m_live",            "max_nbr_0_24m_uns",
        "mon_since_max_bal_124m_uns",
        # ── Latest account ───────────────────────────────────────────────────
        "latest_account_type",
    ]

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


    # ── Rename pipeline aliases to match final_list column names ─────────────
    rename_map = {
        "max_dpd_uns_l36m":         "max_dpd__UNS_L36M",
        "max_dpd_l30m":             "max_dpd_L30M",
        "max_dpd_UNS_L6_M":         "max_dpd__UNS__L6M",
        "max_dpd_UNS_6_12_M":       "max_dpd__UNS_L_6_12_M",
        "max_dpd_sec0_live":        "max_dpd__SEC__0_live",
        "max_sanc_amt_ever":        "max_sanc_amt",
        # max_sanc_amt_sec stays as-is; _real copy is the true SecMov-only value
        "max_sanc_amt_secmov_real": "max_sanc_amt_secmov",
        "total_outflow_wo_cc":      "total_monthly_outflow_wo_cc",
        "live_cnt_6_12":            "live_cnt_0_6_by_7_12",
        # util_l3m_uns_tot stays as-is; util_l3m_exc_cc_live produced directly by util_exc_cc_df
        # util_l3m_exc_cc_live now produced directly by util_exc_cc_df (ratio-of-sums ~isCC & isLive)
        # "util_l3m_uns_tot": "util_l3m_exc_cc_live",  <- REMOVED: separate df handles this now
        # "util_l6m_all_tot":         "avg_util_l6m_all_tot",
        "open_cnt_0_6_by_7_12_bin": "open_cnt_0_6_by_7_12",
        "bal_amt_12_24":            "balance_amt_0_12_by_13_24",
        "mon_since_max_bal_124m_uns": "mon_since_max_bal_l24m_uns",
    }

    result = result.toDF(*[rename_map.get(c, c) for c in result.columns])

    # FIX OOM: coalesce to single partition before collect.
    # Many small partitions each need separate JVM allocation → heap fragmentation.
    result = result.coalesce(1)

    result.printSchema()

    # ── Output: RDD collect → pandas → CSV (Java 25 local workaround) ──────────
    # Spark's write.csv() calls Hadoop Subject.getSubject() which crashes on
    # Java 25 in local mode — same root cause as the original pd.read_csv workaround.
    # Solution: collect result rows to driver via RDD, write with pandas.
    # This is safe because the result has one row per customer (not per trade row).
    # For EMR/S3: switch to result.coalesce(1).write.option("header","true").csv(path)
    # GENERIC FIX: use a timestamped filename so multiple runs don't overwrite each other
    import datetime as _dt
    _ts = "to"
    flat_csv = os.path.join(output_dir, f"scorecard_output_{_ts}.csv")
    _out_cols = result.columns
    # Safe collect: Arrow is disabled (crashes on Java 25).
    # rdd.collect() works because: (a) result has only 1 row per customer,
    # (b) checkpoint above broke the deep 30-join lineage so Spark doesn't
    # re-materialise the whole plan during collect.
    _out_rows = result.rdd.collect()
    pdf = pd.DataFrame(_out_rows, columns=_out_cols)
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

    result = run_pipeline(trade_df, acct_map, prod_map, bank_map, OUTPUT_DIR)
    
    print("\n=== Pipeline complete. ===")


# total_outflow_wo_cc




# >>LEVEL 1 — directly computed from open_dt / closed_dt
# >>>MOB (months on book) — from open_dt
# mob
# max_mob_all_36
# min_mob_all_36
# avg_mob_reg_36
# max_mob_cc_36
# min_mob_uns_wo_cc_36
# min_mob_agri_live_36
# max_mob_comuns_live_36
# mon_since_last_acct_open
# isLiveAccount flag — from closed_dt
# isLiveAccount
# nbr_live_accts_36
# nbr_live_accts_12
# nbr_cc_live_accts_36
# nbr_hl_live_accts_36
# nbr_al_live_accts_36
# nbr_pl_live_accts_36
# nbr_tw_live_accts_36
# nbr_gl_live_accts_36
# nbr_agri_live_accts_36
# nbr_comsec_live_accts_36
# nbr_uns_wo_cc_live_accts_36


# Fixed
# max_dpd_L30M
# max_simul_pl_cd
# max_simul_unsec
# nbr_comuns_tot_accts_36
# totconsdec_bal_tot_36m
# totconsinc_util1_tot_7m
# max_simul_pl_cd
# daut
# balance_amt_0_12_by_13_24

# sum_sanc_amt_uns
#avg_util_l6m_all_tot
# consinc_bal5_cc_13m
# util_l12m_uns_tot

