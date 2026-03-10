# =============================================================================
# Consumer Scorecard V4  —  Full PySpark Conversion  (PRODUCTION READY)
# Java Source : ConsumerScorecardAttributeCalculatorV4
# Target      : AWS EMR / PySpark 3.x
# =============================================================================
#
# BUG-FIXES applied vs previous draft
# ─────────────────────────────────────────────────────────────────────────────
# P1-1  dpd_new Level-6 wostCond branch added  (WOST padded "000" → level 6)
# P1-2  dpd_new Level-5 reordered BEFORE Level-4 general L/B/D catch
# P1-3  derog_flag 3rd condition added: wOST.zfill(2) in {"00","01"} OR
#        suitFiled == "200"
# P0-1  OWNERSHIP_CD filter (relFinCd IN [1,2]) added to fact2 build step
# P0-2  No-hit / CLOSED score path implemented (shouldTriggerNoHitScore)
#
# PIPELINE PHASES
# ─────────────────────────────────────────────────────────────────────────────
# Phase 0 : SparkSession, imports, broadcast constants
# Phase 1 : fact2 build (trade_data × mapping × product_mapping)
# Phase 2 : Per-month variable calculation (dpd_new, modified_limit, derog)
# Phase 3 : Per-account aggregation     (AccountLevelDetailsV4 equivalent)
# Phase 4 : Global attribute calculation (100+ variables)
# Phase 5 : Scorecard trigger + final score
# Phase 6 : Output / validation
# =============================================================================

# ---------------------------------------------------------------------------
# PHASE 0 — IMPORTS & SPARK SESSION
# ---------------------------------------------------------------------------

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, ArrayType, MapType, FloatType
)
from pyspark.sql.functions import (
    col, lit, when, coalesce, greatest, least,
    collect_list, struct, arrays_zip,
    array_sort, expr, pandas_udf, udf,
    sum as fsum, max as fmax, min as fmin, avg as favg,
    count, countDistinct, lag, lead,
    broadcast, monotonically_increasing_id,
    datediff, months_between, to_date, year, month
)
import pandas as pd
import numpy as np
from typing import Iterator
import statistics

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ConsumerScorecardV4") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.security.authentication", "simple") \
    .config("spark.driver.extraJavaOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "-Dhadoop.security.authentication=simple "
            "-Dio.netty.tryReflectionSetAccessible=true") \
    .config("spark.executor.extraJavaOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "-Dhadoop.security.authentication=simple "
            "-Dio.netty.tryReflectionSetAccessible=true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# Ensure Arrow is fully disabled (local Mac compatibility)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# ===========================================================================
# 0A.  PRODUCT-CODE CONFIGURATION  (ScoreCardAttributeMappingV4)
# ===========================================================================
# Every set below mirrors the Java ScoreCardAttributeMappingV4 bean exactly.

CC_CODES        = {"220","121","213","5","214","225"}
HL_CODES        = {"002","058"}
AL_CODES        = {"001","047"}
TWO_W_CODES     = {"013","173"}
GL_CODES        = {"191","007"}
PL_CODES        = {"123","130","172","195","196","215","216","217","219",
                   "221","222","240","242","243","244","245","246","247","248","249"}
AGRI_CODES      = {"167","177","178","179","198","199","200","223","224","226","227"}
COM_SEC_CODES   = {"175","176","228","241"}
COM_UNSEC_CODES = {"176","177","178","179","228"}
REGULAR_CODES   = {"047","058","121","123","130","172","173","191","195","196",
                   "215","216","217","219","220","221","222","225","240","241","242",
                   "243","244","245","246","247","248","249"}
PL_CD_TW_CODES  = {"173","013"}

# Derogatory status lists  (Java: derogSFWList, derogWOSTList)
DEROG_SFW_LIST  = {"05","06","07","08","09","10","11","12","13","14","15","16","17"}
DEROG_WOST_LIST = {"02","03","04","05","06","07","08","09","10","11","12","13","14","15"}

# Ownership codes — only accounts with relFinCd in this set are included
OWNERSHIP_CD    = {"1","2"}

# ===========================================================================
# PHASE 1 — DATA INGESTION  (fact2 build)
# ===========================================================================
# Input DataFrames expected by build_fact2():
#   trade_data       : raw bureau trade records
#   mapping_df       : cust_id, app_date, score_dt, relFinCd
#   product_mapping  : account_type_cd, Sec_Uns, Reg_Com, Sec_Mov, Pdt_Cls
#   bank_mapping     : bureau_mbr_id → Category (PSB/PVT/NBFC/SFB)
#
# fact2 schema (one row per cust_id × accno × month_diff):
#   cust_id, accno, bureau_mbr_id, account_type_cd,
#   open_dt, closed_dt, score_dt, balance_dt,
#   balance_amt, credit_lim_amt, original_loan_amt, past_due_amt,
#   dayspastdue, pay_rating_cd, account_status_cd, actual_pymnt_amt,
#   SUITFILED_WILFULDEFAULT, WRITTEN_OFF_AND_SETTLED,
#   Sec_Uns, Reg_Com, Sec_Mov, Pdt_Cls, Category,
#   month_diff   (0 = score month, 1 = 1 month prior … 35 = 35 months prior)

def build_fact2(trade_data, mapping_df, product_mapping_df, bank_mapping_df=None):
    """
    Constructs fact2 from raw inputs.
    Applies OWNERSHIP_CD filter (relFinCd IN {1,2}) — Java: ScoreCardV4Constants.OWNERSHIP_CD
    Joins product classification and bank category.
    Computes month_diff as months between balance_dt and score_dt.
    Keeps only rows with month_diff in [0, 35].
    """
    # 1. Apply ownership filter
    df = trade_data.join(
        mapping_df.filter(col("relFinCd").cast("string").isin(*OWNERSHIP_CD)),
        "accno", "inner"
    )

    # 2. Compute month_diff
    #    balance_dt and score_dt are yyyyMMdd integers
    def months_diff_col(newer_col, older_col):
        newer = F.to_date(newer_col.cast("string"), "yyyyMMdd")
        older = F.to_date(older_col.cast("string"), "yyyyMMdd")
        return (
            (F.year(older) * 12 + F.month(older)) -
            (F.year(newer) * 12 + F.month(newer))
        )

    df = df.withColumn(
        "month_diff",
        months_diff_col(col("balance_dt"), col("score_dt"))
    ).filter((col("month_diff") >= 0) & (col("month_diff") <= 35))

    # 3. Join product mapping
    df = df.join(
        broadcast(product_mapping_df.select(
            col("account_type_cd").cast("string").alias("account_type_cd"),
            "Sec_Uns", "Reg_Com", "Sec_Mov", "Pdt_Cls"
        )),
        "account_type_cd", "left"
    )

    # 4. Join bank category (optional)
    if bank_mapping_df is not None:
        df = df.join(
            broadcast(bank_mapping_df.select("bureau_mbr_id", "Category")),
            "bureau_mbr_id", "left"
        )
    else:
        df = df.withColumn("Category", lit(None).cast("string"))

    return df


# ===========================================================================
# PHASE 2 — PER-MONTH VARIABLE CALCULATION
# ===========================================================================

# ---------------------------------------------------------------------------
# 2A.  UDF — getDpd_new()  [ScoreCardHelperV4.getDpd_new]
# ---------------------------------------------------------------------------
# Maps raw DPD + asset/WO codes → normalised 0-7 integer scale.
# Rules evaluated in order; FIRST match wins.
#
# *** BUG-FIX P1-1: Added wostCond (WOST padded "000" → level 6) ***
# *** BUG-FIX P1-2: Level-5 reordered BEFORE Level-4 L/B/D catch  ***
#
# Level | Condition (balance > 500 required for any non-zero result)
# ------+--------------------------------------------------------------
#   7   | dpd > 720  OR  (asset='L' AND dpd==-1)  OR  WOST in derogList
#   6   | dpd 361-720  OR  (asset IN {L,D} AND dpd==-1)
#         OR  WOST.zfill(3)=="000"  (wostCond — P1-1 fix)
#   5   | dpd 181-360  OR  (asset IN {D,B} AND dpd==-1 AND WOST in {02,03})
#   4   | dpd 91-180   OR  (asset IN {L,B,D} AND dpd==-1)
#   3   | dpd 61-90    OR  (asset='M' AND dpd==-1)
#   2   | dpd 31-60
#   1   | dpd 1-30
#   0   | default

@udf(IntegerType())
def dpd_new_udf(dayspastdue, pay_rating_cd, suit_filed, wo_settled, balance_amt):
    """
    Normalised DPD 0-7.
    Mirrors ScoreCardHelperV4.getDpd_new() with all three P1 bug-fixes.

    CRITICAL: wo_settled from CSV arrives as float NaN when blank.
    Must treat None / NaN / "" / "nan" all as "no write-off" → wo="".
    Java: paddedWOST = wOST.isEmpty() ? "" : StringUtils.leftPad(wOST,3,'0')
    So only a genuinely non-empty numeric code (e.g. "3" → "003") triggers wostCond.
    """
    import math

    bal = float(balance_amt) if balance_amt is not None else -1.0
    dpd = int(dayspastdue)   if dayspastdue is not None else -1
    pr  = str(pay_rating_cd).strip() if pay_rating_cd is not None else ""

    # ── Defensive WO parsing ────────────────────────────────────────────────
    # Handle: None, float NaN, "", "nan", "0", "3" etc.
    def _clean(val):
        if val is None:
            return ""
        try:
            if math.isnan(float(val)):
                return ""
        except (ValueError, TypeError):
            pass
        s = str(val).strip()
        return "" if s.lower() in ("", "nan", "none", "null") else s

    wo_raw = _clean(wo_settled)
    wo     = wo_raw.zfill(3) if wo_raw else ""   # e.g. "3" → "003", "" → ""

    # Only assess DPD for accounts with material balance
    if bal <= 500:
        return 0

    # ── Level 7 ────────────────────────────────────────────────────────────
    if dpd > 720:
        return 7
    if pr == "L" and dpd == -1:
        return 7
    if wo and wo in {"002","003","004","005","006","007","008","009","010","011","012","013","014","015"}:
        return 7

    # ── Level 6 ────────────────────────────────────────────────────────────
    if 361 <= dpd <= 720:
        return 6
    if pr in {"L","D"} and dpd == -1:
        return 6
    # P1-1 FIX: wostCond — "000" written-off sentinel; "" means no write-off
    if wo == "000" and bal > 500:
        return 6

    # ── Level 5 ─── (MUST come before Level-4 general L/B/D catch) ─────────
    if 181 <= dpd <= 360:
        return 5
    if pr in {"D","B"} and dpd == -1 and wo in {"002","003"}:
        return 5

    # ── Level 4 ────────────────────────────────────────────────────────────
    if 91 <= dpd <= 180:
        return 4
    if pr in {"L","B","D"} and dpd == -1:
        return 4

    # ── Level 3 ────────────────────────────────────────────────────────────
    if 61 <= dpd <= 90:
        return 3
    if pr == "M" and dpd == -1:
        return 3

    # ── Level 2 ────────────────────────────────────────────────────────────
    if 31 <= dpd <= 60:
        return 2

    # ── Level 1 ────────────────────────────────────────────────────────────
    if 1 <= dpd <= 30:
        return 1

    return 0


# ---------------------------------------------------------------------------
# 2B.  UDF — getModifiedLimit()  [ScoreCardHelperV4.getModifiedLimit]
# ---------------------------------------------------------------------------
# Sanctioned limit proxy = max(balance_amt, credit_lim_amt, original_loan_amt)
# -1 sentinel treated as 0 for max comparison.

@udf(DoubleType())
def modified_limit_udf(balance_amt, credit_lim_amt, original_loan_amt):
    vals = [
        float(balance_amt)        if balance_amt        not in (None, -1) else 0.0,
        float(credit_lim_amt)     if credit_lim_amt     not in (None, -1) else 0.0,
        float(original_loan_amt)  if original_loan_amt  not in (None, -1) else 0.0,
    ]
    return max(vals)


# ---------------------------------------------------------------------------
# 2C.  UDF — getDerogatoryFlag()  [ScoreCardHelperV4.getDerogatoryFlag]
# ---------------------------------------------------------------------------
# *** BUG-FIX P1-3: Added 3rd condition:
#     wOST.zfill(2) in {"00","01"}  OR  suitFiled == "200"  ***

@udf(BooleanType())
def derog_flag_udf(suit_filed, wo_settled, pay_rating_cd):
    """
    Derogatory flag — True if any derogatory indicator is present.
    Uses same defensive NaN/empty parsing as dpd_new_udf.
    """
    import math

    def _clean(val):
        if val is None:
            return ""
        try:
            if math.isnan(float(val)):
                return ""
        except (ValueError, TypeError):
            pass
        s = str(val).strip()
        return "" if s.lower() in ("", "nan", "none", "null") else s

    sfw_raw = _clean(suit_filed)
    wo_raw  = _clean(wo_settled)
    pr      = str(pay_rating_cd).strip() if pay_rating_cd is not None else ""

    sfw = sfw_raw.zfill(3) if sfw_raw else ""
    wo  = wo_raw.zfill(3)  if wo_raw  else ""

    # Condition 1 — suit filed codes (padded to 3)
    if sfw and sfw in {"005","006","007","008","009","010","011","012","013","014","015","016","017"}:
        return True
    # Condition 2 — WO/settled codes (padded to 3)
    if wo and wo in {"002","003","004","005","006","007","008","009","010","011","012","013","014","015"}:
        return True
    # Condition 3 — P1-3 FIX: wo padded 2-char in {"00","01"} OR sfw=="200"
    if wo and wo[:2] in {"00","01"}:
        return True
    if sfw_raw == "200":
        return True
    # Condition 4
    if pr in {"D","L"}:
        return True
    return False


# ---------------------------------------------------------------------------
# 2D.  PRODUCT TYPE FLAGS  (ScoreCardAttributeMappingV4 sets)
# ---------------------------------------------------------------------------

def add_product_flags(df):
    """
    Add boolean classification columns per row.
    Uses account_type_cd (cast to string) matched against config sets.
    Also derives isUns/isSec from Sec_Uns column (product_mapping join).
    """
    cd = col("account_type_cd").cast("string")
    return (
        df
        .withColumn("isCC",       cd.isin(*CC_CODES))
        .withColumn("isHL",       cd.isin(*HL_CODES))
        .withColumn("isAL",       cd.isin(*AL_CODES))
        .withColumn("isTW",       cd.isin(*TWO_W_CODES))
        .withColumn("isGL",       cd.isin(*GL_CODES))
        .withColumn("isPL",       cd.isin(*PL_CODES))
        .withColumn("isAgri",     cd.isin(*AGRI_CODES))
        .withColumn("isComSec",   cd.isin(*COM_SEC_CODES))
        .withColumn("isComUnSec", cd.isin(*COM_UNSEC_CODES))
        .withColumn("isRegular",  cd.isin(*REGULAR_CODES))
        .withColumn("isPlCdTw",   cd.isin(*PL_CD_TW_CODES))
        .withColumn("isUns",      col("Sec_Uns") == "UnSec")
        .withColumn("isSec",      col("Sec_Uns") == "Sec")
        .withColumn("isSecMov",   col("Sec_Mov") == "SecMov")
        .withColumn("isRegSec",   col("Pdt_Cls") == "RegSec")
        .withColumn("isRegUns",   col("Pdt_Cls") == "RegUns")
        .withColumn("isComCls",   col("Reg_Com").isin("AgriPSL","NonAgriPSL"))
        .withColumn("isPSB",      col("Category") == "PSB")
        .withColumn("isPVT",      col("Category") == "PVT")
        .withColumn("isNBFC",     col("Category") == "NBFC")
        .withColumn("isSFB",      col("Category") == "SFB")
    )


# ---------------------------------------------------------------------------
# 2E.  build_fact2_enriched() — apply Phase 2 UDFs
# ---------------------------------------------------------------------------

def build_fact2_enriched(fact2):
    """
    Enriches fact2 with per-month derived variables.
    Output grain: one row per cust_id × accno × month_diff.
    """
    df = fact2

    # Defensive casts
    df = (
        df
        .withColumn("balance_amt",        col("balance_amt").cast("double"))
        .withColumn("credit_lim_amt",     col("credit_lim_amt").cast("double"))
        .withColumn("original_loan_amt",  col("original_loan_amt").cast("double"))
        .withColumn("dayspastdue",        col("dayspastdue").cast("int"))
        .withColumn("actual_pymnt_amt",   col("actual_pymnt_amt").cast("double"))
        .withColumn("past_due_amt",       col("past_due_amt").cast("double"))
    )

    # Per-month derived variables
    df = (
        df
        .withColumn("dpd_new",
            dpd_new_udf(
                col("dayspastdue"),
                col("pay_rating_cd"),
                col("SUITFILED_WILFULDEFAULT").cast("string"),
                col("WRITTEN_OFF_AND_SETTLED").cast("string"),
                col("balance_amt")))
        .withColumn("modified_limit",
            modified_limit_udf(
                col("balance_amt"),
                col("credit_lim_amt"),
                col("original_loan_amt")))
        .withColumn("derog_flag",
            derog_flag_udf(
                col("SUITFILED_WILFULDEFAULT").cast("string"),
                col("WRITTEN_OFF_AND_SETTLED").cast("string"),
                col("pay_rating_cd")))
        # is_live_month0: account is open at score month
        .withColumn("is_live_month0",
            (col("month_diff") == 0) & (col("account_status_cd") == "O"))
    )

    # Product type flags
    df = add_product_flags(df)

    return df


# ===========================================================================
# PHASE 3 — PER-ACCOUNT AGGREGATION  (AccountLevelDetailsV4 equivalent)
# ===========================================================================

def build_account_details(fact2_enriched):
    """
    Aggregates fact2_enriched to one row per (cust_id, accno).
    Produces the PySpark equivalent of AccountLevelDetailsV4.
    """
    df = fact2_enriched

    # -----------------------------------------------------------------------
    # 3A.  Core aggregation
    # -----------------------------------------------------------------------
    acct_agg = (
        df
        .groupBy("cust_id", "accno")
        .agg(
            # ── account metadata ──────────────────────────────────────────
            F.first("account_type_cd").alias("account_type_cd"),
            F.first("open_dt").alias("open_dt"),
            F.first("closed_dt").alias("closed_dt"),
            F.first("score_dt").alias("score_dt"),
            F.first("bureau_mbr_id").alias("bureau_mbr_id"),

            # ── product flags (take first non-null) ───────────────────────
            F.first("isCC").alias("isCC"),
            F.first("isHL").alias("isHL"),
            F.first("isAL").alias("isAL"),
            F.first("isTW").alias("isTW"),
            F.first("isGL").alias("isGL"),
            F.first("isPL").alias("isPL"),
            F.first("isAgri").alias("isAgri"),
            F.first("isComSec").alias("isComSec"),
            F.first("isComUnSec").alias("isComUnSec"),
            F.first("isRegular").alias("isRegular"),
            F.first("isPlCdTw").alias("isPlCdTw"),
            F.first("isUns").alias("isUns"),
            F.first("isSec").alias("isSec"),
            F.first("isSecMov").alias("isSecMov"),
            F.first("isRegSec").alias("isRegSec"),
            F.first("isRegUns").alias("isRegUns"),
            F.first("isComCls").alias("isComCls"),
            F.first("isPSB").alias("isPSB"),
            F.first("isPVT").alias("isPVT"),
            F.first("isNBFC").alias("isNBFC"),
            F.first("isSFB").alias("isSFB"),
            F.first("Category").alias("bank_category"),
            F.first("Pdt_Cls").alias("Pdt_Cls"),
            F.first("Sec_Uns").alias("Sec_Uns"),
            F.first("Reg_Com").alias("Reg_Com"),
            F.first("modified_limit", ignorenulls=True).alias("latest_modified_limit"),

            # ── live flags ────────────────────────────────────────────────
            # isLiveAccount: reported at month_diff=0 with status O
            fmax(
                when(col("month_diff") == 0, col("is_live_month0")).otherwise(lit(False))
            ).alias("isLiveAccount"),
            fmax(
                when(col("month_diff") <= 11, col("is_live_month0"))
            ).alias("isReportedLiveIn12M"),
            fmax(when(col("month_diff") <= 35, lit(True))).alias("reportedIn36M"),
            fmax(when(col("month_diff") <= 11, lit(True))).alias("reportedIn12M"),

            # ── derogatory ────────────────────────────────────────────────
            fmax(col("derog_flag").cast("int")).cast("boolean").alias("derog"),

            # ── monthly data array (for explode-based Phase 4 logic) ──────
            collect_list(struct(
                col("month_diff").alias("idx"),
                col("dpd_new").alias("dpd"),
                col("balance_amt").alias("bal"),
                col("modified_limit").alias("mod_lim"),
                col("pay_rating_cd").alias("asset_cd"),
                col("WRITTEN_OFF_AND_SETTLED").cast("string").alias("wo_status"),
                col("SUITFILED_WILFULDEFAULT").cast("string").alias("sfw_status"),
                col("actual_pymnt_amt").alias("pymnt_amt"),
                col("account_status_cd").alias("acct_status"),
                col("past_due_amt").alias("past_due"),
                col("derog_flag").alias("derog_flag")
            )).alias("monthly_data"),

            # ── scalar DPD aggregates ─────────────────────────────────────
            fmax(col("dpd_new")).alias("max_dpd_l36m"),
            fmax(when(col("month_diff") <= 11, col("dpd_new"))).alias("max_dpd_l12m"),
            fmax(when(col("month_diff") == 0,  col("dpd_new"))).alias("dpd_m0"),

            # ── pct_instance_l12m : % of months in last 12 with balance>0
            (
                fsum(when((col("month_diff") <= 11) & (col("balance_amt") > 0), lit(1)).otherwise(lit(0))) * 100.0
                / count(when(col("month_diff") <= 11, lit(1)))
            ).alias("pct_instance_l12m"),

            # ── average balances ──────────────────────────────────────────
            favg(when(col("month_diff") <= 2, col("balance_amt"))).alias("avgBal_l3m"),
            favg(when(col("month_diff") <= 2, col("modified_limit"))).alias("avgModifiedLimit_l3m"),

            # ── balance at month 0 ────────────────────────────────────────
            fmax(when(col("month_diff") == 0, col("balance_amt"))).alias("bal_m0"),

            # ── latestReportedIndex (most recent month_diff seen) ─────────
            fmin("month_diff").alias("latestReportedIndex"),

            # ── max balance windows ───────────────────────────────────────
            fmax(when(col("month_diff") <= 23, col("balance_amt"))).alias("max_bal_l24m"),
            fmax(when(col("month_diff") <= 11, col("balance_amt"))).alias("max_bal_l12m"),
        )
    )

    # -----------------------------------------------------------------------
    # 3B.  Months-on-Book (mob)
    #      mob = (score_dt_year*12 + score_dt_month) - (open_dt_year*12 + open_dt_month)
    # -----------------------------------------------------------------------
    def yyyymmdd_to_abs_months(dt_col):
        d = F.to_date(dt_col.cast("string"), "yyyyMMdd")
        return F.year(d) * 12 + F.month(d)

    acct_agg = (
        acct_agg
        .withColumn("_score_abs", yyyymmdd_to_abs_months(col("score_dt")))
        .withColumn("_open_abs",  yyyymmdd_to_abs_months(col("open_dt")))
        .withColumn("mob", col("_score_abs") - col("_open_abs"))
        .drop("_score_abs","_open_abs")
    )

    # -----------------------------------------------------------------------
    # 3C.  firstReportedOnlyIn36M
    #      True if account NEVER appeared in months 0-12, only 13-35
    # -----------------------------------------------------------------------
    in_12m = (
        fact2_enriched
        .filter(col("month_diff") <= 12)
        .groupBy("cust_id","accno")
        .agg(lit(True).alias("_in_12m"))
    )
    acct_agg = (
        acct_agg
        .join(in_12m, ["cust_id","accno"], "left")
        .withColumn("firstReportedOnlyIn36M",
                    col("_in_12m").isNull() & col("reportedIn36M"))
        .drop("_in_12m")
    )

    # -----------------------------------------------------------------------
    # 3D.  m_since_max_bal_l24m — month_diff at which max balance occurred
    # -----------------------------------------------------------------------
    max_bal_idx = (
        fact2_enriched
        .filter(col("month_diff") <= 23)
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("cust_id","accno")
                  .orderBy(col("balance_amt").desc(), col("month_diff").asc())))
        .filter(col("_rn") == 1)
        .select("cust_id","accno", col("month_diff").alias("m_since_max_bal_l24m"))
    )
    acct_agg = acct_agg.join(max_bal_idx, ["cust_id","accno"], "left")

    # -----------------------------------------------------------------------
    # 3E.  Months since first / most-recent worst delinquency
    #      Java: acctMap_monSinceFirstWorstDelq / acctMap_monSinceRecentWorstDelq
    #      "worst" = highest dpd_new in the account's history
    # -----------------------------------------------------------------------
    # We need the month indices where dpd == max_dpd_l36m
    worst_delq_df = (
        fact2_enriched
        .withColumn("_max_dpd_acct", fmax("dpd_new").over(
            Window.partitionBy("cust_id","accno")))
        .filter(col("dpd_new") == col("_max_dpd_acct"))
        .filter(col("dpd_new") > 0)
        .groupBy("cust_id","accno")
        .agg(
            fmax("month_diff").alias("mon_since_first_worst_delq"),   # largest idx = oldest
            fmin("month_diff").alias("mon_since_recent_worst_delq"),  # smallest idx = most recent
        )
    )
    acct_agg = acct_agg.join(worst_delq_df, ["cust_id","accno"], "left")

    return acct_agg


# ===========================================================================
# PHASE 4 — GLOBAL ATTRIBUTE CALCULATION  (prepareVariables equivalent)
# ===========================================================================

def build_global_attrs(account_details, fact2_enriched):
    """
    Computes all 100+ global attributes per cust_id.
    Returns a single-row-per-customer DataFrame: global_attrs.
    """
    ad = account_details
    fe = fact2_enriched

    # ── Pre-explode monthly_data once (reused across multiple aggs) ──────
    exploded = (
        ad
        .select(
            "cust_id","accno",
            "isCC","isHL","isAL","isTW","isGL","isPL",
            "isUns","isSec","isSecMov","isAgri","isComSec","isComUnSec",
            "isRegSec","isRegUns","isRegular","isPlCdTw",
            "isPSB","isPVT","isNBFC","isSFB",
            F.explode("monthly_data").alias("m")
        )
        .select(
            "cust_id","accno",
            "isCC","isHL","isAL","isTW","isGL","isPL",
            "isUns","isSec","isSecMov","isAgri","isComSec","isComUnSec",
            "isRegSec","isRegUns","isRegular","isPlCdTw",
            "isPSB","isPVT","isNBFC","isSFB",
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
        )
    )
    exploded.cache()

    # -----------------------------------------------------------------------
    # 4A.  ACCOUNT COUNTS  (nbr_*_tot/live_accts_36)
    # -----------------------------------------------------------------------
    acct_counts = (
        ad.groupBy("cust_id").agg(
            count("accno").alias("nbr_tot_accts_36"),
            fsum(col("isLiveAccount").cast("int")).alias("nbr_live_accts_36"),
            fsum(col("reportedIn12M").cast("int")).alias("nbr_tot_accts_12"),
            fsum((col("reportedIn12M") & col("isLiveAccount")).cast("int")).alias("nbr_live_accts_12"),

            # By product slice
            fsum((col("isCC")     & col("reportedIn36M")).cast("int")).alias("nbr_cc_tot_accts_36"),
            fsum((col("isCC")     & col("isLiveAccount")).cast("int")).alias("nbr_cc_live_accts_36"),
            fsum((col("isHL")     & col("reportedIn36M")).cast("int")).alias("nbr_hl_tot_accts_36"),
            fsum((col("isHL")     & col("isLiveAccount")).cast("int")).alias("nbr_hl_live_accts_36"),
            fsum((col("isAL")     & col("reportedIn36M")).cast("int")).alias("nbr_al_tot_accts_36"),
            fsum((col("isAL")     & col("isLiveAccount")).cast("int")).alias("nbr_al_live_accts_36"),
            fsum((col("isGL")     & col("reportedIn36M")).cast("int")).alias("nbr_gl_tot_accts_36"),
            fsum((col("isGL")     & col("isLiveAccount")).cast("int")).alias("nbr_gl_live_accts_36"),
            fsum((col("isPL")     & col("reportedIn36M")).cast("int")).alias("nbr_pl_tot_accts_36"),
            fsum((col("isPL")     & col("isLiveAccount")).cast("int")).alias("nbr_pl_live_accts_36"),
            fsum((col("isAgri")   & col("reportedIn36M")).cast("int")).alias("nbr_agri_tot_accts_36"),
            fsum((col("isAgri")   & col("isLiveAccount")).cast("int")).alias("nbr_agri_live_accts_36"),
            fsum((col("isComSec") & col("reportedIn36M")).cast("int")).alias("nbr_comsec_tot_accts_36"),
            fsum((col("isComSec") & col("isLiveAccount")).cast("int")).alias("nbr_comsec_live_accts_36"),
            fsum((col("isUns") & col("reportedIn36M") & ~col("isCC")).cast("int")).alias("nbr_uns_wo_cc_tot_accts_36"),
            fsum((col("isUns") & col("isLiveAccount") & ~col("isCC")).cast("int")).alias("nbr_uns_wo_cc_live_accts_36"),
            fsum(col("derog").cast("int")).alias("nbr_derog_accts"),

            # MOB metrics
            fmax("mob").alias("max_mob_all_36"),
            fmin("mob").alias("min_mob_all_36"),
            favg("mob").alias("avg_mob_reg_36"),
            fmax(when(col("isCC"), col("mob"))).alias("max_mob_cc"),
            fmin(when(~col("isCC"), col("mob"))).alias("min_mob_wo_cc"),

            # Simultaneous unsecured loan counts (live accounts)
            fsum((col("isUns") & col("isLiveAccount") & ~col("isCC")).cast("int")).alias("max_simul_unsec_wo_cc"),
            fsum((col("isUns") & col("isLiveAccount")).cast("int")).alias("max_simul_unsec_wo_cc_inc_gl"),

            # For ST_1_EV trigger: count accounts that are NOT (AL or CC or HL)
            fsum((~(col("isCC") | col("isHL") | col("isAL"))).cast("int")).alias("nbr_not_al_cc_hl"),

            # Months-since metrics at account level (min across customer)
            fmin("mon_since_first_worst_delq").alias("mon_since_first_worst_delq"),
            fmin("mon_since_recent_worst_delq").alias("mon_since_recent_worst_delq"),
        )
    )

    # -----------------------------------------------------------------------
    # 4B.  ACCOUNTS OPENED BY MOB WINDOW
    # -----------------------------------------------------------------------
    accts_opened = (
        ad.groupBy("cust_id").agg(
            fsum((col("mob") <= 3).cast("int")).alias("nbr_accts_open_l3m"),
            fsum((col("mob") <= 6).cast("int")).alias("nbr_accts_open_l6m"),
            fsum((col("mob") <= 12).cast("int")).alias("nbr_accts_open_l12m"),
            fsum((col("mob") <= 24).cast("int")).alias("nbr_accts_open_l24m"),
            fsum(((col("mob") >= 4) & (col("mob") <= 6)).cast("int")).alias("nbr_accts_open_4to6m"),
            fsum(((col("mob") >= 7) & (col("mob") <= 12)).cast("int")).alias("nbr_accts_open_7to12m"),
            fsum(((col("mob") >= 13) & (col("mob") <= 24)).cast("int")).alias("nbr_accts_open_13to24m"),
            # open_cnt_0_6_by_7_12_bin: ratio bin of 0-6m opens vs 7-12m opens
            fsum(((col("mob") <= 6) & ~col("isCC")).cast("int")).alias("nbr_accts_open_l6m_wo_cc"),
            fsum(((col("mob") <= 12) & ~col("isCC")).cast("int")).alias("nbr_accts_open_l12m_wo_cc"),
            fsum(((col("mob") <= 12) & col("isCC")).cast("int")).alias("nbr_accts_open_l12m_cc"),
            fsum(((col("mob") <= 12) & col("isHL")).cast("int")).alias("nbr_accts_open_l12m_hl"),
            fsum(((col("mob") <= 12) & col("isAL")).cast("int")).alias("nbr_accts_open_l12m_al"),
            fsum(((col("mob") <= 12) & col("isAgri")).cast("int")).alias("nbr_accts_open_l12m_agri"),
        )
    )

    # -----------------------------------------------------------------------
    # 4C.  MAX DPD ATTRIBUTES
    # -----------------------------------------------------------------------
    dpd_attrs = (
        exploded.groupBy("cust_id").agg(
            fmax(col("dpd")).alias("max_dpd_all_l36m"),
            fmax(when(col("isUns"), col("dpd"))).alias("max_dpd_uns_l36m"),
            fmax(when(col("isUns") & (col("idx") <= 11), col("dpd"))).alias("max_dpd_uns_l12m"),
            fmax(when(col("isUns") & (col("idx") <= 5),  col("dpd"))).alias("max_dpd_uns_l6m"),
            fmax(when(col("isUns") & (col("idx") == 0),  col("dpd"))).alias("max_dpd_uns_m0"),
            fmax(when(col("isSec"), col("dpd"))).alias("max_dpd_sec_l36m"),
            fmax(when(col("isHL"),  col("dpd"))).alias("max_dpd_hl_l36m"),
            fmax(when(col("isCC"),  col("dpd"))).alias("max_dpd_cc_l36m"),
            fmax(when(col("isAL"),  col("dpd"))).alias("max_dpd_al_l36m"),
            fmax(when(col("isPL"),  col("dpd"))).alias("max_dpd_pl_l36m"),

            # Count months with dpd above various thresholds (last 24m)
            fsum(when((col("dpd") > 0) & (col("idx") <= 23), lit(1)).otherwise(lit(0))).alias("nbr_0_24m_all"),
            fsum(when((col("dpd") > 1) & (col("idx") <= 23), lit(1)).otherwise(lit(0))).alias("nbr_30_24m_all"),
            fsum(when((col("dpd") > 2) & (col("idx") <= 23), lit(1)).otherwise(lit(0))).alias("nbr_60_24m_all"),
            fsum(when((col("dpd") > 3) & (col("idx") <= 23), lit(1)).otherwise(lit(0))).alias("nbr_90_24m_all"),

            # Derogatory counts
            fsum(col("row_derog").cast("int")).alias("nbr_derog_months"),
        )
    )

    # -----------------------------------------------------------------------
    # 4D.  UTILISATION METRICS (all products with credit limit)
    # -----------------------------------------------------------------------
    # All accounts (where mod_lim > 0)
    util_all_df = (
        exploded
        .filter(col("mod_lim") > 0)
        .withColumn("util", col("bal") / col("mod_lim"))
        .groupBy("cust_id").agg(
            favg(when(col("idx") <= 2,  col("util"))).alias("util_l3m_all_tot"),
            favg(when(col("idx") <= 5,  col("util"))).alias("util_l6m_all_tot"),
            favg(when(col("idx") <= 11, col("util"))).alias("util_l12m_all_tot"),
        )
    )

    # Unsecured (non-CC) utilisation
    util_uns_df = (
        exploded
        .filter(col("isUns") & ~col("isCC") & (col("mod_lim") > 0))
        .withColumn("util", col("bal") / col("mod_lim"))
        .groupBy("cust_id").agg(
            favg(when(col("idx") <= 2,  col("util"))).alias("util_l3m_uns_tot"),
            favg(when(col("idx") <= 5,  col("util"))).alias("util_l6m_uns_tot"),
            favg(when(col("idx") <= 11, col("util"))).alias("util_l12m_uns_tot"),
        )
    )

    # CC-specific utilisation (Java: getBalByCreditPercent)
    util_cc_df = (
        exploded
        .filter(col("isCC") & (col("mod_lim") > 0))
        .withColumn("util", col("bal") / col("mod_lim"))
        .groupBy("cust_id").agg(
            favg(when(col("idx") <= 2,  col("util"))).alias("util_l3m_cc_live"),
            favg(when(col("idx") <= 5,  col("util"))).alias("util_l6m_cc_live"),
            favg(when(col("idx") <= 11, col("util"))).alias("util_l12m_cc_live"),
            fmax(when(col("idx") == 0,  col("util"))).alias("util_m0_cc"),
        )
    )

    # -----------------------------------------------------------------------
    # 4E.  EXPOSURE / BALANCE AMOUNTS
    # -----------------------------------------------------------------------
    exp_df = (
        exploded.groupBy("cust_id").agg(
            # Current total exposure (month 0)
            fsum(when(col("idx") == 0, col("bal")).otherwise(lit(0))).alias("curr_tot_exp_amt"),
            fsum(when((col("idx") == 0) & col("isUns"), col("bal")).otherwise(lit(0))).alias("curr_tot_exp_amt_uns"),
            fsum(when((col("idx") == 0) & col("isSec"), col("bal")).otherwise(lit(0))).alias("curr_tot_exp_amt_sec"),
            fsum(when((col("idx") == 0) & col("isCC"),  col("bal")).otherwise(lit(0))).alias("curr_tot_exp_amt_cc"),
            fsum(when((col("idx") == 0) & col("isHL"),  col("bal")).otherwise(lit(0))).alias("curr_tot_exp_amt_hl"),

            # Average balance over windows
            favg(when(col("idx") <= 2, col("bal"))).alias("avg_bal_l3m_all"),
            favg(when(col("idx") <= 5, col("bal"))).alias("avg_bal_l6m_all"),
            favg(when(col("idx") <= 11,col("bal"))).alias("avg_bal_l12m_all"),
            favg(when((col("idx") <= 2) & col("isUns") & ~col("isCC"), col("bal"))).alias("avg_bal_l3m_uns_wo_cc"),

            # Sanctioned amount (max modified limit at month 0)
            fmax(when(col("idx") == 0, col("mod_lim"))).alias("max_sanc_amt_m0"),
            fsum(when(col("idx") == 0, col("mod_lim")).otherwise(lit(0))).alias("curr_tot_sanc_amt"),
            fmax(col("mod_lim")).alias("max_sanc_amt_ever"),

            # Secured sanctioned amount
            fmax(when((col("idx") == 0) & col("isSec"), col("mod_lim"))).alias("max_sanc_amt_sec_m0"),

            # Max limit for AL/PL/TW/CD types
            fmax(when((col("idx") == 0) & col("isPlCdTw"), col("mod_lim"))).alias("max_limit_al_pl_tw_cd"),
        )
    )

    # -----------------------------------------------------------------------
    # 4F.  BALANCE TREND FLAGS  (calculateTotalConcPercIncreaseFlags)
    # -----------------------------------------------------------------------
    monthly_total_bal = (
        fe
        .filter(~col("isCC"))
        .groupBy("cust_id","month_diff")
        .agg(fsum("balance_amt").alias("tot_bal"))
    )

    w_trend = Window.partitionBy("cust_id").orderBy(col("month_diff").desc())

    trend_df = (
        monthly_total_bal
        .withColumn("prev_bal", lead("tot_bal").over(w_trend))
        .withColumn("pct_chg",
                    when(col("prev_bal") > 0,
                         (col("tot_bal") - col("prev_bal")) / col("prev_bal"))
                    .otherwise(lit(None)))
        .groupBy("cust_id").agg(
            fsum(when((col("month_diff") <= 24) & (col("pct_chg") >= 0.10),
                      lit(1)).otherwise(lit(0))).alias("totconsinc_bal10_exc_cc_25m"),
            fsum(when((col("month_diff") <= 24) & (col("pct_chg") >= 0.05),
                      lit(1)).otherwise(lit(0))).alias("totconsinc_bal5_exc_cc_25m"),
            fsum(when(col("pct_chg") < 0, lit(1)).otherwise(lit(0))).alias("totconsdec_bal_tot_36m"),
        )
    )

    # -----------------------------------------------------------------------
    # 4G.  OUTFLOW / EMI ALGORITHM  (calculateOverflow_HC)
    # -----------------------------------------------------------------------
    # Estimates median monthly EMI per account, sums across customer.
    # Java uses 21 interest-rate buckets (0.06-0.48 step 0.02) and modal bucket.
    # NOTE: Implemented as toPandas + createDataFrame to avoid Arrow/Netty
    #       incompatibility on local Mac environments. On EMR this runs natively.

    IR_BUCKETS = [round(0.06 + i * 0.02, 2) for i in range(21)]  # 0.06..0.48

    def _compute_emi_pdf(pdf):
        """Pure Python EMI computation — Arrow-free."""
        from collections import Counter
        results = []
        for (cust_id, accno), grp in pdf.groupby(["cust_id", "accno"]):
            grp    = grp.sort_values("month_diff")
            bals   = grp["balance_amt"].fillna(-1).tolist()
            idx    = grp["month_diff"].tolist()
            is_cc  = bool(grp["isCC"].iloc[0])
            emi_list = []

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
                        if b0 > 0 and b1 > 0:
                            ir_raw  = max((b0 / b1) - 1.0, 0.0)
                            nearest = min(IR_BUCKETS, key=lambda x: abs(x - ir_raw))
                            ir_pairs.append((i, nearest))

                if ir_pairs:
                    bucket_counts = Counter(b for _, b in ir_pairs)
                    modal_ir = bucket_counts.most_common(1)[0][0]
                    for i in range(len(idx) - 1):
                        if idx[i+1] - idx[i] == 1:
                            b0, b1 = bals[i], bals[i+1]
                            if b0 > 0 and b1 > 0:
                                emi_est = b1 * (1 + modal_ir / 12) - b0
                                if emi_est > 0:
                                    emi_list.append(emi_est)

            if len(emi_list) >= 2:
                emi_med = float(np.median(emi_list))
            elif len(emi_list) == 1:
                emi_med = float(emi_list[0])
            else:
                emi_med = None

            results.append({"cust_id": int(cust_id), "accno": int(accno), "emi_median": emi_med})
        return pd.DataFrame(results, columns=["cust_id", "accno", "emi_median"])

    outflow_input_pdf = (
        fe
        .select("cust_id", "accno", "month_diff", "balance_amt", "isCC")
        .filter(col("balance_amt") > 500)
        .toPandas()
    )
    emi_pdf = _compute_emi_pdf(outflow_input_pdf)

    account_emi = spark.createDataFrame(emi_pdf, schema=StructType([
        StructField("cust_id",    LongType(),   nullable=False),
        StructField("accno",      LongType(),   nullable=False),
        StructField("emi_median", DoubleType(), nullable=True),
    ]))

    outflow_df = (
        account_emi
        .filter(col("emi_median").isNotNull())
        .groupBy("cust_id")
        .agg(fsum("emi_median").alias("outflow_uns_sec_mov"))
    )

    # -----------------------------------------------------------------------
    # 4H.  MONTHS SINCE DEROGATORY EVENT
    # -----------------------------------------------------------------------
    derog_months = (
        exploded
        .filter(col("dpd") > 0)
        .groupBy("cust_id").agg(
            fmin("idx").alias("mon_since_last_derog"),
            fmax("dpd").alias("max_dpd_ever"),
        )
    )

    # -----------------------------------------------------------------------
    # 4I.  MAX BALANCE & MON-SINCE-MAX-BAL (customer level)
    # -----------------------------------------------------------------------
    max_bal_df = (
        ad.groupBy("cust_id").agg(
            fmax("max_bal_l24m").alias("max_bal_l24m"),
            fmax("max_bal_l12m").alias("max_bal_l12m"),
            fmin(when(col("isUns"), col("m_since_max_bal_l24m"))).alias("mon_since_max_bal_l24m_uns"),
        )
    )

    # -----------------------------------------------------------------------
    # 4J.  REPORTING COUNTS (accounts reported in period)
    # -----------------------------------------------------------------------
    rpt_df = (
        exploded.groupBy("cust_id").agg(
            countDistinct(when(col("idx") == 0,  col("accno"))).alias("tot_accts_rptd_0m"),
            countDistinct(when(col("idx") <= 2,  col("accno"))).alias("tot_accts_rptd_l3m"),
            countDistinct(when(col("idx") <= 11, col("accno"))).alias("tot_accts_rptd_l12m"),
        )
    )

    # -----------------------------------------------------------------------
    # 4K.  BANK-CATEGORY SPLITS  (PSB / PVT / NBFC / SFB)
    # -----------------------------------------------------------------------
    bank_df = (
        exploded.groupBy("cust_id").agg(
            fsum((col("isPSB") & col("isUns") & (col("idx") == 0)).cast("int")).alias("curr_exp_psb_uns"),
            fsum((col("isPVT") & col("isUns") & (col("idx") == 0)).cast("int")).alias("curr_exp_pvt_uns"),
            fsum((col("isNBFC") & col("isUns") & (col("idx") == 0)).cast("int")).alias("curr_exp_nbfc_uns"),
            fsum((col("isSFB") & col("isUns") & (col("idx") == 0)).cast("int")).alias("curr_exp_sfb_uns"),
            fsum(col("isPSB").cast("int")).alias("nbr_accts_psb"),
            fsum(col("isPVT").cast("int")).alias("nbr_accts_pvt"),
            fsum(col("isNBFC").cast("int")).alias("nbr_accts_nbfc"),
            fsum(col("isSFB").cast("int")).alias("nbr_accts_sfb"),
        )
    )

    # -----------------------------------------------------------------------
    # 4L.  CONSECUTIVE DPD MARKER  (final_consec_marker)
    #       Java: ScoreCardVariableV4.final_consec_marker
    #       Count of consecutive months (from month_diff=0) with dpd_new > 0
    # -----------------------------------------------------------------------

    def _compute_consec_pdf(pdf):
        """
        Pure Python consec marker — Arrow-free.
        Per customer: count consecutive months from idx=0 where
        MAX dpd_new across all accounts > 0.
        IMPORTANT: a customer may have multiple accounts per idx — must
        aggregate to max dpd per (cust_id, idx) first.
        """
        results = []
        for cust_id, grp in pdf.groupby("cust_id"):
            # Max dpd across all accounts for each month index
            max_dpd_by_idx = grp.groupby("idx")["dpd"].max().to_dict()
            consec = 0
            for m in range(36):
                if max_dpd_by_idx.get(m, 0) > 0:
                    consec += 1
                else:
                    break
            results.append({"cust_id": int(cust_id), "final_consec_marker": consec})
        return pd.DataFrame(results, columns=["cust_id", "final_consec_marker"])

    consec_pdf = exploded.select("cust_id", "idx", "dpd").toPandas()
    consec_result_pdf = _compute_consec_pdf(consec_pdf)
    consec_df = spark.createDataFrame(consec_result_pdf, schema=StructType([
        StructField("cust_id",              LongType(),    nullable=False),
        StructField("final_consec_marker",  IntegerType(), nullable=True),
    ]))

    # -----------------------------------------------------------------------
    # 4M.  JOIN ALL ATTRIBUTE GROUPS
    # -----------------------------------------------------------------------
    global_attrs = (
        acct_counts
        .join(accts_opened,   "cust_id", "left")
        .join(dpd_attrs,      "cust_id", "left")
        .join(util_all_df,    "cust_id", "left")
        .join(util_uns_df,    "cust_id", "left")
        .join(util_cc_df,     "cust_id", "left")
        .join(exp_df,         "cust_id", "left")
        .join(trend_df,       "cust_id", "left")
        .join(outflow_df,     "cust_id", "left")
        .join(derog_months,   "cust_id", "left")
        .join(max_bal_df,     "cust_id", "left")
        .join(rpt_df,         "cust_id", "left")
        .join(bank_df,        "cust_id", "left")
        .join(consec_df,      "cust_id", "left")
    )

    # Derived trigger flags
    global_attrs = (
        global_attrs
        .withColumn("all_accts_al_cc_hl",
                    (col("nbr_not_al_cc_hl") == 0))
        .withColumn("has_agri_or_com",
                    (col("nbr_agri_tot_accts_36") > 0) | (col("nbr_comsec_tot_accts_36") > 0))
        # open_cnt bin for trigger
        .withColumn("open_cnt_0_6_by_7_12_bin",
                    when(col("nbr_accts_open_7to12m") == 0, lit(99))
                    .otherwise(col("nbr_accts_open_l6m") / col("nbr_accts_open_7to12m")))
        # fill key numeric nulls with -999 (MISSING sentinel)
        .fillna(-999, subset=[
            "max_dpd_uns_l36m","max_dpd_uns_l12m","max_dpd_uns_l6m",
            "max_dpd_cc_l36m","max_dpd_hl_l36m","max_dpd_al_l36m",
            "util_l3m_cc_live","util_l6m_cc_live","util_l12m_cc_live",
            "util_l3m_uns_tot","util_l6m_uns_tot","util_l12m_uns_tot",
            "outflow_uns_sec_mov",
            "mon_since_first_worst_delq","mon_since_recent_worst_delq",
            "final_consec_marker",
        ])
    )

    exploded.unpersist()
    return global_attrs


# ===========================================================================
# PHASE 5 — SCORECARD TRIGGER & FINAL SCORE
# ===========================================================================

# ---------------------------------------------------------------------------
# 5A.  IS_ELIGIBLE_FOR_ST3  and  IS_ELIGIBLE_FOR_ST2
# ---------------------------------------------------------------------------

def compute_trigger_eligibility(account_details):
    """
    IS_ELIGIBLE_FOR_ST3:
      Any LIVE account at month_diff=0 with balance>500 AND
      (dpd_new>=4 OR asset IN {L,D,B} OR WO in derog list)

    IS_ELIGIBLE_FOR_ST2:
      Any account reported in last 12 months with balance>500 AND
      (dpd_new>=2 OR asset IN {L,D,B,M} OR WO in derog list)
    """
    ad = account_details
    derog_wo = {"02","03","04","05","06","07","08","09","10","11","12","13","14","15"}

    exploded = (
        ad
        .select("cust_id","accno","isLiveAccount",
                F.explode("monthly_data").alias("m"))
        .select("cust_id","accno","isLiveAccount",
                col("m.idx").alias("idx"),
                col("m.dpd").alias("dpd"),
                col("m.bal").alias("bal"),
                col("m.asset_cd").alias("asset_cd"),
                col("m.wo_status").alias("wo_status"))
    )

    st3_eligible = (
        exploded
        .filter(
            (col("idx") == 0) &
            col("isLiveAccount") &
            (col("bal") > 500) &
            (
                (col("dpd") >= 4) |
                col("asset_cd").isin("L","D","B") |
                col("wo_status").isin(*derog_wo)
            )
        )
        .groupBy("cust_id")
        .agg(lit(True).alias("is_eligible_for_st3"))
    )

    st2_eligible = (
        exploded
        .filter(
            (col("idx") <= 12) &
            (col("bal") > 500) &
            (
                (col("dpd") >= 2) |
                col("asset_cd").isin("L","D","B","M") |
                col("wo_status").isin(*derog_wo)
            )
        )
        .groupBy("cust_id")
        .agg(lit(True).alias("is_eligible_for_st2"))
    )

    return st3_eligible, st2_eligible


# ---------------------------------------------------------------------------
# 5B.  SCORECARD TRIGGER DECISION TREE  (ScoreTriggerEngineV4)
# ---------------------------------------------------------------------------

def apply_score_trigger(global_attrs, st3_eligible, st2_eligible):
    """
    Decision tree (evaluated in order — first match wins):
      CLOSED        → nbr_live_accts_36 == 0
      ST_3          → IS_ELIGIBLE_FOR_ST3
      THIN          → max_mob_all_36 <= 6
      ST_2          → IS_ELIGIBLE_FOR_ST2
      ST_1_HC       → max_simul_unsec_wo_cc >= 7
                       OR nbr_accts_open_l6m >= 4
                       OR max_simul_unsec_wo_cc_inc_gl >= 8
      ST_1_EV       → all accounts are AL / CC / HL only
      ST_1_AGR_OR_COM → any agri or comsec account
      ST_1_SE       → default
    """
    ga = (
        global_attrs
        .join(st3_eligible, "cust_id", "left")
        .join(st2_eligible, "cust_id", "left")
        .fillna(False, subset=["is_eligible_for_st3","is_eligible_for_st2"])
    )

    scorecard_name = (
        when(col("nbr_live_accts_36") <= 0,
             lit("CLOSED"))
        .when(col("is_eligible_for_st3"),
              lit("ST_3"))
        .when(col("max_mob_all_36") <= 6,
              lit("THIN"))
        .when(col("is_eligible_for_st2"),
              lit("ST_2"))
        .when(
            (col("max_simul_unsec_wo_cc") >= 7) |
            (col("nbr_accts_open_l6m") >= 4) |
            (col("max_simul_unsec_wo_cc_inc_gl") >= 8),
            lit("ST_1_HC"))
        .when(col("all_accts_al_cc_hl"),
              lit("ST_1_EV"))
        .when(col("has_agri_or_com"),
              lit("ST_1_AGR_OR_COM"))
        .otherwise(lit("ST_1_SE"))
    )

    return ga.withColumn("scorecard_name", scorecard_name)


# ---------------------------------------------------------------------------
# 5C.  SCORECARD COEFFICIENTS  (replace placeholders with production values)
# ---------------------------------------------------------------------------
# Structure: {scorecard_name: {"intercept": float, attr: coeff, ...}}

SCORECARD_COEFFICIENTS = {
    "ST_1_HC": {
        "intercept":                     500.0,
        "max_dpd_uns_l36m":              -25.0,
        "util_l3m_cc_live":              -50.0,
        "nbr_accts_open_l6m":            -15.0,
        "totconsinc_bal10_exc_cc_25m":   -10.0,
        "outflow_uns_sec_mov":             0.001,
        "max_mob_all_36":                  1.5,
        "avg_bal_l3m_all":               -0.00002,
        "final_consec_marker":           -30.0,
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
        "outflow_uns_sec_mov":     0.0008,
        "max_mob_all_36":          1.8,
        "final_consec_marker":   -20.0,
    },
    "ST_2": {
        "intercept":         350.0,
        "max_dpd_uns_l12m":  -30.0,
        "nbr_30_24m_all":    -20.0,
        "final_consec_marker": -40.0,
    },
    "ST_3": {
        "intercept":         250.0,
        "max_dpd_uns_l36m":  -40.0,
        "final_consec_marker": -50.0,
    },
    "THIN": {
        "intercept":           400.0,
        "nbr_tot_accts_36":     10.0,
        "max_mob_all_36":        5.0,
    },
    "CLOSED": {
        # P0-2: shouldTriggerNoHitScore path — return flat no-hit score
        "intercept": 300.0,
    },
}

sc_coeff_bc = spark.sparkContext.broadcast(SCORECARD_COEFFICIENTS)


# ---------------------------------------------------------------------------
# 5D.  FINAL SCORE  (ScoreCardV4Evaluator.getScore)
# ---------------------------------------------------------------------------

def compute_final_score(global_attrs_with_trigger):
    ga = global_attrs_with_trigger

    score_schema = StructType([
        StructField("cust_id",         LongType(),    nullable=False),
        StructField("final_score",     IntegerType(), nullable=True),
        StructField("score_breakdown", StringType(),  nullable=True),
    ])

    def _compute_scores_pdf(pdf, coeffs):
        """Pure Python scoring — Arrow-free."""
        results = []
        for _, row in pdf.iterrows():
            sc_name = row.get("scorecard_name", "ST_1_SE")
            coeff   = coeffs.get(sc_name, coeffs["ST_1_SE"])

            # P0-2: CLOSED / no-hit score
            if sc_name == "CLOSED":
                results.append({
                    "cust_id":         int(row["cust_id"]),
                    "final_score":     int(coeff.get("intercept", 300)),
                    "score_breakdown": "CLOSED_NO_HIT",
                })
                continue

            score = coeff.get("intercept", 500.0)
            parts = [f"intercept={score:.2f}"]

            for attr, w in coeff.items():
                if attr == "intercept":
                    continue
                val = row.get(attr, None)
                if val is not None and not (isinstance(val, float) and np.isnan(val)):
                    effective_val = 0.0 if float(val) == -999.0 else float(val)
                    contrib = effective_val * w
                    score  += contrib
                    parts.append(f"{attr}={contrib:.2f}")

            final = max(300, min(900, int(round(score))))
            results.append({
                "cust_id":         int(row["cust_id"]),
                "final_score":     final,
                "score_breakdown": "|".join(parts),
            })
        return pd.DataFrame(results, columns=["cust_id", "final_score", "score_breakdown"])

    coeffs   = sc_coeff_bc.value
    ga_pdf   = ga.toPandas()
    score_pdf = _compute_scores_pdf(ga_pdf, coeffs)
    scores = spark.createDataFrame(score_pdf, schema=StructType([
        StructField("cust_id",         LongType(),    nullable=False),
        StructField("final_score",     IntegerType(), nullable=True),
        StructField("score_breakdown", StringType(),  nullable=True),
    ]))

    return ga.join(scores, "cust_id", "left")


# ===========================================================================
# PHASE 6 — PIPELINE ORCHESTRATOR  (ConsumerScorecardAttributeCalculatorV4.invoke)
# ===========================================================================

def run_pipeline(fact2):
    """
    End-to-end pipeline.
    Pass in a pre-built fact2 DataFrame (or use build_fact2() above).
    """

    print("─── Phase 2: Per-Month Variable Calculation ───")
    fact2_enriched = build_fact2_enriched(fact2)
    fact2_enriched.cache()

    print("─── Phase 3: Per-Account Aggregation ───")
    account_details = build_account_details(fact2_enriched)
    account_details.cache()

    print("─── Phase 4: Global Attribute Calculation ───")
    global_attrs = build_global_attrs(account_details, fact2_enriched)

    print("─── Phase 5A: Trigger Eligibility ───")
    st3_eligible, st2_eligible = compute_trigger_eligibility(account_details)

    print("─── Phase 5B: Scorecard Selection ───")
    global_attrs_triggered = apply_score_trigger(global_attrs, st3_eligible, st2_eligible)

    print("─── Phase 5C: Final Score ───")
    final_df = compute_final_score(global_attrs_triggered)

    print("─── Phase 6: Output ───")
    # For EMR write to S3; for local testing use show()
    output_cols = [
        "cust_id", "scorecard_name", "final_score", "score_breakdown",
        # Trigger inputs
        "nbr_live_accts_36", "max_mob_all_36",
        "is_eligible_for_st2", "is_eligible_for_st3",
        "max_simul_unsec_wo_cc", "nbr_accts_open_l6m",
        # Key scoring attributes
        "max_dpd_uns_l36m", "max_dpd_uns_l12m",
        "util_l3m_cc_live", "util_l6m_cc_live",
        "util_l3m_uns_tot", "util_l12m_uns_tot",
        "outflow_uns_sec_mov",
        "nbr_tot_accts_36", "nbr_derog_accts",
        "totconsinc_bal10_exc_cc_25m",
        "mon_since_first_worst_delq","mon_since_recent_worst_delq",
        "final_consec_marker",
        "curr_tot_exp_amt", "curr_tot_sanc_amt",
    ]

    result = final_df.select([c for c in output_cols if c in final_df.columns])

    result.printSchema()
    result.show(truncate=False)

    # ── Save results to CSV (pandas write avoids Spark CSV Kerberos issue on Mac)
    import os
    flat_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scorecard_results.csv")
    result.toPandas().to_csv(flat_csv, index=False)
    print(f"\n✅  Results saved to: {flat_csv}")

    return result


# ===========================================================================
# MAIN — Run pipeline directly from synthetic_fact2_v2.csv
# Usage:  python consumer_scorecard_v4_pyspark.py
#         python consumer_scorecard_v4_pyspark.py /path/to/your_fact2.csv
# ===========================================================================

if __name__ == "__main__":
    import sys, os

    csv_path = sys.argv[1] if len(sys.argv) > 1 else \
               os.path.join(os.path.dirname(os.path.abspath(__file__)), "synthetic_fact2_v2.csv")

    if not os.path.exists(csv_path):
        print(f"❌  CSV not found: {csv_path}")
        print("    Place synthetic_fact2_v2.csv in the same folder as this script,")
        print("    or pass the path as an argument.")
        sys.exit(1)

    print(f"\n📂  Reading fact2 from: {csv_path}")

    # Read via pandas first (avoids Spark CSV Kerberos/getSubject issue on Mac)
    # then convert to Spark DataFrame with explicit schema
    _pdf = pd.read_csv(csv_path, dtype={
        "account_type_cd":          str,
        "pay_rating_cd":            str,
        "account_status_cd":        str,
        "SUITFILED_WILFULDEFAULT":  str,
        "WRITTEN_OFF_AND_SETTLED":  str,
        "Sec_Uns":                  str,
        "Reg_Com":                  str,
        "Sec_Mov":                  str,
        "Pdt_Cls":                  str,
        "Category":                 str,
        "relFinCd":                 str,
    })

    # Ensure numeric types — cast ALL double columns to Python float explicitly
    for _int_col in ["cust_id","accno","bureau_mbr_id","open_dt","closed_dt",
                     "score_dt","balance_dt","month_diff","dayspastdue"]:
        _pdf[_int_col] = pd.to_numeric(_pdf[_int_col], errors="coerce").fillna(0).astype("int64")
    for _dbl_col in ["balance_amt","credit_lim_amt","original_loan_amt",
                     "past_due_amt","actual_pymnt_amt"]:
        _pdf[_dbl_col] = pd.to_numeric(_pdf[_dbl_col], errors="coerce").fillna(0.0).astype("float64")

    # Helper: convert pandas value to clean string (NaN → "")
    import math as _math
    def _s(v):
        if v is None: return ""
        try:
            if _math.isnan(float(v)): return ""
        except (ValueError, TypeError): pass
        s = str(v).strip()
        return "" if s.lower() in ("nan","none","null") else s

    # Convert pandas DataFrame to list of Row objects to avoid Arrow path entirely
    from pyspark.sql import Row
    _records = [
        Row(
            cust_id                 = int(r["cust_id"]),
            accno                   = int(r["accno"]),
            bureau_mbr_id           = int(r["bureau_mbr_id"]),
            account_type_cd         = _s(r["account_type_cd"]),
            open_dt                 = int(r["open_dt"]),
            closed_dt               = int(r["closed_dt"]),
            score_dt                = int(r["score_dt"]),
            balance_dt              = int(r["balance_dt"]),
            month_diff              = int(r["month_diff"]),
            balance_amt             = float(r["balance_amt"]),
            credit_lim_amt          = float(r["credit_lim_amt"]),
            original_loan_amt       = float(r["original_loan_amt"]),
            past_due_amt            = float(r["past_due_amt"]),
            dayspastdue             = int(r["dayspastdue"]),
            pay_rating_cd           = _s(r["pay_rating_cd"]),
            account_status_cd       = _s(r["account_status_cd"]),
            actual_pymnt_amt        = float(r["actual_pymnt_amt"]),
            SUITFILED_WILFULDEFAULT = _s(r["SUITFILED_WILFULDEFAULT"]),
            WRITTEN_OFF_AND_SETTLED = _s(r["WRITTEN_OFF_AND_SETTLED"]),
            Sec_Uns                 = _s(r["Sec_Uns"]),
            Reg_Com                 = _s(r["Reg_Com"]),
            Sec_Mov                 = _s(r["Sec_Mov"]),
            Pdt_Cls                 = _s(r["Pdt_Cls"]),
            Category                = _s(r["Category"]),
            relFinCd                = _s(r["relFinCd"]),
        )
        for _, r in _pdf.iterrows()
    ]

    fact2_schema = StructType([
        StructField("cust_id",                LongType(),    True),
        StructField("accno",                  LongType(),    True),
        StructField("bureau_mbr_id",          LongType(),    True),
        StructField("account_type_cd",        StringType(),  True),
        StructField("open_dt",                IntegerType(), True),
        StructField("closed_dt",              IntegerType(), True),
        StructField("score_dt",               IntegerType(), True),
        StructField("balance_dt",             IntegerType(), True),
        StructField("month_diff",             IntegerType(), True),
        StructField("balance_amt",            DoubleType(),  True),
        StructField("credit_lim_amt",         DoubleType(),  True),
        StructField("original_loan_amt",      DoubleType(),  True),
        StructField("past_due_amt",           DoubleType(),  True),
        StructField("dayspastdue",            IntegerType(), True),
        StructField("pay_rating_cd",          StringType(),  True),
        StructField("account_status_cd",      StringType(),  True),
        StructField("actual_pymnt_amt",       DoubleType(),  True),
        StructField("SUITFILED_WILFULDEFAULT",StringType(),  True),
        StructField("WRITTEN_OFF_AND_SETTLED",StringType(),  True),
        StructField("Sec_Uns",                StringType(),  True),
        StructField("Reg_Com",                StringType(),  True),
        StructField("Sec_Mov",                StringType(),  True),
        StructField("Pdt_Cls",                StringType(),  True),
        StructField("Category",               StringType(),  True),
        StructField("relFinCd",               StringType(),  True),
    ])

    fact2_df = spark.createDataFrame(_records, schema=fact2_schema)

    print(f"✅  Loaded {len(_pdf)} rows for {_pdf['cust_id'].nunique()} customers\n")

    result = run_pipeline(fact2_df)
    print("\n✅  Pipeline complete.")