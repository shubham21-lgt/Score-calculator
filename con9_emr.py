
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


TRADE_CSV = "s3://aws-emr-studio-934472299739-ap-south-1/Anmol_Chargen/Sample_Trade/TRADE_CSV.csv"


# ACCT_MAP_CSV
ACCT_MAP_CSV = "s3://aws-emr-studio-934472299739-ap-south-1/Anmol_Chargen/Account_mapping/account_df.csv"

# PROD_MAP_CSV
# PROD_MAP_CSV = "s3://aws-emr-studio-934472299739-ap-south-1/Anmol_Chargen/Input/chargen_product_mapping_12feb.csv"
PROD_MAP_CSV = "s3://aws-emr-studio-934472299739-ap-south-1/Anmol_Chargen/Product_Mapping/product_mapping_df.csv"
# PROD_MAP_CSV = "s3://aws-emr-studio-934472299739-ap-south-1/Anmol_Chargen/Input/chargen_product_mapping_9feb.xlsx"

# BANK_MAP_CSV
BANK_MAP_CSV = "s3://aws-emr-studio-934472299739-ap-south-1/Anmol_Chargen/Bank_Mapping/bank_mapping_df.csv"


trade_df = (
    spark.read
         .format("csv")
         .option("header", "true")        # set to "false" if no header
         .option("inferSchema", "true")   # or define a schema for speed & correctness
         .option("compression", "gzip")   # important due to .gzi
         .load(TRADE_CSV)
)

print("***TRADE_CSV****")
trade_df.printSchema()


account_df = (
    spark.read
         .format("csv")
         .option("header", "true")        # set to "false" if no header
         .option("inferSchema", "true")   # or define a schema for speed & correctness
         .option("compression", "gzip")   # important due to .gzi
         .load(ACCT_MAP_CSV)
)

print("***account_df****")
account_df.printSchema()

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


product_mapping_df = (
    spark.read
         .format("csv")
         .option("header", "true")        # set to "false" if no header
         .option("inferSchema", "true")   # or define a schema for speed & correctness
         .option("compression", "gzip")   # important due to .gzi
         .load(PROD_MAP_CSV)
)
print("****product_mapping_df****")
product_mapping_df.printSchema()

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

bank_mapping_df = (
    spark.read
         .format("csv")
         .option("header", "true")        # set to "false" if no header
         .option("inferSchema", "true")   # or define a schema for speed & correctness
         .option("compression", "gzip")   # important due to .gzi
         .load(BANK_MAP_CSV)
)
# print("*****bank_mapping_df*****")
# bank_mapping_df.printSchema()

# ***TRADE_CSV****
# root
#  |-- accno: integer (nullable = true)
#  |-- cons_acct_key: long (nullable = true)
#  |-- open_dt: integer (nullable = true)
#  |-- acct_nb: string (nullable = true)
#  |-- closed_dt: integer (nullable = true)
#  |-- bureau_mbr_id: integer (nullable = true)
#  |-- account_type_cd: integer (nullable = true)
#  |-- term_freq: integer (nullable = true)
#  |-- charge_off_amt: integer (nullable = true)
#  |-- resp_code: integer (nullable = true)
#  |-- balance_dt: integer (nullable = true)
#  |-- account_status_cd: string (nullable = true)
#  |-- actual_pymnt_amt: integer (nullable = true)
#  |-- balance_amt: integer (nullable = true)
#  |-- credit_lim_amt: integer (nullable = true)
#  |-- original_loan_amt: integer (nullable = true)
#  |-- past_due_amt: integer (nullable = true)
#  |-- pay_rating_cd: string (nullable = true)
#  |-- dayspastdue: integer (nullable = true)
#  |-- written_off_amt: integer (nullable = true)
#  |-- principal_written_off: integer (nullable = true)
#  |-- SUITFILED_WILFULDEFAULT: integer (nullable = true)
#  |-- SUITFILEDWILLFULDEFAULT: integer (nullable = true)
#  |-- WRITTEN_OFF_AND_SETTLED: integer (nullable = true)

# ***account_df****
# root
#  |-- cust_id: long (nullable = true)
#  |-- app_date: integer (nullable = true)

# ****product_mapping_df****
# root
#  |-- account_type_cd: string (nullable = true)
#  |-- Sec_Uns: string (nullable = true)
#  |-- Reg_Com: string (nullable = true)
#  |-- Sec_Mov: string (nullable = true)
#  |-- Pdt_Cls: string (nullable = true)

# *****bank_mapping_df*****
# root
#  |-- BUREAU_MBR_ID: integer (nullable = true)
#  |-- Bank_Name: string (nullable = true)
#  |-- LEGACY_CUST_NB: string (nullable = true)
#  |-- Category: string (nullable = true)

import os
# JAVA_TOOL_OPTIONS not needed on EMR — the JVM is managed by AWS.
# Kept as no-op for local compatibility.
if not os.environ.get("EMR_MODE"):
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

# ── EMR-aware SparkSession ────────────────────────────────────────────────
# On EMR: SparkContext is already initialised by YARN — getOrCreate() reuses it.
# .master() must NOT be set on EMR (YARN controls the master).
# On local: .master("local[*]") is applied for developer testing.
# shuffle.partitions: read from env var — 8 for local, 200+ recommended for EMR.
_IS_EMR        = os.environ.get("EMR_MODE", "").lower() in ("1", "true", "yes")
_SHUFFLE_PARTS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "200" if _IS_EMR else "8")

_builder = (
    SparkSession.builder
    .appName("ConsumerScorecardV4_Final")
    .config("spark.sql.shuffle.partitions",     _SHUFFLE_PARTS)
    .config("spark.sql.ansi.enabled",           "false")
    .config("spark.sql.execution.arrow.pyspark.enabled",          "true")
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
)

if not _IS_EMR:
    # Local-only settings — Java 25 workarounds not needed on EMR
    _builder = (
        _builder
        .master("local[*]")
        .config("spark.hadoop.security.authentication", "simple")
        .config("spark.driver.extraJavaOptions",
                "-Darrow.memory.manager=unsafe "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions",
                "-Darrow.memory.manager=unsafe "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
    )

spark = _builder.getOrCreate()

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


from pyspark.sql.functions import col, lit, broadcast

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
                   "221", "222", "240", "242", "243", "244", "245", "246", "247", "248", "249"}
AGRI_CODES      = {"167", "177", "178", "179", "198", "199", "200", "223", "224", "226", "227"}
COM_SEC_CODES   = {"175", "176", "228", "241", "191", "243", "007"}  # B3 FIX: GL codes treated as commercial secured
COM_UNSEC_CODES = {"176", "177", "178", "179", "228"}
REGULAR_CODES   = {"047", "058", "121", "123", "130", "172", "173", "191", "195", "196",
                   "215", "216", "217", "219", "220", "221", "222", "225", "240", "241",
                   "242", "243", "244", "245", "246", "247", "248", "249"}
PL_CD_TW_CODES  = {"173", "013", "123", "242", "189"}   # PL + CD + TW as per spec
LAP_HL_CODES    = {"058", "195", "58"}                        # HL + LAP (excluded from freq_installment)
GL_CODES_EXT    = {"191", "243"}                        # GL codes for max_simul_inc_gl

DEROG_SFW_LIST  = {"05","06","07","08","09","10","11","12","13","14","15","16","17"}
DEROG_WOST_LIST = {"02","03","04","05","06","07","08","09","10","11","12","13","14","15"}
OWNERSHIP_CD    = {"1", "2"}


@udf(DoubleType())
def modified_limit_udf(balance_amt, credit_lim_amt, original_loan_amt):
    vals = [
        float(balance_amt)       if balance_amt       not in (None, -1) else 0.0,
        float(credit_lim_amt)    if credit_lim_amt    not in (None, -1) else 0.0,
        float(original_loan_amt) if original_loan_amt not in (None, -1) else 0.0,
    ]
    return max(vals)

@udf(BooleanType())
def derog_flag_udf(suit_filed, wo_settled, pay_rating_cd):
    def _clean(val):
        if val is None: return ""
        try:
            if _math.isnan(float(val)): return ""
        except (ValueError, TypeError): pass
        s = str(val).strip()
        return "" if s.lower() in ("", "nan", "none", "null") else s

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
    if wo and wo[:2] in {"00","01"}:
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
        .withColumn("isUns",      col("Sec_Uns") == "U")
        .withColumn("isSec",      col("Sec_Uns") == "S")
        .withColumn("isSecMov",   col("Sec_Mov") == "M")
        .withColumn("isRegSec",   col("Pdt_Cls") == "CS")
        .withColumn("isRegUns",   col("Pdt_Cls") == "CU")
        .withColumn("isComCls",   col("Reg_Com") == "C")
        .withColumn("isPSB",      col("Category") == "PSB")
        .withColumn("isPVT",      col("Category") == "PVT")
        .withColumn("isNBFC",     col("Category").isin("NBF","NBFC"))
        .withColumn("isSFB",      col("Category") == "SFB")
        # SecMov + RegSec = secured movable (for outflow_uns_secmov)
        .withColumn("isSecMovRegSec",
                    (col("Sec_Mov") == "M") & (col("Pdt_Cls") == "CS"))
    )


def load_inputs(trade_csv, account_mapping_csv, product_mapping_csv, bank_mapping_csv, cust_ids=None):
    """
    Load all inputs from S3 using Spark (distributed).
    If cust_ids is provided (list/tuple of ints/strings), we pre-filter account mapping
    and then reduce the trade dataset to only accnos for those customers.
    """
    # Read as strings first; your pipeline casts types later
    def read_csv(path):
        return (
            spark.read
            .option("header", True)
            .option("multiLine", False)
            .option("escape", '"')
            .csv(path)
        )

    trade_df = read_csv(trade_csv)
    acct_map = read_csv(account_mapping_csv)
    prod_map = read_csv(product_mapping_csv)
    bank_map = read_csv(bank_mapping_csv)

    # Normalize key columns used later
    # (keep as strings; downstream casts in build_fact2_enriched)
    for df, cols in [
        (trade_df, ["accno", "cons_acct_key", "account_type_cd", "bureau_mbr_id", "balance_dt", "open_dt", "closed_dt"]),
        (acct_map, ["accno", "cust_id", "score_dt", "relFinCd"]),
        (prod_map, ["account_type_cd", "Sec_Uns", "Reg_Com", "Sec_Mov", "Pdt_Cls"]),
        (bank_map, ["BUREAU_MBR_ID", "Category"]),
    ]:
        for c in cols:
            if c in df.columns:
                df = df.withColumn(c, col(c).cast("string"))
        if df is trade_df: trade_df = df
        elif df is acct_map: acct_map = df
        elif df is prod_map: prod_map = df
        else: bank_map = df

    # Optional trial filter: only process these customers
    if cust_ids:
        cust_ids_lit = [str(x).strip() for x in cust_ids if str(x).strip()]
        acct_map = acct_map.filter(col("cust_id").isin(*cust_ids_lit))
        # Reduce trades to only accnos we need (semi-join)
        accnos_needed = acct_map.select("accno").distinct()
        trade_df = trade_df.join(broadcast(accnos_needed), on="accno", how="inner")

    # Log sizes (distributed counts)
    print(f" trade_data : {trade_df.count():,} rows")
    print(f" account_mapping: {acct_map.count():,} rows")
    print(f" product_mapping: {prod_map.count():,} rows")
    print(f" bank_mapping : {bank_map.count():,} rows")

    return trade_df, acct_map, prod_map, bank_map


from pyspark.sql.functions import col, lit

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
        if val is None: return ""
        try:
            if _math.isnan(float(val)): return ""
        except (ValueError, TypeError): pass
        s = str(val).strip()
        return "" if s.lower() in ("", "nan", "none", "null") else s

    wo_raw = _clean(wo_settled)
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
    if 61 <= dpd <= 90:
        return 3
    if pr == "M" and dpd == -1:
        return 3
    if 31 <= dpd <= 60:
        return 2
    if 1 <= dpd <= 30:
        return 1
    return 0




# ===========================================================================
# PHASE 3 — PER-ACCOUNT AGGREGATION
# ===========================================================================

def build_account_details(fact2_enriched):
    df = fact2_enriched

    acct_agg = (
        df.groupBy("cust_id", "accno").agg(
            first("account_type_cd").alias("account_type_cd"),
            first("open_dt").alias("open_dt"),
            first("closed_dt").alias("closed_dt"),
            first("score_dt").alias("score_dt"),
            first("bureau_mbr_id").alias("bureau_mbr_id"),
            first("cons_acct_key").alias("cons_acct_key"),
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
            fmax(
                when(col("closed_dt").isNull() | (col("closed_dt") == "0") |
                     (col("closed_dt") == "") | (col("closed_dt") == "."),
                     lit(True))
                .when(col("month_diff") <= 5,
                      (col("account_status_cd").cast("string").substr(1,1) == "O"))
                .otherwise(lit(False))
            ).alias("isLiveAccount"),
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

    # Months-on-Book
    def yyyymmdd_to_abs_months(dt_col):
        d = to_date(dt_col.cast("string"), "yyyyMMdd")
        return year(d) * 12 + month(d)

    acct_agg = (
        acct_agg
        .withColumn("_score_abs", yyyymmdd_to_abs_months(col("score_dt")))
        .withColumn("_open_abs",  yyyymmdd_to_abs_months(col("open_dt")))
        .withColumn("mob", col("_score_abs") - col("_open_abs") + 1)  # B7/B8 FIX: +1 to include open month
        .drop("_score_abs", "_open_abs")
    )

    # firstReportedOnlyIn36M (reported in 36m but NOT in 12m)
    in_12m = (
        fact2_enriched.filter(col("month_diff") <= 12)
        .groupBy("cust_id", "accno")
        .agg(lit(True).alias("_in_12m"))
    )
    acct_agg = (
        acct_agg.join(in_12m, ["cust_id", "accno"], "left")
        .withColumn("firstReportedOnlyIn36M",
                    col("_in_12m").isNull() & col("reportedIn36M"))
        .drop("_in_12m")
    )

    # m_since_max_bal_l24m
    max_bal_idx = (
        fact2_enriched.filter(col("month_diff") <= 23)
        .withColumn("_rn", row_number().over(
            Window.partitionBy("cust_id", "accno")
                  .orderBy(col("balance_amt").desc(), col("month_diff").asc())))
        .filter(col("_rn") == 1)
        .select("cust_id", "accno", col("month_diff").alias("m_since_max_bal_l24m"))
    )
    acct_agg = acct_agg.join(max_bal_idx, ["cust_id", "accno"], "left")

    # Worst delinquency history
    worst_delq_df = (
        fact2_enriched
        .withColumn("_max_dpd_acct",
                    fmax("dpd_new").over(Window.partitionBy("cust_id", "accno")))
        .filter(col("dpd_new") == col("_max_dpd_acct"))
        .filter(col("dpd_new") > 0)
        .groupBy("cust_id", "accno").agg(
            fmax("month_diff").alias("mon_since_first_worst_delq"),
            fmin("month_diff").alias("mon_since_recent_worst_delq"),
        )
    )
    acct_agg = acct_agg.join(worst_delq_df, ["cust_id", "accno"], "left")

    return acct_agg

def build_fact2_enriched_2(fact2):
    df = fact2

    # Cast numeric measures you use downstream
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

    # Derivations that rely on the casts above
    df = (
        df
        .withColumn(
            "dpd_new",
            dpd_new_udf(
                col("dayspastdue"), col("pay_rating_cd"),
                col("SUITFILED_WILFULDEFAULT").cast("string"),
                col("WRITTEN_OFF_AND_SETTLED").cast("string"),
                col("balance_amt")
            )
        )
        .withColumn(
            "modified_limit",
            modified_limit_udf(
                col("balance_amt"), col("credit_lim_amt"), col("original_loan_amt")
            )
        )
        .withColumn(
            "derog_flag",
            derog_flag_udf(
                col("SUITFILED_WILFULDEFAULT").cast("string"),
                col("WRITTEN_OFF_AND_SETTLED").cast("string"),
                col("pay_rating_cd")
            )
        )
        # Safer "is missing/invalid close date" for string inputs
        .withColumn(
            "is_live_month0",
            (col("month_diff") == 0) & (
                col("closed_dt").isNull()
                | col("closed_dt").isin("", "0", ".")
                | (col("closed_dt") >= col("score_dt"))  # both yyyyMMdd strings
            )
        )
        .withColumn(
            "is_not_closed",
            col("closed_dt").isNull()
            | col("closed_dt").isin("", "0", ".")
            | (col("closed_dt") >= col("score_dt"))
        )
    )

    df = add_product_flags(df)
    return df
# *************************************************************************************************************
# Phase one
from pyspark.sql.functions import col, lit, to_date, year, month, broadcast, regexp_replace


from pyspark.sql.functions import (
    col, lit, to_date, year, month, broadcast, regexp_replace, trim
)
from pyspark.sql import functions as F




def build_fact22(trade_df, acct_map, prod_map, bank_map):
    # Normalize mapping to ensure required cols exist
    if "score_dt" not in acct_map.columns and "app_date" in acct_map.columns:
        acct_map = acct_map.withColumnRenamed("app_date", "score_dt")
    if "relFinCd" not in acct_map.columns:
        acct_map = acct_map.withColumn("relFinCd", lit("1"))
    if "accno" not in acct_map.columns and "cust_id" in acct_map.columns:
        acct_map = acct_map.withColumn("accno", col("cust_id"))

    # Ownership filter (expects strings like "1","2")
    acct_filtered = acct_map.filter(col("relFinCd").cast("string").isin(*OWNERSHIP_CD))  # OWNERSHIP_CD = {"1","2"}

    # Join trade ↔ mapping on accno (cast both to string)
    df = (
        trade_df.join(
            acct_filtered.select(
                col("accno").cast("string").alias("_accno_join"),
                "cust_id", "score_dt", "relFinCd"
            ),
            on=trade_df["accno"].cast("string") == col("_accno_join"),
            how="inner"
        )
        .drop("_accno_join")
    )

    # (optional) date clean so "20250930.0" parses
    df = (
        df
        .withColumn("score_dt_clean",   regexp_replace(col("score_dt").cast("string"),   r"\.0$", ""))
        .withColumn("balance_dt_clean", regexp_replace(col("balance_dt").cast("string"), r"\.0$", ""))
    )

    def to_abs_months(date_col):
        d = to_date(date_col, "yyyyMMdd")
        return year(d) * 12 + month(d)

    df = (
        df
        .withColumn("_score_abs",   to_abs_months(col("score_dt_clean")))
        .withColumn("_balance_abs", to_abs_months(col("balance_dt_clean")))
        .withColumn("month_diff", col("_score_abs") - col("_balance_abs"))
        .drop("_score_abs", "_balance_abs", "score_dt_clean", "balance_dt_clean")
        .filter((col("month_diff") >= 0) & (col("month_diff") <= 35))
    )

    # product & bank joins (unchanged)
    prod_sel = broadcast(
        prod_map.select(
            col("account_type_cd").cast("string").alias("_prod_acct_cd"),
            "Sec_Uns", "Reg_Com", "Sec_Mov", "Pdt_Cls"
        )
    )
    df = df.join(
        prod_sel,
        on=df["account_type_cd"].cast("string") == col("_prod_acct_cd"),
        how="left"
    ).drop("_prod_acct_cd")

    df = df.join(
        broadcast(
            bank_map.select(
                col("BUREAU_MBR_ID").cast("long").alias("_bk_id"),
                col("Category")
            )
        ),
        on=df["bureau_mbr_id"].cast("long") == col("_bk_id"),
        how="left"
    ).drop("_bk_id")

    return df

import sys, os

print("\n=== Consumer Scorecard V4 — Final Pipeline (EMR) ===")
OUTPUT_DIR = None

# CUST_ID = [196537, 621582]  # two customers for your trial
CUST_ID = None

trade_df, acct_map, prod_map, bank_map = load_inputs(
    TRADE_CSV, ACCT_MAP_CSV, PROD_MAP_CSV, BANK_MAP_CSV
)
print(f">>>>>>>>> Step One Done <<<<< {acct_map}")


# Run the pipeline and write to S3 (your run_pipeline already handles this if you replaced the write step)
# result = run_pipeline(trade_df, acct_map, prod_map, bank_map, OUTPUT_DIR)

# === Consumer Scorecard V4 ? Final Pipeline (EMR) ===
#  trade_data : 473 rows
#  account_mapping: 16,251 rows
#  product_mapping: 59 rows
#  bank_mapping : 13,161 rows
# >>>>>>>>> Step One Done <<<<< DataFrame[cust_id: string, app_date: string]

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


# working 
def build_fact22(trade_df, acct_map, prod_map, bank_map):
    # Normalize mapping to ensure required cols exist
    if "score_dt" not in acct_map.columns and "app_date" in acct_map.columns:
        acct_map = acct_map.withColumnRenamed("app_date", "score_dt")
    if "relFinCd" not in acct_map.columns:
        acct_map = acct_map.withColumn("relFinCd", lit("1"))
    if "accno" not in acct_map.columns and "cust_id" in acct_map.columns:
        acct_map = acct_map.withColumn("accno", col("cust_id"))

    # Ownership filter (expects strings like "1","2")
    acct_filtered = acct_map.filter(col("relFinCd").cast("string").isin(*OWNERSHIP_CD))  # OWNERSHIP_CD = {"1","2"}

    # Join trade ↔ mapping on accno (cast both to string)
    df = (
        trade_df.join(
            acct_filtered.select(
                col("accno").cast("string").alias("_accno_join"),
                "cust_id", "score_dt", "relFinCd"
            ),
            on=trade_df["accno"].cast("string") == col("_accno_join"),
            how="inner"
        )
        .drop("_accno_join")
    )

    # (optional) date clean so "20250930.0" parses
    df = (
        df
        .withColumn("score_dt_clean",   regexp_replace(col("score_dt").cast("string"),   r"\.0$", ""))
        .withColumn("balance_dt_clean", regexp_replace(col("balance_dt").cast("string"), r"\.0$", ""))
    )

    def to_abs_months(date_col):
        d = to_date(date_col, "yyyyMMdd")
        return year(d) * 12 + month(d)

    df = (
        df
        .withColumn("_score_abs",   to_abs_months(col("score_dt_clean")))
        .withColumn("_balance_abs", to_abs_months(col("balance_dt_clean")))
        .withColumn("month_diff", col("_score_abs") - col("_balance_abs"))
        .drop("_score_abs", "_balance_abs", "score_dt_clean", "balance_dt_clean")
        .filter((col("month_diff") >= 0) & (col("month_diff") <= 35))
    )

    # product & bank joins (unchanged)
    prod_sel = broadcast(
        prod_map.select(
            col("account_type_cd").cast("string").alias("_prod_acct_cd"),
            "Sec_Uns", "Reg_Com", "Sec_Mov", "Pdt_Cls"
        )
    )
    df = df.join(
        prod_sel,
        on=df["account_type_cd"].cast("string") == col("_prod_acct_cd"),
        how="left"
    ).drop("_prod_acct_cd")

    df = df.join(
        broadcast(
            bank_map.select(
                col("BUREAU_MBR_ID").cast("long").alias("_bk_id"),
                col("Category")
            )
        ),
        on=df["bureau_mbr_id"].cast("long") == col("_bk_id"),
        how="left"
    ).drop("_bk_id")

    return df

from pyspark.sql.functions import col, to_date, year, month, regexp_replace, broadcast, lit

OWNERSHIP_CD = {"1","2"}

def build_fact2_plus(trade_df, acct_map, prod_map, bank_map):
    # ---- Normalize acct_map safely ----
    am = acct_map
    if "score_dt" not in am.columns and "app_date" in am.columns:
        am = am.withColumnRenamed("app_date", "score_dt")

    # Hard requirement: accno and relFinCd must be present; fail fast if not
    required = {"accno", "relFinCd", "score_dt"}
    missing = [c for c in required if c not in am.columns]
    if missing:
        raise ValueError(f"acct_map missing required columns: {missing}")

    # Ownership filter (strings)
    acct_filtered = am.filter(col("relFinCd").cast("string").isin(*OWNERSHIP_CD))

    # ---- Join trade ↔ acct on accno with type tolerance ----
    df = (
        trade_df.join(
            acct_filtered.select(
                col("accno").cast("string").alias("_accno_join"),
                "cust_id", "score_dt", "relFinCd"
            ),
            on=trade_df["accno"].cast("string") == col("_accno_join"),
            how="inner"
        ).drop("_accno_join")
    )

    # ---- Date cleaning then month_diff ----
    df = (
        df
        .withColumn("score_dt_clean",   regexp_replace(col("score_dt").cast("string"),   r"\.0$", ""))
        .withColumn("balance_dt_clean", regexp_replace(col("balance_dt").cast("string"), r"\.0$", ""))
    )

    def to_abs_months(date_col):
        d = to_date(date_col, "yyyyMMdd")
        return year(d)*12 + month(d)

    df = (
        df
        .withColumn("_score_abs",   to_abs_months(col("score_dt_clean")))
        .withColumn("_balance_abs", to_abs_months(col("balance_dt_clean")))
        .withColumn("month_diff", col("_score_abs") - col("_balance_abs"))
        .drop("_score_abs", "_balance_abs", "score_dt_clean", "balance_dt_clean")
        .filter((col("month_diff") >= 0) & (col("month_diff") <= 35))
    )

    # ---- Product join WITH leading-zero normalization (keep from build_fact2) ----
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

    # ---- Bank join (unchanged) ----
    df = df.join(
        broadcast(bank_map.select(
            col("BUREAU_MBR_ID").cast("long").alias("_bk_id"),
            col("Category")
        )),
        on=df["bureau_mbr_id"].cast("long") == col("_bk_id"),
        how="left"
    ).drop("_bk_id")

    return df


def build_facts2(trade_df, acct_map, prod_map, bank_map):
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
fact2 = build_fact22(trade_df, acct_map, prod_map, bank_map)
print(f"    fact2 rows: {fact2.count():,}")



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

print("--- Phase 2: Per-Month Variables ---")
fact2_enriched = build_fact2_enriched(fact2)
fact2_enriched.cache()

# --- Phase 2: Per-Month Variables ---
# DataFrame[accno: string, cons_acct_key: string, open_dt: string, acct_nb: string, closed_dt: string, bureau_mbr_id: string, account_type_cd: string, term_freq: string, charge_off_amt: string, resp_code: string, balance_dt: string, account_status_cd: string, actual_pymnt_amt: double, balance_amt: double, credit_lim_amt: double, original_loan_amt: double, past_due_amt: double, pay_rating_cd: string, dayspastdue: int, written_off_amt: string, principal_written_off: string, SUITFILED_WILFULDEFAULT: string, SUITFILEDWILLFULDEFAULT: string, WRITTEN_OFF_AND_SETTLED: string, cust_id: string, score_dt: string, relFinCd: string, month_diff: int, Sec_Uns: string, Reg_Com: string, Sec_Mov: string, Pdt_Cls: string, Category: string, dpd_new: int, modified_limit: double, derog_flag: boolean, is_live_month0: boolean, is_not_closed: boolean, isCC: boolean, isCL: boolean, isHL: boolean, isAL: boolean, isTW: boolean, isGL: boolean, isGLExt: boolean, isPL: boolean, isAgri: boolean, isComSec: boolean, isComUnSec: boolean, isRegular: boolean, isPlCdTw: boolean, isLapHl: boolean, isUns: boolean, isSec: boolean, isSecMov: boolean, isRegSec: boolean, isRegUns: boolean, isComCls: boolean, isPSB: boolean, isPVT: boolean, isNBFC: boolean, isSFB: boolean, isSecMovRegSec: boolean]

# root
#  |-- accno: string (nullable = true)
#  |-- cons_acct_key: string (nullable = true)
#  |-- open_dt: string (nullable = true)
#  |-- acct_nb: string (nullable = true)
#  |-- closed_dt: string (nullable = true)
#  |-- bureau_mbr_id: string (nullable = true)
#  |-- account_type_cd: string (nullable = true)
#  |-- term_freq: string (nullable = true)
#  |-- charge_off_amt: string (nullable = true)
#  |-- resp_code: string (nullable = true)
#  |-- balance_dt: string (nullable = true)
#  |-- account_status_cd: string (nullable = true)
#  |-- actual_pymnt_amt: double (nullable = true)
#  |-- balance_amt: double (nullable = true)
#  |-- credit_lim_amt: double (nullable = true)
#  |-- original_loan_amt: double (nullable = true)
#  |-- past_due_amt: double (nullable = true)
#  |-- pay_rating_cd: string (nullable = true)
#  |-- dayspastdue: integer (nullable = true)
#  |-- written_off_amt: string (nullable = true)
#  |-- principal_written_off: string (nullable = true)
#  |-- SUITFILED_WILFULDEFAULT: string (nullable = true)
#  |-- SUITFILEDWILLFULDEFAULT: string (nullable = true)
#  |-- WRITTEN_OFF_AND_SETTLED: string (nullable = true)
#  |-- cust_id: string (nullable = true)
#  |-- score_dt: string (nullable = true)
#  |-- relFinCd: string (nullable = false)
#  |-- month_diff: integer (nullable = true)
#  |-- Sec_Uns: string (nullable = true)
#  |-- Reg_Com: string (nullable = true)
#  |-- Sec_Mov: string (nullable = true)
#  |-- Pdt_Cls: string (nullable = true)
#  |-- Category: string (nullable = true)
#  |-- dpd_new: integer (nullable = true)
#  |-- modified_limit: double (nullable = true)
#  |-- derog_flag: boolean (nullable = true)
#  |-- is_live_month0: boolean (nullable = true)
#  |-- is_not_closed: boolean (nullable = true)
#  |-- isCC: boolean (nullable = true)
#  |-- isCL: boolean (nullable = true)
#  |-- isHL: boolean (nullable = true)
#  |-- isAL: boolean (nullable = true)
#  |-- isTW: boolean (nullable = true)
#  |-- isGL: boolean (nullable = true)
#  |-- isGLExt: boolean (nullable = true)
#  |-- isPL: boolean (nullable = true)
#  |-- isAgri: boolean (nullable = true)
#  |-- isComSec: boolean (nullable = true)
#  |-- isComUnSec: boolean (nullable = true)
#  |-- isRegular: boolean (nullable = true)
#  |-- isPlCdTw: boolean (nullable = true)
#  |-- isLapHl: boolean (nullable = true)
#  |-- isUns: boolean (nullable = true)
#  |-- isSec: boolean (nullable = true)
#  |-- isSecMov: boolean (nullable = true)
#  |-- isRegSec: boolean (nullable = true)
#  |-- isRegUns: boolean (nullable = true)
#  |-- isComCls: boolean (nullable = true)
#  |-- isPSB: boolean (nullable = true)
#  |-- isPVT: boolean (nullable = true)
#  |-- isNBFC: boolean (nullable = true)
#  |-- isSFB: boolean (nullable = true)
#  |-- isSecMovRegSec: boolean (nullable = true)
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
            first("open_dt").alias("open_dt"),
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
            fmax(
                when(
                    # A real yyyyMMdd past closed_dt closes the account regardless of status
                    col("closed_dt").isNotNull() &
                    ~col("closed_dt").isin("", "0", "0.0", ".", "nan", "NaN", "None") &
                    (col("closed_dt") < col("score_dt")),
                    lit(False))
                .when(
                    F.trim(col("account_status_cd")).substr(1, 1) == "O",
                    lit(True))
                .otherwise(lit(False))
            ).alias("isLiveAccount"),
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
                  .orderBy(col("balance_amt").desc(), col("month_diff").asc())))
        .filter(col("_rn") == 1)
        .select("cust_id", "cons_acct_key", col("month_diff").alias("m_since_max_bal_l24m"))
    )
    acct_agg = acct_agg.join(max_bal_idx, ["cust_id", "cons_acct_key"], "left")​
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
# ​
# print("--- Phase 3: Per-Account Aggregation ---")
# account_details = build_account_details(fact2_enriched)
# account_details.cache()
# print(f"    account rows: {account_details.count():,}")
# ​
# ​
# Last executed at 2026-03-24 20:14:09 in 9.31s
# Spark Job Progress
# --- Phase 3: Per-Account Aggregation ---
#     account rows: 26
# .printSchema()
# account_details.printSchema()
# Last executed at 2026-03-24 20:15:12 in 41ms
# root
#  |-- cust_id: string (nullable = true)
#  |-- cons_acct_key: string (nullable = true)
#  |-- accno: string (nullable = true)
#  |-- account_type_cd: string (nullable = true)
#  |-- open_dt: string (nullable = true)
#  |-- closed_dt: string (nullable = true)
#  |-- score_dt: string (nullable = true)
#  |-- bureau_mbr_id: string (nullable = true)
#  |-- isCC: boolean (nullable = true)
#  |-- isHL: boolean (nullable = true)
#  |-- isAL: boolean (nullable = true)
#  |-- isTW: boolean (nullable = true)
#  |-- isGL: boolean (nullable = true)
#  |-- isGLExt: boolean (nullable = true)
#  |-- isPL: boolean (nullable = true)
#  |-- isAgri: boolean (nullable = true)
#  |-- isComSec: boolean (nullable = true)
#  |-- isComUnSec: boolean (nullable = true)
#  |-- isRegular: boolean (nullable = true)
#  |-- isPlCdTw: boolean (nullable = true)
#  |-- isLapHl: boolean (nullable = true)
#  |-- isUns: boolean (nullable = true)
#  |-- isSec: boolean (nullable = true)
#  |-- isSecMov: boolean (nullable = true)
#  |-- isSecMovRegSec: boolean (nullable = true)
#  |-- isRegSec: boolean (nullable = true)
#  |-- isRegUns: boolean (nullable = true)
#  |-- isComCls: boolean (nullable = true)
#  |-- isPSB: boolean (nullable = true)
#  |-- isPVT: boolean (nullable = true)
#  |-- isNBFC: boolean (nullable = true)
#  |-- isSFB: boolean (nullable = true)
#  |-- bank_category: string (nullable = true)
#  |-- Pdt_Cls: string (nullable = true)
#  |-- Sec_Uns: string (nullable = true)
#  |-- Reg_Com: string (nullable = true)
#  |-- isCL: boolean (nullable = true)
#  |-- Sec_Mov: string (nullable = true)
#  |-- latest_modified_limit: double (nullable = true)
#  |-- isLiveAccount: boolean (nullable = true)
#  |-- isReportedLiveIn12M: boolean (nullable = true)
#  |-- reportedIn36M: boolean (nullable = true)
#  |-- reportedIn12M: boolean (nullable = true)
#  |-- derog: boolean (nullable = true)
#  |-- monthly_data: array (nullable = false)
#  |    |-- element: struct (containsNull = false)
#  |    |    |-- idx: integer (nullable = true)
#  |    |    |-- dpd: integer (nullable = true)
#  |    |    |-- raw_dpd: integer (nullable = true)
#  |    |    |-- bal: double (nullable = true)
#  |    |    |-- mod_lim: double (nullable = true)
#  |    |    |-- asset_cd: string (nullable = true)
#  |    |    |-- wo_status: string (nullable = true)
#  |    |    |-- sfw_status: string (nullable = true)
#  |    |    |-- pymnt_amt: double (nullable = true)
#  |    |    |-- acct_status: string (nullable = true)
#  |    |    |-- past_due: double (nullable = true)
#  |    |    |-- derog_flag: boolean (nullable = true)
#  |    |    |-- is_not_closed: boolean (nullable = true)
#  |-- max_dpd_l36m: integer (nullable = true)
#  |-- max_dpd_l12m: integer (nullable = true)
#  |-- dpd_m0: integer (nullable = true)
#  |-- pct_instance_l12m: double (nullable = true)
#  |-- avgBal_l3m: double (nullable = true)
#  |-- avgModifiedLimit_l3m: double (nullable = true)
#  |-- bal_m0: double (nullable = true)
#  |-- latestReportedIndex: integer (nullable = true)
#  |-- max_bal_l24m: double (nullable = true)
#  |-- max_bal_l12m: double (nullable = true)
#  |-- mob: integer (nullable = true)
#  |-- firstReportedOnlyIn36M: boolean (nullable = true)
#  |-- m_since_max_bal_l24m: integer (nullable = true)
#  |-- mon_since_first_worst_delq: integer (nullable = true)
#  |-- mon_since_recent_worst_delq: integer (nullable = true)
# cols_ = account_details.columns
batch_size = 4
​
for i in range(0, len(cols_), batch_size):
    batch = cols_[i:i + batch_size]
    print(f"\n=== Columns {i+1} to {i+len(batch)} ===")
    account_details.select(batch).show(25, truncate=False)
# Last executed at 2026-03-24 20:29:13 in 5.40s
# Spark Job Progress

# === Columns 1 to 4 ===
# +--------+-------------+--------+---------------+
# |cust_id |cons_acct_key|accno   |account_type_cd|
# +--------+-------------+--------+---------------+
# |10004499|531045921    |10004499|5              |
# |10005593|3733639255   |10005593|191            |
# |10005593|6510367287   |10005593|191            |
# |10003821|1060068564   |10003821|58             |
# |10003821|1294988501   |10003821|195            |
# |10004499|1819004543   |10004499|58             |
# |10004499|220732246    |10004499|5              |
# |10005593|2678456497   |10005593|47             |
# |10005593|4216313220   |10005593|191            |
# |10005593|4608244802   |10005593|178            |
# |10003821|4243522267   |10003821|173            |
# |10003821|5680303167   |10003821|47             |
# |10005593|4271825530   |10005593|999            |
# |10005593|7069962019   |10005593|191            |
# |10005593|5223241838   |10005593|191            |
# |10005593|5353837342   |10005593|178            |
# |10005593|6231042991   |10005593|191            |
# |10005593|193064962    |10005593|5              |
# |10005593|5210715938   |10005593|178            |
# |10005593|3433964206   |10005593|189            |
# |10005593|3617567571   |10005593|999            |
# |10005593|3786084470   |10005593|189            |
# |10005593|5035793486   |10005593|121            |
# |10005593|5472717732   |10005593|224            |
# |10003821|2382601869   |10003821|5              |
# +--------+-------------+--------+---------------+
# only showing top 25 rows


# === Columns 5 to 8 ===
# +--------+---------+--------+-------------+
# |open_dt |closed_dt|score_dt|bureau_mbr_id|
# +--------+---------+--------+-------------+
# |20150828|NULL     |20250930|312          |
# |20211222|NULL     |20250930|359          |
# |20241022|NULL     |20250930|627          |
# |20160831|NULL     |20250930|312          |
# |20170512|NULL     |20250930|312          |
# |20180628|NULL     |20250930|805          |
# |20050106|NULL     |20250930|337          |
# |20200302|NULL     |20250930|805          |
# |20220707|NULL     |20250930|137134       |
# |20221221|NULL     |20250930|627          |
# |20220630|NULL     |20250930|312          |
# |20240104|NULL     |20250930|103          |
# |20220810|NULL     |20250930|357          |
# |20250315|NULL     |20250930|972          |
# |20230814|NULL     |20250930|357          |
# |20231025|NULL     |20250930|627          |
# |20240718|NULL     |20250930|627          |
# |20080605|NULL     |20250930|79           |
# |20230801|NULL     |20250930|627          |
# |20210627|NULL     |20250930|1037         |
# |20210914|NULL     |20250930|149152       |
# |20210627|NULL     |20250930|9302         |
# |20230603|NULL     |20250930|312          |
# |20230707|NULL     |20250930|137134       |
# |20141118|NULL     |20250930|685          |
# +--------+---------+--------+-------------+
# only showing top 25 rows


# === Columns 9 to 12 ===
# +-----+-----+-----+-----+
# |isCC |isHL |isAL |isTW |
# +-----+-----+-----+-----+
# |true |false|false|false|
# |false|false|false|false|
# |false|false|false|false|
# |false|true |false|false|
# |false|false|false|false|
# |false|true |false|false|
# |true |false|false|false|
# |false|false|true |false|
# |false|false|false|false|
# |false|false|false|false|
# |false|false|false|true |
# |false|false|true |false|
# |false|false|false|false|
# |false|false|false|false|
# |false|false|false|false|
# |false|false|false|false|
# |false|false|false|false|
# |true |false|false|false|
# |false|false|false|false|
# |false|false|false|false|
# |false|false|false|false|
# |false|false|false|false|
# |true |false|false|false|
# |false|false|false|false|
# |true |false|false|false|
# +-----+-----+-----+-----+
# only showing top 25 rows


# === Columns 13 to 16 ===
# +-----+-------+-----+------+
# |isGL |isGLExt|isPL |isAgri|
# +-----+-------+-----+------+
# |false|false  |false|false |
# |true |true   |false|false |
# |true |true   |false|false |
# |false|false  |false|false |
# |false|false  |true |false |
# |false|false  |false|false |
# |false|false  |false|false |
# |false|false  |false|false |
# |true |true   |false|false |
# |false|false  |false|true  |
# |false|false  |false|false |
# |false|false  |false|false |
# |false|false  |false|false |
# |true |true   |false|false |
# |true |true   |false|false |
# |false|false  |false|true  |
# |true |true   |false|false |
# |false|false  |false|false |
# |false|false  |false|true  |
# |false|false  |false|false |
# |false|false  |false|false |
# |false|false  |false|false |
# |false|false  |false|false |
# |false|false  |false|true  |
# |false|false  |false|false |
# +-----+-------+-----+------+
# only showing top 25 rows


# === Columns 17 to 20 ===
# +--------+----------+---------+--------+
# |isComSec|isComUnSec|isRegular|isPlCdTw|
# +--------+----------+---------+--------+
# |false   |false     |false    |false   |
# |true    |false     |true     |false   |
# |true    |false     |true     |false   |
# |false   |false     |false    |false   |
# |false   |false     |true     |false   |
# |false   |false     |false    |false   |
# |false   |false     |false    |false   |
# |false   |false     |false    |false   |
# |true    |false     |true     |false   |
# |false   |true      |false    |false   |
# |false   |false     |true     |true    |
# |false   |false     |false    |false   |
# |false   |false     |false    |false   |
# |true    |false     |true     |false   |
# |true    |false     |true     |false   |
# |false   |true      |false    |false   |
# |true    |false     |true     |false   |
# |false   |false     |false    |false   |
# |false   |true      |false    |false   |
# |false   |false     |false    |true    |
# |false   |false     |false    |false   |
# |false   |false     |false    |true    |
# |false   |false     |true     |false   |
# |false   |false     |false    |false   |
# |false   |false     |false    |false   |
# +--------+----------+---------+--------+
# only showing top 25 rows


# === Columns 21 to 24 ===
# +-------+-----+-----+--------+
# |isLapHl|isUns|isSec|isSecMov|
# +-------+-----+-----+--------+
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |true   |NULL |NULL |NULL    |
# |true   |NULL |NULL |NULL    |
# |true   |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# |false  |NULL |NULL |NULL    |
# +-------+-----+-----+--------+
# only showing top 25 rows


# === Columns 25 to 28 ===
# +--------------+--------+--------+--------+
# |isSecMovRegSec|isRegSec|isRegUns|isComCls|
# +--------------+--------+--------+--------+
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# |NULL          |NULL    |NULL    |NULL    |
# +--------------+--------+--------+--------+
# only showing top 25 rows


# === Columns 29 to 32 ===
# +-----+-----+------+-----+
# |isPSB|isPVT|isNBFC|isSFB|
# +-----+-----+------+-----+
# |false|true |false |false|
# |false|false|false |false|
# |false|false|false |false|
# |false|true |false |false|
# |false|true |false |false|
# |false|false|false |false|
# |false|true |false |false|
# |false|false|false |false|
# |false|false|false |false|
# |false|false|false |false|
# |false|true |false |false|
# |false|false|false |false|
# |false|false|false |false|
# |false|false|false |false|
# |false|false|false |false|
# |false|false|false |false|
# |false|false|false |false|
# |false|true |false |false|
# |false|false|false |false|
# |false|false|true  |false|
# |false|false|true  |false|
# |false|false|false |false|
# |false|true |false |false|
# |false|false|false |false|
# |false|false|true  |false|
# +-----+-----+------+-----+
# only showing top 25 rows


# === Columns 33 to 36 ===
# +-------------+-------+-------+-------+
# |bank_category|Pdt_Cls|Sec_Uns|Reg_Com|
# +-------------+-------+-------+-------+
# |PVT          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |PVT          |NULL   |NULL   |NULL   |
# |PVT          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |PVT          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |RRB          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |PVT          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |PVT          |NULL   |NULL   |NULL   |
# |PUB          |NULL   |NULL   |NULL   |
# |NBF          |NULL   |NULL   |NULL   |
# |NBF          |NULL   |NULL   |NULL   |
# |COB          |NULL   |NULL   |NULL   |
# |PVT          |NULL   |NULL   |NULL   |
# |RRB          |NULL   |NULL   |NULL   |
# |NBF          |NULL   |NULL   |NULL   |
# +-------------+-------+-------+-------+
# only showing top 25 rows


# === Columns 37 to 40 ===
# +-----+-------+---------------------+-------------+
# |isCL |Sec_Mov|latest_modified_limit|isLiveAccount|
# +-----+-------+---------------------+-------------+
# |false|NULL   |135610.0             |true         |
# |false|NULL   |300000.0             |true         |
# |false|NULL   |150000.0             |true         |
# |false|NULL   |2000000.0            |true         |
# |false|NULL   |81095.0              |true         |
# |false|NULL   |77774.0              |true         |
# |false|NULL   |51771.0              |true         |
# |false|NULL   |800000.0             |true         |
# |false|NULL   |201028.0             |true         |
# |false|NULL   |200000.0             |true         |
# |false|NULL   |212659.0             |true         |
# |false|NULL   |1201727.0            |true         |
# |false|NULL   |147959.0             |true         |
# |false|NULL   |200798.0             |true         |
# |false|NULL   |139593.0             |true         |
# |false|NULL   |150000.0             |true         |
# |false|NULL   |200000.0             |true         |
# |false|NULL   |25000.0              |true         |
# |false|NULL   |200000.0             |true         |
# |false|NULL   |5000.0               |true         |
# |false|NULL   |10000.0              |true         |
# |false|NULL   |20000.0              |true         |
# |false|NULL   |353000.0             |true         |
# |false|NULL   |200000.0             |true         |
# |false|NULL   |90000.0              |true         |
# +-----+-------+---------------------+-------------+
# only showing top 25 rows


# === Columns 41 to 44 ===
# +-------------------+-------------+-------------+-----+
# |isReportedLiveIn12M|reportedIn36M|reportedIn12M|derog|
# +-------------------+-------------+-------------+-----+
# |false              |true         |true         |false|
# |NULL               |true         |NULL         |false|
# |false              |true         |true         |false|
# |NULL               |true         |NULL         |false|
# |NULL               |true         |NULL         |false|
# |false              |true         |true         |false|
# |false              |true         |true         |false|
# |false              |true         |true         |false|
# |NULL               |true         |NULL         |false|
# |NULL               |true         |NULL         |false|
# |NULL               |true         |NULL         |false|
# |false              |true         |true         |false|
# |NULL               |true         |NULL         |false|
# |false              |true         |true         |false|
# |NULL               |true         |NULL         |false|
# |false              |true         |true         |false|
# |false              |true         |true         |false|
# |false              |true         |true         |true |
# |NULL               |true         |NULL         |false|
# |false              |true         |true         |false|
# |NULL               |true         |NULL         |false|
# |false              |true         |true         |false|
# |false              |true         |true         |false|
# |NULL               |true         |NULL         |false|
# |NULL               |true         |NULL         |false|
# +-------------------+-------------+-------------+-----+
# only showing top 25 rows


# === Columns 45 to 48 ===
# +---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+------------+------+
# |monthly_data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |max_dpd_l36m|max_dpd_l12m|dpd_m0|
# +---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+------------+------+
# |[{35, 0, 0, 31084.0, 135610.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {33, 0, 0, 8672.0, 135610.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {32, 0, 0, 16458.0, 135610.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {31, 0, 0, 9504.0, 135610.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {30, 0, 0, 6692.0, 135610.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {29, 0, 0, 10478.0, 135610.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {28, 0, 0, 28481.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {27, 0, 0, 5140.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {26, 0, 0, 38608.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {25, 0, 0, 33498.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {24, 0, 0, 23038.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {23, 0, 0, 2276.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {22, 0, 0, 17946.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {21, 0, 0, 31075.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {20, 0, 0, 2012.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {19, 0, 0, 12012.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {18, 0, 0, 16921.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {17, 0, 0, 37733.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {16, 0, 0, 6533.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {15, 0, 0, 3813.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {14, 0, 0, 8945.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {13, 0, 0, 9558.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {12, 0, 0, 47319.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {11, 0, 0, 21917.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {10, 0, 0, 19782.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {9, 0, 0, 4045.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {8, 0, 0, 8271.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {7, 0, 0, 10706.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {6, 0, 0, 29545.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {5, 0, 0, 19502.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {4, 0, 0, 24360.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {3, 0, 0, 3203.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}, {2, 0, 0, 9444.0, 318000.0, -1, -1, -1, -1.0, O  , -1.0, false, true}]                                                                                                                                                                                                                                                                                                          |0           |0           |NULL  |
# |[{35, 0, -1, 300000.0, 300000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {34, 0, -1, 300000.0, 300000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {33, 0, -1, 0.0, 300000.0, S, -1, -1, -1.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |0           |NULL        |NULL  |
# |[{11, 0, 0, 150000.0, 150000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {10, 0, 0, 150000.0, 150000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {9, 0, 0, 150000.0, 150000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {8, 0, 0, 150000.0, 150000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {7, 0, 0, 150000.0, 150000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {6, 0, 0, 155885.0, 155885.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {5, 0, 0, 155885.0, 155885.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {4, 0, 0, 155885.0, 155885.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {3, 0, 0, 155885.0, 155885.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {2, 0, 0, 155885.0, 155885.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {1, 0, 0, 155885.0, 155885.0, -1, -1, 0, -1.0, O  , 0.0, false, true}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |0           |0           |NULL  |
# |[{35, 0, 0, 1492393.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {34, 0, 0, 1483863.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {33, 0, 0, 1475893.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {32, 0, 0, 1467866.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {31, 0, 0, 1459781.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {30, 0, 0, 1452367.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {29, 0, 0, 1444896.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {28, 0, 0, 1437368.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {27, 0, 0, 1429782.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {26, 0, 0, 1422137.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {25, 0, 0, 1414433.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {24, 0, 0, 1406670.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {23, 0, 0, 1398847.0, 2000000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {22, 0, 0, 0.0, 2000000.0, -1, -1, 0, -1.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |0           |NULL        |NULL  |
# |[{35, 0, 0, 62653.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {34, 0, 0, 62343.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {33, 0, 0, 62057.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {32, 0, 0, 61768.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {31, 0, 0, 61476.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {30, 0, 0, 61212.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {29, 0, 0, 60946.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {28, 0, 0, 60677.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {27, 0, 0, 60406.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {26, 0, 0, 60132.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {25, 0, 0, 59855.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {24, 0, 0, 59576.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {23, 0, 0, 59294.0, 81095.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {22, 0, 0, 0.0, 81095.0, -1, -1, 0, -1.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |0           |NULL        |NULL  |
# |[{35, 0, -1, 64862.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {34, 0, -1, 64493.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {33, 0, -1, 64135.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {32, 0, -1, 63775.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {31, 0, -1, 63373.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {30, 0, -1, 63008.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {29, 0, -1, 62628.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {28, 0, -1, 62258.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {27, 0, -1, 61873.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {26, 0, -1, 61554.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {25, 0, -1, 61234.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {24, 0, -1, 60898.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {23, 0, -1, 60573.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {22, 0, -1, 60232.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {21, 0, -1, 59902.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {20, 0, -1, 59570.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {19, 0, -1, 59208.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {18, 0, -1, 58871.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {17, 0, -1, 58518.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {15, 0, -1, 57818.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {14, 0, -1, 57483.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {13, 0, -1, 57145.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {12, 0, -1, 56779.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {11, 0, -1, 56424.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {10, 0, -1, 56053.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {9, 0, -1, 55693.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {8, 0, -1, 55330.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {7, 0, -1, 54924.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {6, 0, -1, 54555.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {5, 0, -1, 54170.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {4, 0, -1, 53795.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {3, 0, -1, 53405.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {2, 0, -1, 53035.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {1, 0, -1, 52255.0, 77774.0, S, -1, -1, -1.0, O  , -1.0, false, true}]                                                                                                                                                                                                                                                      |0           |0           |NULL  |
# |[{14, 0, 0, -1.0, 51771.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {8, 0, 0, -1.0, 52000.0, -1, -1, -1, -1.0, C  , 0.0, false, false}, {7, 0, 0, -1.0, 52000.0, -1, -1, -1, -1.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |0           |0           |NULL  |
# |[{35, 0, -1, 547489.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {34, 0, -1, 538339.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {33, 0, -1, 529249.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {32, 0, -1, 520091.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {31, 0, -1, 510503.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {30, 0, -1, 501207.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {29, 0, -1, 491726.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {28, 0, -1, 482294.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {27, 0, -1, 472678.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {26, 0, -1, 463106.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {25, 0, -1, 453460.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {24, 0, -1, 443638.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {23, 0, -1, 433848.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {22, 0, -1, 423888.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {21, 0, -1, 413952.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {20, 0, -1, 403943.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {19, 0, -1, 393673.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {18, 0, -1, 383514.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {17, 0, -1, 373194.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {16, 0, -1, 362883.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {15, 0, -1, 352413.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {14, 0, -1, 341949.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {13, 0, -1, 331408.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {12, 0, -1, 320713.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {11, 0, -1, 310015.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {10, 0, -1, 299167.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {9, 0, -1, 288309.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {8, 0, -1, 277371.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {7, 0, -1, 266163.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {6, 0, -1, 255061.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {5, 0, -1, 243820.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {4, 0, -1, 232554.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {3, 0, -1, 221152.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}, {2, 0, -1, 194697.0, 800000.0, S, -1, -1, 5000.0, O  , -1.0, false, true}, {1, 0, -1, 181697.0, 800000.0, S, -1, -1, 13000.0, O  , -1.0, false, true}]|0           |0           |NULL  |
# |[{35, 0, -1, 201028.0, 201028.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {34, 0, -1, 201028.0, 201028.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {33, 0, -1, 201028.0, 201028.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {32, 0, -1, 201028.0, 201028.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {31, 0, -1, 201028.0, 201028.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {28, 0, -1, 201028.0, 201028.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {26, 0, -1, 0.0, 200000.0, S, -1, -1, -1.0, C  , -1.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |0           |NULL        |NULL  |
# |[{33, 0, 0, 200000.0, 200000.0, S, -1, 0, 590.0, O  , 0.0, false, true}, {32, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {31, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {30, 0, 0, 204483.0, 204483.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {29, 0, 0, 204483.0, 204483.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {28, 0, 0, 204483.0, 204483.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {27, 0, 0, 0.0, 200000.0, S, -1, 0, 208431.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |0           |NULL        |NULL  |
# |[{35, 0, 0, 189570.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {34, 0, 0, 181671.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {33, 0, 0, 173669.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {32, 0, 0, 165561.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {31, 0, 0, 157347.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {30, 0, 0, 149025.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {29, 0, 0, 140594.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {28, 0, 0, 132052.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {27, 0, 0, 123398.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {26, 0, 0, 114631.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {25, 0, 0, 105748.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {24, 0, 0, 96749.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {23, 0, 0, 87632.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {22, 0, 0, 78395.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {21, 0, 0, 69037.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {20, 0, 0, 59556.0, 212659.0, S, -1, 0, 10387.0, O  , 0.0, false, true}, {19, 0, 0, 49951.0, 212659.0, S, -1, 0, 10387.0, O  , 0.0, false, true}, {18, 0, 0, 40219.0, 212659.0, S, -1, 0, 10387.0, O  , 0.0, false, true}, {17, 0, 0, 30360.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {16, 0, 0, 20371.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {15, 0, 0, 10252.0, 212659.0, S, -1, -1, 10387.0, O  , 0.0, false, true}, {14, 0, 0, 0.0, 212659.0, S, -1, -1, 10387.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |0           |NULL        |NULL  |
# |[{20, 1, 22, 1201727.0, 1201727.0, S, 99, 0, 0.0, O  , 1727.0, false, true}, {19, 2, 51, 1210658.0, 1210658.0, S, 99, 0, 0.0, O  , 26492.0, false, true}, {17, 2, 48, 1179479.0, 1200000.0, S, 99, 0, 49510.0, O  , 27556.0, false, true}, {16, 2, 52, 1163174.0, 1200000.0, S, 99, 0, 74275.0, O  , 27556.0, false, true}, {15, 2, 52, 1147030.0, 1200000.0, S, 99, 0, 99040.0, O  , 27556.0, false, true}, {14, 2, 52, 1130492.0, 1200000.0, S, 99, 0, 123805.0, O  , 27556.0, false, true}, {13, 2, 53, 1114153.0, 1200000.0, S, 99, 0, 148570.0, O  , 27598.0, false, true}, {12, 0, 0, 1069812.0, 1200000.0, S, 99, 0, 200965.0, O  , 0.0, false, true}, {11, 0, 0, 1052837.0, 1200000.0, -1, 99, 0, 225748.0, O  , 0.0, false, true}, {10, 0, 0, 1035875.0, 1200000.0, -1, 99, 0, 250513.0, O  , 0.0, false, true}, {9, 0, 0, 1018539.0, 1200000.0, -1, 99, 0, 275278.0, O  , 0.0, false, true}, {8, 0, 0, 1001329.0, 1200000.0, -1, 99, 0, 300043.0, O  , 0.0, false, true}, {7, 0, 0, 983985.0, 1200000.0, -1, 99, 0, 324808.0, O  , 0.0, false, true}, {6, 0, 0, 465636.0, 1200000.0, -1, 99, 0, 349573.0, O  , 0.0, false, true}, {5, 0, 0, 445510.0, 1200000.0, -1, 99, 0, 374338.0, O  , 0.0, false, true}, {4, 0, 0, 423766.0, 1200000.0, -1, 99, 0, 399103.0, O  , 0.0, false, true}, {3, 0, 0, 401947.0, 1200000.0, -1, 99, 0, 423868.0, O  , 0.0, false, true}, {2, 0, 0, 379742.0, 1200000.0, -1, 99, 0, 448633.0, O  , 0.0, false, true}, {1, 0, 0, 357477.0, 1200000.0, -1, 99, 0, 473398.0, O  , 0.0, false, true}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |2           |0           |NULL  |
# |[{35, 0, -1, 147959.0, 147959.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {34, 0, -1, 147959.0, 147959.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {33, 0, -1, 147959.0, 147959.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {32, 0, -1, 147959.0, 147959.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {31, 0, -1, 147959.0, 147959.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {30, 0, -1, 152878.0, 152878.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {29, 0, -1, 152878.0, 152878.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {28, 0, -1, 152878.0, 152878.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {27, 0, -1, 152878.0, 152878.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {26, 0, -1, 152878.0, 152878.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {19, 0, -1, 0.0, 147000.0, S, -1, -1, -1.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |0           |NULL        |NULL  |
# |[{6, 0, 0, 200798.0, 200798.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {5, 0, 0, 202300.0, 202300.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {4, 0, 0, 203864.0, 203864.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {3, 0, 0, 205389.0, 205389.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {2, 0, 0, 196944.0, 200000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {1, 0, 0, 196944.0, 200000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |0           |0           |NULL  |
# |[{25, 0, -1, 139593.0, 139593.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {24, 0, -1, 140581.0, 140581.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {23, 0, -1, 140581.0, 140581.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {22, 0, -1, 140581.0, 140581.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {21, 0, -1, 140581.0, 140581.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {20, 0, -1, 140581.0, 140581.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {19, 0, -1, 140581.0, 140581.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {18, 0, -1, 146609.0, 146609.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {17, 0, -1, 146609.0, 146609.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {16, 0, -1, 146609.0, 146609.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {15, 0, -1, 146609.0, 146609.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {14, 0, -1, 146609.0, 146609.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {13, 0, -1, 0.0, 139000.0, S, -1, -1, -1.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |0           |NULL        |NULL  |
# |[{23, 0, 0, 150000.0, 150000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {22, 0, 0, 150000.0, 150000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {21, 0, 0, 150000.0, 150000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {20, 0, 0, 150000.0, 150000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {19, 0, 0, 150000.0, 150000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {18, 0, 0, 155652.0, 155652.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {17, 0, 0, 155652.0, 155652.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {16, 0, 0, 155652.0, 155652.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {15, 0, 0, 155652.0, 155652.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {14, 0, 0, 155652.0, 155652.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {13, 0, 0, 155652.0, 155652.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {12, 0, 0, 155652.0, 155652.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {11, 0, 0, 0.0, 150000.0, S, -1, 0, 163215.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |0           |0           |NULL  |
# |[{14, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {13, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {12, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {11, 0, 0, 203588.0, 203588.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {10, 0, 0, 203588.0, 203588.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {9, 0, 0, 200000.0, 200000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {8, 0, 0, 200000.0, 200000.0, -1, -1, 0, -1.0, O  , 0.0, false, true}, {7, 0, 0, 0.0, 200000.0, -1, -1, 0, 209602.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |0           |0           |NULL  |
# |[{35, 0, 0, 0.0, 25000.0, -1, 2, -1, 5111.0, O  , 0.0, true, true}, {34, 0, 0, 0.0, 25000.0, -1, 2, -1, 5111.0, O  , 0.0, true, true}, {33, 0, 0, 0.0, 25000.0, -1, 2, -1, 5111.0, O  , 0.0, true, true}, {32, 0, 0, 0.0, 25000.0, -1, 2, -1, 5111.0, O  , 0.0, true, true}, {31, 0, 0, 0.0, 25000.0, -1, 2, -1, 5111.0, O  , 0.0, true, true}, {30, 0, 0, 0.0, 25000.0, -1, 2, -1, 5111.0, O  , 0.0, true, true}, {29, 0, 0, 0.0, 25000.0, -1, 2, -1, 5111.0, O  , 0.0, true, true}, {28, 0, 0, 0.0, 25000.0, -1, 2, -1, 5111.0, O  , 0.0, true, true}, {27, 0, 0, 0.0, 25000.0, -1, 2, -1, 5111.0, O  , 0.0, true, true}, {26, 0, 0, 0.0, 25000.0, -1, 2, -1, 5111.0, O  , 0.0, true, true}, {25, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {24, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {23, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {22, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {21, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {20, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {19, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {18, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {17, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {16, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {15, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {14, 0, 0, 0.0, 25000.0, -1, 2, -1, -1.0, O  , 0.0, true, true}, {13, 0, 0, 0.0, 25000.0, -1, -1, -1, -1.0, C  , 0.0, false, false}, {4, 0, 0, 0.0, 25000.0, -1, 99, -1, -1.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |0           |0           |NULL  |
# |[{25, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {24, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {23, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {22, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {21, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {20, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {19, 0, 0, 200000.0, 200000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {18, 0, 0, 211498.0, 211498.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {17, 0, 0, 211498.0, 211498.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {16, 0, 0, 211498.0, 211498.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {15, 0, 0, 211498.0, 211498.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {14, 0, 0, 0.0, 200000.0, S, -1, 0, -1.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |0           |NULL        |NULL  |
# |[{35, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {34, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {33, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {32, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {31, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {30, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {29, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {28, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {27, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {26, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {25, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {24, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {23, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {22, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {21, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {20, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {19, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {18, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {17, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {16, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {15, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {14, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {13, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {12, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {11, 0, 0, 0.0, 5000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {10, 0, 0, 0.0, 5000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {9, 0, 0, 0.0, 5000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {8, 0, 0, 0.0, 5000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {7, 0, 0, 0.0, 5000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {6, 0, 0, 298.0, 5000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {5, 0, 0, 718.0, 5000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {4, 0, 0, 170.0, 5000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {3, 0, 0, 510.0, 5000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {2, 0, 0, 39.0, 5000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {1, 0, 0, 0.0, 5000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}]                                                                                                                                                                                                                                                                                                                                                                                                                |0           |0           |NULL  |
# |[{35, 0, 0, 2403.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {34, 0, 0, 7009.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {33, 0, 0, 800.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {32, 0, 0, 3065.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {31, 0, 0, 4215.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {30, 0, 0, 200.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {29, 0, 0, 450.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {28, 0, 0, 0.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {27, 0, 0, 5718.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {26, 0, 0, 4107.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {25, 0, 0, 4013.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {24, 0, 0, 1330.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {23, 0, 0, 5356.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {22, 0, 0, 7783.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {21, 0, 0, 0.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {20, 0, 0, 0.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {19, 0, 0, 0.0, 10000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {18, 0, 0, 0.0, 10000.0, S, -1, -1, -1.0, C  , 0.0, false, false}, {17, 0, 0, 0.0, 10000.0, S, -1, -1, -1.0, C  , 0.0, false, false}, {16, 0, 0, 0.0, 10000.0, S, -1, -1, -1.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |0           |NULL        |NULL  |
# |[{35, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {34, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {33, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {32, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {31, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {30, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {28, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {27, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {26, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {25, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {24, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {23, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {22, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {21, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {20, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {19, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {18, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {17, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {16, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {15, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {14, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {13, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {12, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {11, 0, 0, 0.0, 20000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {10, 0, 0, 0.0, 20000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {9, 0, 0, 0.0, 20000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {8, 0, 0, 0.0, 20000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {7, 0, 0, 0.0, 20000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {6, 0, 0, 1192.0, 20000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {5, 0, 0, 2870.0, 20000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {4, 0, 0, 680.0, 20000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {3, 0, 0, 2042.0, 20000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {2, 0, 0, 155.0, 20000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |0           |0           |NULL  |
# |[{27, 0, 0, 297201.0, 353000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {26, 0, 0, 201110.0, 353000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {25, 0, 0, 270657.0, 353000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {24, 0, 0, 117000.0, 353000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {23, 0, 0, 164763.0, 353000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {22, 0, 0, 332325.0, 353000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {21, 0, 0, 243557.0, 353000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {20, 0, 0, 491041.0, 509000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {19, 0, 0, 409000.0, 509000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {18, 0, 0, 308065.0, 509000.0, S, -1, 0, -1.0, O  , 0.0, false, true}, {17, 0, 0, 0.0, 509000.0, S, -1, -1, 579156.0, O  , 0.0, false, true}, {16, 0, 0, 0.0, 0.0, S, -1, -1, -1.0, C  , 0.0, false, false}, {15, 0, 0, 1359.0, 561000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {14, 0, 0, 442943.0, 624000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {13, 0, 0, 446101.0, 624000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {12, 0, 0, 536456.0, 624000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {11, 0, 0, 583019.0, 624000.0, S, -1, -1, -1.0, O  , 0.0, false, true}, {10, 0, 0, 593729.0, 624000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {9, 0, 0, 610006.0, 624000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {8, 0, 0, 447311.0, 592000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {7, 0, 0, 507955.0, 564000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {6, 0, 0, 578886.0, 586000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {5, 0, 0, 136864.0, 607000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {4, 0, 0, 576688.0, 624000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {3, 0, 0, 605356.0, 624000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {2, 0, 0, 512962.0, 624000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}, {1, 0, 0, 358463.0, 624000.0, -1, -1, -1, -1.0, O  , 0.0, false, true}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |0           |0           |NULL  |
# |[{26, 0, -1, -200000.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {25, 0, -1, -200000.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {24, 0, -1, -203299.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {23, 0, -1, -203299.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {22, 0, -1, -203299.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {21, 0, -1, -203299.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {20, 0, -1, -203299.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {19, 0, -1, -203299.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {18, 0, -1, -210850.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {17, 0, -1, -210850.0, 200000.0, S, -1, 0, -1.0, O  , -1.0, false, true}, {16, 0, -1, -210850.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {15, 0, -1, -210850.0, 200000.0, S, -1, -1, -1.0, O  , -1.0, false, true}, {14, 0, -1, 0.0, 200000.0, S, -1, -1, -1.0, C  , -1.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |0           |NULL        |NULL  |
# |[{35, 0, 0, -6004.0, 90000.0, -1, -1, -1, 0.0, O  , 0.0, false, true}, {34, 0, 0, -6004.0, 90000.0, -1, -1, -1, 0.0, O  , 0.0, false, true}, {33, 0, 0, -5415.0, 90000.0, -1, -1, -1, 589.0, O  , 0.0, false, true}, {32, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, O  , 0.0, false, true}, {31, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, O  , 0.0, false, true}, {29, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, O  , 0.0, false, true}, {28, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, O  , 0.0, false, true}, {27, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, O  , 0.0, false, true}, {26, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, O  , 0.0, false, true}, {25, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, O  , 0.0, false, true}, {24, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {23, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {22, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {20, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {19, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {18, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {17, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {16, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {15, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {14, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {13, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}, {12, 0, 0, -5415.0, 90000.0, -1, -1, -1, 0.0, C  , 0.0, false, false}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |0           |NULL        |NULL  |
# +---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+------------+------+
# only showing top 25 rows


# === Columns 49 to 52 ===
# +-----------------+----------+--------------------+------+
# |pct_instance_l12m|avgBal_l3m|avgModifiedLimit_l3m|bal_m0|
# +-----------------+----------+--------------------+------+
# |100.0            |9444.0    |318000.0            |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |100.0            |155885.0  |155885.0            |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |100.0            |52645.0   |77774.0             |NULL  |
# |0.0              |NULL      |NULL                |NULL  |
# |100.0            |188197.0  |800000.0            |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |100.0            |368609.5  |1200000.0           |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |100.0            |196944.0  |200000.0            |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |0.0              |NULL      |NULL                |NULL  |
# |80.0             |NULL      |NULL                |NULL  |
# |0.0              |NULL      |NULL                |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |45.45454545454545|19.5      |5000.0              |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |50.0             |155.0     |20000.0             |NULL  |
# |100.0            |435712.5  |624000.0            |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# |NULL             |NULL      |NULL                |NULL  |
# +-----------------+----------+--------------------+------+
# only showing top 25 rows


# === Columns 53 to 56 ===
# +-------------------+------------+------------+---+
# |latestReportedIndex|max_bal_l24m|max_bal_l12m|mob|
# +-------------------+------------+------------+---+
# |2                  |47319.0     |29545.0     |121|
# |33                 |NULL        |NULL        |45 |
# |1                  |155885.0    |155885.0    |11 |
# |22                 |1398847.0   |NULL        |109|
# |22                 |59294.0     |NULL        |100|
# |1                  |60573.0     |56424.0     |87 |
# |7                  |-1.0        |-1.0        |248|
# |1                  |433848.0    |310015.0    |66 |
# |26                 |NULL        |NULL        |38 |
# |27                 |NULL        |NULL        |33 |
# |14                 |87632.0     |NULL        |39 |
# |1                  |1210658.0   |1052837.0   |20 |
# |19                 |0.0         |NULL        |37 |
# |1                  |205389.0    |205389.0    |6  |
# |13                 |146609.0    |NULL        |25 |
# |11                 |155652.0    |0.0         |23 |
# |7                  |203588.0    |203588.0    |14 |
# |4                  |0.0         |0.0         |207|
# |14                 |211498.0    |NULL        |25 |
# |1                  |718.0       |718.0       |51 |
# |16                 |7783.0      |NULL        |48 |
# |2                  |2870.0      |2870.0      |51 |
# |1                  |610006.0    |610006.0    |27 |
# |14                 |0.0         |NULL        |26 |
# |12                 |-5415.0     |NULL        |130|
# +-------------------+------------+------------+---+
# only showing top 25 rows


# === Columns 57 to 60 ===
# +----------------------+--------------------+--------------------------+---------------------------+
# |firstReportedOnlyIn36M|m_since_max_bal_l24m|mon_since_first_worst_delq|mon_since_recent_worst_delq|
# +----------------------+--------------------+--------------------------+---------------------------+
# |false                 |12                  |NULL                      |NULL                       |
# |true                  |NULL                |NULL                      |NULL                       |
# |false                 |1                   |NULL                      |NULL                       |
# |true                  |23                  |NULL                      |NULL                       |
# |true                  |23                  |NULL                      |NULL                       |
# |false                 |23                  |NULL                      |NULL                       |
# |false                 |7                   |NULL                      |NULL                       |
# |false                 |23                  |NULL                      |NULL                       |
# |true                  |NULL                |NULL                      |NULL                       |
# |true                  |NULL                |NULL                      |NULL                       |
# |true                  |23                  |NULL                      |NULL                       |
# |false                 |19                  |19                        |13                         |
# |true                  |19                  |NULL                      |NULL                       |
# |false                 |3                   |NULL                      |NULL                       |
# |true                  |14                  |NULL                      |NULL                       |
# |false                 |12                  |NULL                      |NULL                       |
# |false                 |10                  |NULL                      |NULL                       |
# |false                 |4                   |NULL                      |NULL                       |
# |true                  |15                  |NULL                      |NULL                       |
# |false                 |5                   |NULL                      |NULL                       |
# |true                  |22                  |NULL                      |NULL                       |
# |false                 |5                   |NULL                      |NULL                       |
# |false                 |9                   |NULL                      |NULL                       |
# |true                  |14                  |NULL                      |NULL                       |
# |false                 |12                  |NULL                      |NULL                       |
# +----------------------+--------------------+--------------------------+---------------------------+
# only showing top 25 rows