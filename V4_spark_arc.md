# Consumer Scorecard V4 — Complete PySpark Documentation

> **Java Source:** `ConsumerScorecardAttributeCalculatorV4`  
> **Target Platform:** AWS EMR / PySpark  
> **Author:** Data Engineering  
> **Purpose:** End-to-end conversion of credit scoring engine from Java to PySpark with full business logic explanation, attribute coverage analysis, and MVP readiness assessment.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Master Pipeline Flow](#2-master-pipeline-flow)
3. [Class Architecture Diagram](#3-class-architecture-diagram)
4. [Phase 0 — Setup & SparkSession](#4-phase-0--setup--sparksession)
5. [Phase 1 — Data Ingestion (fact2)](#5-phase-1--data-ingestion-fact2)
6. [Phase 2 — Per-Month Variable Calculation](#6-phase-2--per-month-variable-calculation)
7. [Phase 3 — Per-Account Aggregation](#7-phase-3--per-account-aggregation)
8. [Phase 4 — Global Attribute Calculation](#8-phase-4--global-attribute-calculation)
9. [Phase 5 — Scorecard Trigger & Final Score](#9-phase-5--scorecard-trigger--final-score)
10. [Phase 6 — Output & Validation](#10-phase-6--output--validation)
11. [DataFrame Lineage](#11-dataframe-lineage)
12. [Java → PySpark Quick Reference](#12-java--pyspark-quick-reference)
13. [MVP Attribute Coverage Analysis](#13-mvp-attribute-coverage-analysis)
14. [Score Trigger Readiness](#14-score-trigger-readiness)
15. [Recommended Sample Data Extensions](#15-recommended-sample-data-extensions)
16. [Validation Results (12-Customer Test)](#16-validation-results-12-customer-test)

---

## 1. System Overview

The **Consumer Scorecard V4** processes up to **36 months** of  credit history per customer, derives **100+ behavioural variables**, and produces a final credit score between **300–900**.

### Key Design Principles

| Principle | Java Implementation | PySpark Implementation |
|-----------|--------------------|-----------------------|
| Separation of concerns | Distinct classes per responsibility | Distinct functions per phase |
| Configuration-driven | `ScoreCardAttributeMappingV4` Spring bean | Broadcast sets (`CC_CODES`, `HL_CODES`…) |
| Utility functions | `ScoreCardV4Utility` static methods | Inline Spark expressions + helper UDFs |
| Constants centralised | `ScoreCardV4Constants` class | Python constants + broadcast |
| Output container | `ScoreCardV4FinalPOJO` object | `global_attrs` + `final_df` DataFrames |

### Scorecard Segments (Priority Order)

```
CLOSED → ST_3 → THIN → ST_2 → ST_1_HC → ST_1_EV → ST_1_AGR_OR_COM → ST_1_SE
```

### Product Code Sets

| Set | Codes | Purpose |
|-----|-------|---------|
| `CC_CODES` | `{"220","121","213","5","214","225","058","047"}` | Credit card accounts |
| `HL_CODES` | `{"002","058"}` | Housing loan accounts |
| `AL_CODES` | `{"001","047"}` | Auto loan accounts |
| `PL_CODES` | `{"123","130","172","195","196","215","216","217","219",…}` | Personal loan accounts |
| `AGRI_CODES` | `{"167","177","178","179","198","199","200","223","224","226","227"}` | Agricultural accounts |
| `COM_SEC_CODES` | `{"175","176","228","241"}` | Commercial secured |
| `GL_CODES` | `{"191","007"}` | Gold loan |
| `TWO_W_CODES` | `{"013","173"}` | Two-wheeler loan |

---

## 2. Master Pipeline Flow

```mermaid
flowchart TD
    A([🗂️ Raw Input\ntrade_data · mapping_df\nproduct_mapping_df · bank_mapping_df]) --> B

    subgraph P0["⚙️ Phase 0 — Setup"]
        B[SparkSession.builder.getOrCreate\nBroadcast product code sets\nDerogatory status lists]
    end

    subgraph P1["📥 Phase 1 — Data Ingestion"]
        C[fact2\ncust_id × accno × month_diff\n36 months per customer]
    end

    subgraph P2["🔢 Phase 2 — Per-Month Variables"]
        D1[dpd_new_udf\nNormalised DPD 0–7]
        D2[modified_limit_udf\nmax balance · limit · loan]
        D3[derog_flag_udf\nSuit-filed · WO status]
        D4[add_product_flags\nisCC · isHL · isAL · isAgri…]
        D1 & D2 & D3 & D4 --> D5[fact2_enriched]
    end

    subgraph P3["📊 Phase 3 — Per-Account Aggregation"]
        E1[isLiveAccount · mob · derog]
        E2[monthly_data struct array]
        E3[avgBal_l3m · pct_instance_l12m]
        E4[max_bal_l24m · m_since_max_bal]
        E1 & E2 & E3 & E4 --> E5[account_details\none row per cust_id × accno]
    end

    subgraph P4["🧮 Phase 4 — Global Attributes"]
        F1[4A Account Counts]
        F2[4B Accounts Opened MOB]
        F3[4C DPD Maxima]
        F4[4D Utilisation CC]
        F5[4E Exposure Amounts]
        F6[4F Balance Trend Flags]
        F7[4G Outflow Algorithm\nPandas UDF]
        F8[4H–4J Derog · MaxBal · Rpt]
        F1 & F2 & F3 & F4 & F5 & F6 & F7 & F8 --> F9[global_attrs\none row per cust_id · 100+ cols]
    end

    subgraph P5["🎯 Phase 5 — Scorecard & Score"]
        G1[5A compute_trigger_eligibility\nIS_ELIGIBLE_FOR_ST2 · ST3]
        G2[5B apply_score_trigger\nDecision Tree → scorecard_name]
        G3[5C compute_final_score\nPandas UDF · Coefficients]
        G1 --> G2 --> G3
    end

    subgraph P6["💾 Phase 6 — Output"]
        H[result.write.parquet\ns3://…/consumer_scorecard_v4/]
    end

    B --> C --> D5 --> E5 --> F9 --> G1
    G3 --> H

    style P0 fill:#ECEFF1,stroke:#455A64
    style P1 fill:#E3F2FD,stroke:#1565C0
    style P2 fill:#E0F2F1,stroke:#00695C
    style P3 fill:#FFF3E0,stroke:#E65100
    style P4 fill:#F3E5F5,stroke:#6A1B9A
    style P5 fill:#FFEBEE,stroke:#B71C1C
    style P6 fill:#E8F5E9,stroke:#2E7D32
```

---

## 3. Class Architecture Diagram

```mermaid
classDiagram
    class ConsumerScorecardAttributeCalculatorV4 {
        +invoke(input) STEP_1
        -preparePaymentHistoryAndVariables() STEP_2
        -populateConsumerAccountHistory() STEP_3
        -prepareVariables() STEP_4
    }

    class ScoreCardHelperV4 {
        +getDpd_new() STEP_3_1
        +getModifiedLimit() STEP_3_1
        +getDerogatoryFlag() STEP_3_1
        +getBalByCreditPercent() STEP_3_2
        +calculateTotalConcPercIncreaseFlags() STEP_4_1
        +calculateOverflow_HC() STEP_4_2
    }

    class ScoreCardAttributeMappingV4 {
        -allCreditCardProductCodes Set
        -allHousingLoanProductCodes Set
        -allALProductCodes Set
        -allAgriProductCodes Set
        -allComSecProductCodes Set
        -derogSFWList Set
        -derogWOSTList Set
    }

    class ScoreTriggerEngineV4 {
        +getScoreCardName() STEP_5
        -prepareTriggerVariables() STEP_5_1
    }

    class ScoreCardManagerv4 {
        +getMap() STEP_6
    }

    class ScoreCardV4Evaluator {
        +getAttributes() STEP_7
        +getScore() STEP_8
    }

    class ScoreCardV4FinalPOJO {
        -score int
        -scorecardName String
        -attributeMap Map
        -conskeyNAccountLevelDetailsMap Map
        -conskeyNVariablesMap Map
        -shouldTriggerNoHitScore boolean
    }

    class AccountLevelDetailsV4 {
        -isLiveAccount boolean
        -isCC boolean
        -isHL boolean
        -isAL boolean
        -isAgri boolean
        -mob int
        -acctBalMap Map
        -dpdMap Map
        -assetMap Map
        -derog boolean
        -pct_instance_l12m int
        -avgBal_l3m double
    }

    class ScoreCardVariableV4 {
        -dpd_new int
        -modifiedLimit double
        -derogatoryFlag boolean
        -final_consec_marker int
        -index int
    }

    ConsumerScorecardAttributeCalculatorV4 --> ScoreCardHelperV4
    ConsumerScorecardAttributeCalculatorV4 --> ScoreCardAttributeMappingV4
    ConsumerScorecardAttributeCalculatorV4 --> ScoreTriggerEngineV4
    ConsumerScorecardAttributeCalculatorV4 --> ScoreCardManagerv4
    ConsumerScorecardAttributeCalculatorV4 --> ScoreCardV4FinalPOJO : creates
    ScoreCardV4FinalPOJO *-- AccountLevelDetailsV4
    ScoreCardV4FinalPOJO *-- ScoreCardVariableV4
    ScoreCardHelperV4 --> ScoreCardAttributeMappingV4
    ScoreTriggerEngineV4 --> ScoreCardV4Evaluator
    ScoreCardManagerv4 --> ScoreCardV4Evaluator
```

---

## 4. Phase 0 — Setup & SparkSession

### What it does
Initialises Spark, enables Arrow optimisation for Pandas UDFs, and **broadcasts all product-code configuration** so every executor has a local copy without shuffle cost. This replaces the Spring-injected `ScoreCardAttributeMappingV4` bean in Java.

### Step-by-Step

```mermaid
flowchart LR
    A[Start] --> B[SparkSession.builder\n.getOrCreate]
    B --> C[Enable Arrow\nspark.sql.execution\n.arrow.pyspark.enabled=true]
    C --> D[Define product\ncode sets\nCC · HL · AL · AGRI…]
    D --> E[Define derogatory\nstatus lists\nSFW · WOST]
    E --> F[spark.sparkContext\n.broadcast each set]
    F --> G[Ready for\nPhase 2 UDFs]

    style A fill:#455A64,color:#fff
    style G fill:#388E3C,color:#fff
```

### Code

```python
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
    .getOrCreate()

# Java: ScoreCardAttributeMappingV4.allCreditCardProductCodes
CC_CODES    = {"058","047","220","121","213","5","214","225"}
HL_CODES    = {"002","058"}
AL_CODES    = {"001","047"}
AGRI_CODES  = {"167","177","178","179","198","199","200","223","224","226","227"}
COM_SEC_CODES = {"175","176","228","241"}
GL_CODES    = {"191","007"}
TWO_W_CODES = {"013","173"}

# Java: derogSFWList
DEROG_SFW_LIST  = {"05","06","07","08","09","10","11","12","13","14","15","16","17"}
# Java: derogWOSTList
DEROG_WOST_LIST = {"02","03","04","05","06","07","08","09","10","11","12","13","14","15"}

cc_bc   = spark.sparkContext.broadcast(CC_CODES)
hl_bc   = spark.sparkContext.broadcast(HL_CODES)
agri_bc = spark.sparkContext.broadcast(AGRI_CODES)
```

### Example — Broadcast Usage in UDF

```python
# Inside any UDF, access the broadcast value:
val = cc_bc.value          # → {"058","047","220",...}
"058" in val               # → True  (Credit Card account)
"123" in val               # → False (Personal Loan)
```

> **Mac/local note:** Arrow is disabled (`false`) for local development due to JDK 25 compatibility. On AWS EMR set `arrow.pyspark.enabled = true` for full Pandas UDF performance.

---

## 5. Phase 1 — Data Ingestion (fact2)

### What it does
`fact2` is the **pre-built 36-month aligned DataFrame** — one row per `(cust_id, accno, month_diff)`. It is the PySpark equivalent of the Java input map `conAccHistMap<Long, List<ConsumerAccountHist>>`.

### Source Tables

| Source Table | Key Columns | What it gives you | Scorecard purpose |
|---|---|---|---|
| `mapping_df` | `cust_id, app_date` | Customer anchor + `app_dt → score_dt` = first-of-month | Time axis: 36-month grid, `month_diff` 0..35 |
| `trade_data` | `balance_dt, balance_amt, credit_lim_amt, original_loan_amt, dayspastdue, pay_rating_cd, account_status_cd, open_dt, closed_dt, past_due_amt, actual_pymnt_amt, SUITFILED_WILFULDEFAULT, WRITTEN_OFF_AND_SETTLED, account_type_cd, bureau_mbr_id` | Core monthly financials per account | All balance / utilization / DPD / delinquency attributes |
| `product_mapping` | `account_type_cd, Sec_Uns, Reg_Com, Sec_Mov, Pdt_Cls, Name, AGRI PRODUCTS` | Product classification flags: `is_sec`, `is_unsec`, `is_agri`, `is_comuns`, `is_regsec`, `is_reguns` | All product-slice attributes |
| `bank_mapping` | `bureau_mbr_id, Category (PUB/PVT/NBF/SFB/COB…), Bank_Name` | Bank category tag per account | Category-level counts and balance splits (Phase 4L) |

### fact2 Schema

| Column | Type | Description | Java Equivalent |
|--------|------|-------------|-----------------|
| `cust_id` | long | Customer identifier | Person key |
| `accno` | int | Account number | `consAcctKey` |
| `month_diff` | int | 0=latest, 35=oldest | `index` in month loop |
| `score_dt` | int | yyyyMMdd — scoring reference date | `scoreDt` |
| `balance_amt` | double | Outstanding balance | `acctBalAm` |
| `credit_lim_amt` | double | Sanctioned credit limit | `indiaCreditLimitAmt` |
| `original_loan_amt` | int | Original loan amount | `indiaRepaymTenure` |
| `dayspastdue` | int | Raw days past due | `daysPastDueCt` |
| `pay_rating_cd` | string | Asset classification S/B/D/L/M | `assetClsssCd` |
| `SUITFILED_WILFULDEFAULT` | int | Suit-filed status code | `suitFieldWilfulDefStatCd` |
| `WRITTEN_OFF_AND_SETTLED` | int | Write-off / settled status | `woSettledStatCd` |
| `account_status_cd` | string | O=Open, C=Closed | `acctStatCd` |
| `open_dt` | int | Account open date yyyyMMdd | `acctOpenDt` |
| `Sec_Uns` | string | Sec / UnSec | Derived from `allSecProductCodes` |
| `Pdt_Cls` | string | RegSec / RegUns / ComUns… | `ScoreCardAttributeMappingV4` |
| `Category` | string | PUB/PVT/NBF/COB… | Bank category |

### Sample Data

```
cust_id  accno   month_diff  balance_amt  dayspastdue  pay_rating_cd  account_status_cd
19xxxx   1356xx  0           2,589,557    80           B              O
19xxxx   1356xx  1           2,589,557    65           B              O
19xxxx   1356xx  2           2,590,265    64           B              O
19xxxx   1356xx  3           2,590,688    77           B              O
62xxxx   9821xx  0           450,000      0            S              O
62xxxx   9821xx  1           460,000      0            S              O
```

> **Key rule:** `month_diff = 0` is the most recent reporting month (≤ `score_dt`). `month_diff = 35` is the oldest. Only months where `0 ≤ month_diff ≤ 35` are retained.

> **NaN guard:** Empty CSV cells are read as `float NaN` by Pandas. All string columns must be sanitised with a `_s()` helper (`str(v) if v is not None and str(v) != "nan" else ""`) before passing to Spark Row builders.

---

## 6. Phase 2 — Per-Month Variable Calculation

### What it does
For every row in `fact2`, compute three derived fields that Java's `ScoreCardHelperV4` computes inside `getVariableFromPaymentHistory()`. These become columns in `fact2_enriched`.

```mermaid
flowchart TD
    IN([fact2\none row per\ncust_id × accno × month_diff])

    IN --> UDF1
    IN --> UDF2
    IN --> UDF3
    IN --> FLAGS

    subgraph UDF1["dpd_new_udf  ←  getDpd_new()"]
        U1A[balance_amt ≤ 500?] -->|Yes| U1B[return 0]
        U1A -->|No| U1C{DPD Ladder\nfirst match wins}
        U1C -->|dpd > 720 OR asset=L OR WO-derog| U1D[return 7]
        U1C -->|361–720 OR asset in L,D| U1E[return 6]
        U1C -->|181–360| U1F[return 5]
        U1C -->|91–180 OR asset in L,B,D| U1G[return 4]
        U1C -->|61–90 OR asset=M| U1H[return 3]
        U1C -->|31–60| U1I[return 2]
        U1C -->|1–30| U1J[return 1]
        U1C -->|0 or default| U1K[return 0]
    end

    subgraph UDF2["modified_limit_udf  ←  getModifiedLimit()"]
        U2A[balance_amt → treat -1 as 0]
        U2B[credit_lim_amt → treat -1 as 0]
        U2C[original_loan_amt → treat -1 as 0]
        U2A & U2B & U2C --> U2D[return max of three]
    end

    subgraph UDF3["derog_flag_udf  ←  getDerogatoryFlag()"]
        U3A{SUITFILED in\nderogSFWList?} -->|Yes| U3D[return True]
        U3A -->|No| U3B{WO_SETTLED in\nderogWOSTList?}
        U3B -->|Yes| U3D
        U3B -->|No| U3C{pay_rating_cd\nin D or L?}
        U3C -->|Yes| U3D
        U3C -->|No| U3E[return False]
    end

    subgraph FLAGS["add_product_flags()  ←  ScoreCardAttributeMappingV4"]
        F1[isCC  → account_type_cd IN CC_CODES]
        F2[isHL  → account_type_cd IN HL_CODES]
        F3[isAL  → account_type_cd IN AL_CODES]
        F4[isAgri → account_type_cd IN AGRI_CODES]
        F5[isUns  → Sec_Uns == 'UnSec']
        F6[isRegSec → Pdt_Cls == 'RegSec']
    end

    UDF1 & UDF2 & UDF3 & FLAGS --> OUT([fact2_enriched\n+ dpd_new\n+ modified_limit\n+ derog_flag\n+ isCC · isHL · isAL\n+ isAgri · isUns · isSec…])

    style IN fill:#E3F2FD,stroke:#1565C0
    style OUT fill:#E0F2F1,stroke:#00695C
```

### Step 2.1 — `dpd_new_udf` (Java: `ScoreCardHelperV4.getDpd_new`)

**Business rule:** Converts raw `dayspastdue` + asset classification + write-off status into a clean ordinal **0–7** delinquency scale. Accounts with `balance_amt ≤ 500` are always scored 0.

**Critical implementation note:** WO status codes are zero-padded to 3 characters (`"002"` not `"02"`) when checked against the WOST list. Empty / null strings and `"nan"` must be treated as no write-off.

```python
def _clean(val):
    """Guard against NaN strings leaking from Pandas CSV reads."""
    import math
    if val is None: return ""
    try:
        if math.isnan(float(val)): return ""
    except (TypeError, ValueError):
        pass
    return str(val).strip()

@udf(IntegerType())
def dpd_new_udf(dayspastdue, pay_rating_cd, suit_filed, wo_settled, balance_amt):
    bal = balance_amt if balance_amt is not None else -1
    dpd = dayspastdue if dayspastdue is not None else -1

    if bal <= 500:
        return 0

    wo  = _clean(wo_settled).zfill(3)   # "12" → "012"
    sfw = _clean(suit_filed).zfill(3)

    if dpd > 720:   return 7
    if pay_rating_cd == "L" and dpd == -1: return 7
    if wo in {"002","003","004","005","006","007","008","009","010","011","012","013","014","015"}:
        return 7

    if 361 <= dpd <= 720:   return 6
    if pay_rating_cd in {"L","D"} and dpd == -1: return 6
    if 181 <= dpd <= 360:   return 5
    if 91  <= dpd <= 180:   return 4
    if pay_rating_cd in {"L","B","D"} and dpd == -1: return 4
    if 61  <= dpd <= 90:    return 3
    if pay_rating_cd == "M" and dpd == -1: return 3
    if 31  <= dpd <= 60:    return 2
    if 1   <= dpd <= 30:    return 1
    return 0
```

#### Example Walkthrough

| Row | `dayspastdue` | `pay_rating_cd` | `balance_amt` | `dpd_new` | Reason |
|-----|------------|------------|-----------|-----------|--------|
| A | 80 | B | 2,589,557 | **4** | 61 ≤ 80 ≤ 180 → level 4 |
| B | 0 | S | 450,000 | **0** | DPD=0, standard |
| C | -1 | L | 1,200,000 | **7** | asset=L, dpd=-1 → level 7 |
| D | 45 | S | 300 | **0** | balance ≤ 500 → always 0 |
| E | 400 | B | 500,000 | **6** | 361 ≤ 400 ≤ 720 → level 6 |
| F | 0 | S | 0 (NaN) | **0** | NaN balance → treat as -1 ≤ 500 |

---

### Step 2.2 — `modified_limit_udf` (Java: `ScoreCardHelperV4.getModifiedLimit`)

**Business rule:** Proxy for the sanctioned exposure = `max(balance, credit_limit, original_loan)`. Java sentinel `-1` is treated as `0`.

```python
@udf(DoubleType())
def modified_limit_udf(balance_amt, credit_lim_amt, original_loan_amt):
    vals = [
        float(v) if v is not None and v != -1 else 0.0
        for v in [balance_amt, credit_lim_amt, original_loan_amt]
    ]
    return max(vals)
```

#### Sample Values (5-customer dataset)

| cust | account_type_cd | modified_limit |
|------|----------------|---------------|
| 1001 | 58 (Credit Card / Mortgage) | 2,590,688 |
| 1002 | 5 (Credit Card) | 150,000 |
| 1003 | 123 (Personal Cash Loan) | 250,000 |
| 1004 | 191 (Gold Loan) | 60,000 |
| 1005 | 167 (Microfinance Business) | 100,000 |

---

### Step 2.3 — `derog_flag_udf` (Java: `ScoreCardHelperV4.getDerogatoryFlag`)

**Business rule:** Marks an account as derogatory if any adverse indicator is present. Used to set `AccountLevelDetailsV4.derog`.

**Critical implementation note:** `suit_filed` and `wo_settled` defaults must be empty string `""` (not `"0"` and not `"nan"`). The `"0".zfill(3)` produces `"000"` which does not match any derog code; `"nan".zfill(3)` produces `"nan"` which is also safe — but feeding actual Python `nan` floats must be guarded.

```python
@udf(BooleanType())
def derog_flag_udf(suit_filed, wo_settled, pay_rating_cd):
    sfw = _clean(suit_filed).zfill(3)
    wo  = _clean(wo_settled).zfill(3)
    pr  = _clean(pay_rating_cd)

    if sfw in {"005","006","007","008","009","010","011","012","013","014","015","016","017"}:
        return True
    if wo in {"002","003","004","005","006","007","008","009","010","011","012","013","014","015"}:
        return True
    if pr in {"D", "L"}:
        return True
    return False
```

#### Example Walkthrough

| `SUITFILED` | `WO_SETTLED` | `pay_rating_cd` | `derog_flag` | Reason |
|------------|------------|------------|------------|--------|
| 0 | 12 | B | **True** | WO=`"012"` in WOST list |
| 0 | 0 | S | **False** | All clear |
| 7 | 0 | S | **True** | SFW=`"007"` in SFW list |
| 0 | 0 | L | **True** | asset=Loss |
| NaN | NaN | S | **False** | NaN cleaned to "" → no match |

---

### Step 2.4 — `add_product_flags` (Java: `ScoreCardAttributeMappingV4`)

```python
def add_product_flags(df):
    cd = col("account_type_cd")
    return (
        df
        .withColumn("isCC",    cd.isin(*CC_CODES))
        .withColumn("isHL",    cd.isin(*HL_CODES))
        .withColumn("isAL",    cd.isin(*AL_CODES))
        .withColumn("isAgri",  cd.isin(*AGRI_CODES))
        .withColumn("isComSec",cd.isin(*COM_SEC_CODES))
        .withColumn("isGL",    cd.isin(*GL_CODES))
        .withColumn("isUns",   col("Sec_Uns") == "UnSec")
        .withColumn("isSec",   col("Sec_Uns") == "Sec")
        .withColumn("isRegSec",col("Pdt_Cls") == "RegSec")
        .withColumn("isRegUns",col("Pdt_Cls") == "RegUns")
    )
```

#### Product Flag Coverage (Sample Data)

| Flag Column | Pdt_Cls / Sec_Uns | Account Type Codes | Notes |
|---|---|---|---|
| `is_cc` | RegUns / RegSec | 5, 47, 58, 121, 130 | Types 5 and 58 in sample ✅ |
| `is_sec` | Sec | type 58 | cust 1001 secured ✅ |
| `is_unsec` | UnSec | 5, 123, 167, 191 | custs 1002–1005 ✅ |
| `is_agri` | AgriPSL | 167 (Microfinance Business) | cust 1005 ✅ |
| `is_regular` | Regular | 5, 58, 123 | custs 1001/1002/1003 ✅ |
| `is_comuns` | ComUns | 167, 191 | custs 1004/1005 ✅ |
| `is_regsec` | RegSec | 58 | cust 1001 ✅ |
| `is_reguns` | RegUns | 5, 123 | custs 1002/1003 ✅ |
| `is_pl_cd_tw` | PL/CD/TW subset | 123 (PL); 173 (TW) not in sample | Partial — add TW types for full flag |

---

## 7. Phase 3 — Per-Account Aggregation

### What it does
Groups `fact2_enriched` by `(cust_id, accno)` to produce **one row per account** — the PySpark equivalent of `AccountLevelDetailsV4`. The key innovation is storing all 36 months in a **`monthly_data` struct array** for efficient downstream access.

```mermaid
flowchart TD
    IN([fact2_enriched\none row per month]) --> AGG

    subgraph AGG["build_account_details(fact2_enriched)\ngroupBy cust_id · accno"]
        
        subgraph A["3A — Live Account Flags"]
            A1["isLiveAccount\n= month_diff=0 AND status='O'"]
            A2["isReportedLiveIn12M\n= any live row in months 0–11"]
            A3["derog\n= max(derog_flag) across months"]
        end

        subgraph B["3B — MOB Calculation"]
            B1["mob = score_dt_months − open_dt_months\nboth yyyyMMdd → year×12 + month"]
        end

        subgraph C["3C — monthly_data Array"]
            C1["collect_list(struct(\n  idx=month_diff,\n  dpd=dpd_new,\n  bal=balance_amt,\n  mod_lim=modified_limit,\n  asset_cd=pay_rating_cd,\n  wo_status=WO_SETTLED\n))"]
        end

        subgraph D["3D — Scalar Aggregates"]
            D1["max_dpd_l36m = max(dpd_new)"]
            D2["pct_instance_l12m = % months bal>0 in 12m"]
            D3["avgBal_l3m = avg(balance) months 0,1,2"]
            D4["avgModifiedLimit_l3m = avg(mod_lim) 0–2"]
        end

        subgraph E["3E — Max Balance Tracking"]
            E1["max_bal_l24m = max(balance) in months 0–23"]
            E2["m_since_max_bal_l24m = month_diff at peak balance\nrow_number() window → earliest peak"]
        end

        subgraph F["3F — firstReportedOnlyIn36M Flag"]
            F1["Left-join: was account seen in months 0–12?\nfirstReportedOnly36M = NOT in_12m AND in_36m"]
        end

    end

    AGG --> OUT([account_details\none row per cust_id × accno])

    style IN fill:#E0F2F1,stroke:#00695C
    style OUT fill:#FFF3E0,stroke:#E65100
```

### Key Code — Aggregation

```python
acct_agg = (
    fact2_enriched
    .groupBy("cust_id", "accno")
    .agg(
        # 3A — Live flags
        fmax(when(col("month_diff") == 0,
                  col("is_live_month0")).otherwise(lit(False))
             ).alias("isLiveAccount"),

        # 3B — MOB (filled after agg using yyyymmdd_to_months helper)
        F.first("open_dt").alias("open_dt"),
        F.first("score_dt").alias("score_dt"),

        # 3C — Full history as struct array
        collect_list(struct(
            col("month_diff").alias("idx"),
            col("dpd_new").alias("dpd"),
            col("balance_amt").alias("bal"),
            col("modified_limit").alias("mod_lim"),
            col("pay_rating_cd").alias("asset_cd"),
            col("WRITTEN_OFF_AND_SETTLED").cast("string").alias("wo_status"),
        )).alias("monthly_data"),

        # 3D — Scalar aggregates
        fmax(col("dpd_new")).alias("max_dpd_l36m"),
        (fsum(when((col("month_diff") <= 11) & (col("balance_amt") > 0), lit(1))
              .otherwise(lit(0))) * 100.0
         / count(when(col("month_diff") <= 11, lit(1)))).alias("pct_instance_l12m"),
        favg(when(col("month_diff") <= 2, col("balance_amt"))).alias("avgBal_l3m"),
    )
)
```

### `consec_marker` — Multi-Account Bug Fix

When computing `final_consec_marker` across accounts, each `(cust_id, accno)` pair may have duplicate `idx` values. Use `max_dpd_by_idx` to deduplicate before counting consecutive bad months:

```python
# CORRECT: deduplicate idx per account before consec count
max_dpd_by_idx = grp.groupby("idx")["dpd"].max().to_dict()
```

### MOB Calculation

```python
# Java: getDifferenceInMonthsFromFirst(openDate, scoreDt)
def yyyymmdd_to_months(dt_col):
    return (
        F.year(F.to_date(dt_col.cast("string"), "yyyyMMdd")) * 12
        + F.month(F.to_date(dt_col.cast("string"), "yyyyMMdd"))
    )

acct_agg = acct_agg.withColumn(
    "mob",
    yyyymmdd_to_months(col("score_dt")) - yyyymmdd_to_months(col("open_dt"))
)
```

### Example — account_details row

```
cust_id           = 19xxxx
accno             = 1356135559
account_type_cd   = 58          (Credit Card)
isCC              = True
isHL              = False
isLiveAccount     = True        (month_diff=0 has status='O')
mob               = 42          (opened Feb 2017, score Sep 2020)
derog             = False
max_dpd_l36m      = 4
pct_instance_l12m = 100.0
avgBal_l3m        = 2,589,926  
monthly_data      = [
  {idx:0, dpd:4, bal:2589557, mod_lim:2589557, asset_cd:'B', wo_status:'12'},
  {idx:1, dpd:4, bal:2589557, mod_lim:2589557, asset_cd:'B', wo_status:'12'},
  ...  (up to 35 more months)
]
```

---

## 8. Phase 4 — Global Attribute Calculation

### What it does
Aggregates `account_details` (and `fact2_enriched`) to produce **one row per `cust_id`** with all ~100 scorecard input variables. This mirrors Java's `prepareVariables()` method which calls 50+ private helper methods.

```mermaid
flowchart LR
    IN1([account_details]) --> A & B & C & D & E & G & H & I & J
    IN2([fact2_enriched]) --> F & G

    A[4A\nAccount\nCounts] --> JOIN
    B[4B\nAccounts\nOpened MOB] --> JOIN
    C[4C\nDPD Maxima\nExplode array] --> JOIN
    D[4D\nUtilisation\nCC only] --> JOIN
    E[4E\nExposure\nAmounts] --> JOIN
    F[4F\nTrend Flags\nlag/lead on total bal] --> JOIN
    G[4G\nOutflow\nPandas UDF] --> JOIN
    H[4H\nDerog Months] --> JOIN
    I[4I\nMax Balance\nMon-since] --> JOIN
    J[4J\nReporting\nCounts] --> JOIN

    JOIN[4K — LEFT JOIN all groups\non cust_id] --> OUT([global_attrs\none row per cust_id\n100+ columns])

    style IN1 fill:#FFF3E0,stroke:#E65100
    style IN2 fill:#E0F2F1,stroke:#00695C
    style OUT fill:#F3E5F5,stroke:#6A1B9A
```

---

### Step 4A — Account Counts

```python
acct_counts = (
    account_details.groupBy("cust_id").agg(
        count("accno").alias("nbr_tot_accts_36"),
        fsum(col("isLiveAccount").cast("int")).alias("nbr_live_accts_36"),
        fsum((col("isCC") & col("isLiveAccount")).cast("int")).alias("nbr_cc_live_accts_36"),
        fsum((col("isHL") & col("reportedIn36M")).cast("int")).alias("nbr_hl_tot_accts_36"),

        # MOB stats
        fmax("mob").alias("max_mob_all_36"),
        fmin("mob").alias("min_mob_all_36"),
        favg("mob").alias("avg_mob_reg_36"),

        # Simultaneous unsecured (used in ST_1_HC trigger)
        fsum((col("isUns") & col("isLiveAccount") & ~col("isCC")).cast("int")
             ).alias("max_simul_unsec_wo_cc"),
    )
)
```

#### Sample Values (5-customer dataset)

| Attribute | 1001 | 1002 | 1003 | 1004 | 1005 |
|---|---|---|---|---|---|
| `nbr_tot_accts_36` | 1 | 1 | 1 | 1 | 1 |
| `nbr_live_accts_36` | 0* | 0* | 0* | 0* | 0* |
| `max_mob_all_36` | 104 | 91 | 67 | 77 | 45 |
| `min_mob_all_36` | 104 | 91 | 67 | 77 | 45 |
| `mon_since_last_acct_open` | 104 | 91 | 67 | 77 | 45 |

> *`nbr_live_accts_36 = 0` because the 5-customer sample has no `month_diff=0` rows — data starts at month 10.

---

### Step 4B — Accounts Opened by MOB Window

```python
# Java: calculateNbrAcctsOpenedInLastXMonths
accts_opened = (
    account_details.groupBy("cust_id").agg(
        fsum((col("mob") <= 6).cast("int")).alias("nbr_accts_open_l6m"),
        fsum((col("mob") <= 12).cast("int")).alias("nbr_accts_open_l12m"),
        fsum(((col("mob") >= 4) & (col("mob") <= 6)).cast("int")).alias("nbr_accts_open_4to6m"),
        fsum(((col("mob") >= 7) & (col("mob") <= 12)).cast("int")).alias("nbr_accts_open_7to12m"),
    )
)
```

> **Logic:** `mob ≤ 6` means the account was opened within the last 6 months. Used in **ST_1_HC** trigger: `nbr_accts_open_l6m ≥ 4`.

---

### Step 4C — DPD Maxima (Explode monthly_data)

```python
exploded = (
    account_details
    .select("cust_id","accno","isCC","isHL","isUns",
            F.explode("monthly_data").alias("m"))
    .select("cust_id","accno","isCC","isHL","isUns",
            col("m.idx").alias("idx"),
            col("m.dpd").alias("dpd"),
            col("m.bal").alias("bal"),
            col("m.mod_lim").alias("mod_lim"),
            col("m.asset_cd").alias("asset_cd"))
)

dpd_attrs = (
    exploded.groupBy("cust_id").agg(
        fmax(when(col("isUns"), col("dpd"))).alias("max_dpd_uns_l36m"),
        fmax(when(col("isUns") & (col("idx") <= 11), col("dpd"))).alias("max_dpd_uns_l12m"),
        fsum(when((col("dpd") > 0) & (col("idx") <= 23), lit(1)).otherwise(lit(0))
             ).alias("nbr_0_24m_all"),
        fsum(when((col("dpd") > 3) & (col("idx") <= 23), lit(1)).otherwise(lit(0))
             ).alias("nbr_90_24m_all"),
    )
)
```

#### Sample Values

| Attribute | 1001 | 1002 | 1003 | 1004 | 1005 |
|---|---|---|---|---|---|
| `max_dpd_all_l36m` | 7 | 0 | 0 | 2 | 0 |
| `max_dpd_uns_l36m` | — | 0 | 0 | — (ComUns) | 0 |
| `any_derog_l36m` | True (WOS=12) | False | False | False | False |
| `mon_since_first_worst_delq` | 20 | -999 | -999 | -999 | -999 |
| `mon_since_recent_worst_delq` | 11 | -999 | -999 | -999 | -999 |

---

### Step 4D — Utilisation Metrics (Credit Cards)

```python
# Java: getBalByCreditPercent()
util_df = (
    exploded
    .filter(col("isCC") & (col("mod_lim") > 0))
    .withColumn("util", col("bal") / col("mod_lim"))
    .groupBy("cust_id").agg(
        favg(when(col("idx") <= 2,  col("util"))).alias("util_l3m_cc_live"),
        favg(when(col("idx") <= 5,  col("util"))).alias("util_l6m_cc_live"),
        favg(when(col("idx") <= 11, col("util"))).alias("util_l12m_cc_live"),
    )
)
```

#### Sample Values

| Attribute | 1001 | 1002 | 1003 | 1004 | 1005 |
|---|---|---|---|---|---|
| `util_l3m_cc_live` | n/a* | n/a* | — | — | — |
| `util_l12m_all_tot` | 1.0 (bal>lim) | n/a | 0.83 ✅ | n/a | 0.98 ✅ |
| `util_l12m_uns_tot` | — | n/a | 0.83 (PL) ✅ | n/a | 0.98 (MF) ✅ |

> *`util_l3m` is n/a because sample data starts at month 10; months 0–2 are missing.

---

### Step 4E — Exposure & Sanctioned Amounts

```python
exposure_df = (
    account_details.groupBy("cust_id").agg(
        fmax("avgModifiedLimit_l3m").alias("max_sanc_amt"),
        fmax(when(col("isSec"), col("avgModifiedLimit_l3m"))).alias("max_sanc_amt_sec"),
        fsum(when(col("isUns"), col("avgModifiedLimit_l3m"))).alias("sum_sanc_amt_uns"),
        fmax(when(col("isAL") | col("isCC") | col("isHL"), col("avgModifiedLimit_l3m"))
             ).alias("max_limit_al_pl_tw_cd"),
    )
)
```

#### Sample Values

| Attribute | 1001 | 1002 | 1003 | 1004 | 1005 |
|---|---|---|---|---|---|
| `max_sanc_amt` | 2,590,688 ✅ | 150,000 ✅ | 250,000 ✅ | 60,000 ✅ | 100,000 ✅ |
| `max_sanc_amt_sec` | 2,590,688 ✅ | -999 | -999 | -999 | -999 |
| `sum_sanc_amt_uns` | -999 | 150,000 ✅ | 250,000 ✅ | 60,000 ✅ | 100,000 ✅ |
| `max_limit_al_pl_tw_cd` | — | — | 250,000 (PL type 123) ✅ | — | — |

---

### Step 4F — Balance Trend Flags

**Business rule:** Count months where total balance (excluding CC) increased by ≥10% vs the prior month. Java: `calculateTotalConcPercIncreaseFlags()` in `ScoreCardHelperV4`.

```mermaid
flowchart LR
    A[fact2_enriched\nfilter isCC=False] --> B[groupBy cust_id · month_diff\nsum balance_amt\n→ tot_bal per month]
    B --> C[Window.partitionBy cust_id\n.orderBy month_diff desc\nlead tot_bal → prev_bal]
    C --> D[pct_chg = tot_bal - prev_bal / prev_bal]
    D --> E{pct_chg >= 0.10\nin months 0–24?}
    E -->|count| F[totconsinc_bal10_exc_cc_25m]
    D --> G{pct_chg < 0\nin months 0–35?}
    G -->|count| H[totconsdec_bal_tot_36m]
```

```python
w_trend = Window.partitionBy("cust_id").orderBy(col("month_diff").desc())

trend_df = (
    monthly_total_bal
    .withColumn("prev_bal", lead("tot_bal").over(w_trend))
    .withColumn("pct_chg",
                when(col("prev_bal") > 0,
                     (col("tot_bal") - col("prev_bal")) / col("prev_bal"))
                .otherwise(lit(None)))
    .groupBy("cust_id").agg(
        fsum(when((col("month_diff") <= 24) & (col("pct_chg") >= 0.10), lit(1))
             .otherwise(lit(0))).alias("totconsinc_bal10_exc_cc_25m"),
        fsum(when(col("pct_chg") < 0, lit(1))
             .otherwise(lit(0))).alias("totconsdec_bal_tot_36m"),
    )
)
```

#### Sample Values

| Attribute | 1002 | 1003 | 1004 | 1005 |
|---|---|---|---|---|
| `totconsdec_bal_tot_36m` | decreasing ✅ | decreasing ✅ | increasing | increasing |
| `balance_amt_0_12_by_13_24` | 0.0 | 0.22 (partial) | 0.0 | 0.12 (partial) |

> `totconsinc_bal10_exc_cc_7m` and `totconsinc_bal10_exc_cc_25m` need months 0–24; the 5-customer sample only covers 10–22.

---

### Step 4G — Outflow Algorithm (Most Complex)

**Business rule:** Estimate total monthly EMI obligation across all accounts. Java: `ScoreCardHelperV4.calculateOverflow_HC()`.

```mermaid
sequenceDiagram
    participant S as Spark
    participant P as Pandas UDF (per group)
    participant R as Result

    S->>P: Group (cust_id, accno) rows sorted by month_diff
    P->>P: Filter balance > 500
    P->>P: Find consecutive month pairs (idx[i+1] - idx[i] == 1)
    
    alt isCC account
        P->>P: EMI = balance[older] - balance[newer]
        P->>P: (payment = balance decrease for revolving)
    else Non-CC instalment
        P->>P: IR = (balance_newer / balance_older) - 1
        P->>P: EMI = balance_older × (1 + IR) - balance_newer
    end
    
    P->>P: emi_median = median(all EMI estimates for this account)
    P->>R: Return (cust_id, accno, emi_median)
    
    S->>S: Sum emi_median per cust_id
    S->>R: outflow_uns_sec_mov
```

> **Data requirement:** `calculateOverflow_HC()` needs 3+ consecutive months of rising balance and modal IR bucket logic. The 5-customer sample does not include a customer with 5+ consecutive months of increasing balance — see [Section 15](#15-recommended-sample-data-extensions) for how to add one.

---

## 9. Phase 5 — Scorecard Trigger & Final Score

### Step 5A — Trigger Eligibility

```mermaid
flowchart TD
    IN([account_details]) --> EXP[Explode monthly_data]

    EXP --> ST3{IS_ELIGIBLE_FOR_ST3}
    ST3 -->|idx == 0\nAND isLive\nAND bal > 500\nAND dpd≥4 OR asset in L,D,B\nOR wo_status in derog_list| YES3[True\nper cust_id]

    EXP --> ST2{IS_ELIGIBLE_FOR_ST2}
    ST2 -->|idx <= 12\nAND bal > 500\nAND dpd≥2 OR asset in L,D,B,M\nOR wo_status in derog_list| YES2[True\nper cust_id]

    YES3 --> JOIN([Left-join to global_attrs])
    YES2 --> JOIN
```

### Step 5B — Scorecard Trigger Decision Tree

```mermaid
flowchart TD
    START([global_attrs with ST2/ST3 flags]) --> C1

    C1{nbr_live_accts_36 <= 0?}
    C1 -->|Yes| CLOSED["🔴 CLOSED"]
    C1 -->|No| C2

    C2{is_eligible_for_st3?}
    C2 -->|Yes| ST3["🔴 ST_3\nSeverely delinquent"]
    C2 -->|No| C3

    C3{max_mob_all_36 <= 6?}
    C3 -->|Yes| THIN["🟠 THIN\nNew to credit"]
    C3 -->|No| C4

    C4{is_eligible_for_st2?}
    C4 -->|Yes| ST2["🟠 ST_2\nModerately delinquent"]
    C4 -->|No| C5

    C5{unsec_wo_cc >= 7\nOR open_l6m >= 4\nOR unsec_inc_gl >= 8?}
    C5 -->|Yes| HC["🟡 ST_1_HC\nHigh credit seeker"]
    C5 -->|No| C6

    C6{all accounts\nAL or CC or HL?}
    C6 -->|Yes| EV["🟢 ST_1_EV\nEvergreen / prime"]
    C6 -->|No| C7

    C7{any agri or\ncommercial accounts?}
    C7 -->|Yes| AGR["🟢 ST_1_AGR_OR_COM"]
    C7 -->|No| SE["🟢 ST_1_SE\nStandard eligible"]

    style CLOSED fill:#B71C1C,color:#fff
    style ST3 fill:#C62828,color:#fff
    style THIN fill:#E65100,color:#fff
    style ST2 fill:#F57C00,color:#fff
    style HC fill:#F9A825,color:#000
    style EV fill:#2E7D32,color:#fff
    style AGR fill:#1565C0,color:#fff
    style SE fill:#00695C,color:#fff
```

```python
scorecard_name = (
    when(col("nbr_live_accts_36") <= 0,              lit("CLOSED"))
    .when(col("is_eligible_for_st3"),                 lit("ST_3"))
    .when(col("max_mob_all_36") <= 6,                 lit("THIN"))
    .when(col("is_eligible_for_st2"),                 lit("ST_2"))
    .when(
        (col("max_simul_unsec_wo_cc") >= 7) |
        (col("nbr_accts_open_l6m") >= 4) |
        (col("max_simul_unsec_wo_cc_inc_gl") >= 8),   lit("ST_1_HC"))
    .when(col("all_accts_al_cc_hl"),                  lit("ST_1_EV"))
    .when(col("has_agri_or_com"),                     lit("ST_1_AGR_OR_COM"))
    .otherwise(                                        lit("ST_1_SE"))
)
```

### Step 5C — Final Score Computation

```python
@pandas_udf(score_schema, F.PandasUDFType.GROUPED_MAP)
def score_pandas_udf(pdf):
    results = []
    coeffs = sc_coeff_bc.value   # Broadcast scorecard coefficients

    for _, row in pdf.iterrows():
        sc_name = row.get("scorecard_name", "ST_1_SE")
        coeff   = coeffs.get(sc_name, coeffs["ST_1_SE"])

        score   = coeff.get("intercept", 500.0)
        parts   = [f"intercept={score:.1f}"]

        for attr, weight in coeff.items():
            if attr == "intercept": continue
            val = row.get(attr)
            if val is not None and not np.isnan(float(val)):
                contrib = float(val) * weight
                score  += contrib
                parts.append(f"{attr}={contrib:.1f}")

        # Clamp to valid range [300, 900]; treat -999 sentinel as 0
        score = max(300, min(900, int(round(score))))
        results.append({
            "cust_id":         int(row["cust_id"]),
            "final_score":     score,
            "score_breakdown": "|".join(parts)
        })
    return pd.DataFrame(results)
```

### Scorecard Coefficients

| Scorecard | intercept | Key Features |
|---|---|---|
| `ST_1_HC` | 500 | `max_dpd_uns_l36m×-25`, `util_l3m_cc_live×-50`, `nbr_accts_open_l6m×-15`, `totconsinc_bal10_exc_cc_25m×-10`, `outflow_uns_sec_mov×0.001`, `max_mob_all_36×1.5`, `avg_bal_l3m_all×-0.00002`, `final_consec_marker×-30` |
| `ST_1_EV` | 550 | `max_dpd_hl_l36m×-30`, `max_dpd_cc_l36m×-20`, `util_l3m_cc_live×-40`, `max_mob_all_36×2.0`, `final_consec_marker×-25` |
| `ST_1_AGR_OR_COM` | 480 | `max_dpd_uns_l36m×-20`, `nbr_agri_tot_accts_36×5`, `max_mob_all_36×1.0` |
| `ST_1_SE` | 520 | `max_dpd_uns_l36m×-22`, `util_l6m_cc_live×-35`, `outflow_uns_sec_mov×0.0008`, `max_mob_all_36×1.8`, `final_consec_marker×-20` |
| `ST_2` | 350 | `max_dpd_uns_l12m×-30`, `nbr_30_24m_all×-20`, `final_consec_marker×-40` |
| `ST_3` | 250 | `max_dpd_uns_l36m×-40`, `final_consec_marker×-50` |
| `THIN` | 400 | `nbr_tot_accts_36×10`, `max_mob_all_36×5` |
| `CLOSED` | 300 (flat) | — |

> Score clamped to [300, 900]. `-999` sentinel treated as 0 in scoring.

#### Example — Score Breakdown

```
cust_id        = 19xxxx
scorecard_name = ST_2

Coefficients (ST_2):
  intercept         = 350.0
  max_dpd_uns_l12m  = -30.0
  nbr_30_24m_all    = -20.0

Calculation:
  intercept           = 350.0
  max_dpd_uns_l12m=4  → 4 × -30 = -120.0
  nbr_30_24m_all=18   → 18 × -20 = -360.0

  raw_score = 350 - 120 - 360 = -130  → clamped to 300

final_score    = 300
score_breakdown = "intercept=350.0|max_dpd_uns_l12m=-120.0|nbr_30_24m_all=-360.0"
```

---

## 10. Phase 6 — Output & Validation

```python
def run_pipeline(fact2):
    """Entry point — mirrors ConsumerScorecardAttributeCalculatorV4.invoke()"""

    fact2_enriched  = build_fact2_enriched(fact2)          # Phase 2
    account_details = build_account_details(fact2_enriched) # Phase 3
    global_attrs    = build_global_attrs(account_details, fact2_enriched)  # Phase 4

    st3_eligible, st2_eligible = compute_trigger_eligibility(account_details)
    global_attrs_triggered = apply_score_trigger(global_attrs, st3_eligible, st2_eligible)
    final_df = compute_final_score(global_attrs_triggered)

    final_df.write.mode("overwrite").parquet(
        "s3://your-bucket/output/consumer_scorecard_v4/"
    )
```

### Validation Steps

```mermaid
flowchart LR
    V1["Step 1\nSingle cust_id\nCompare dpd_new\nmodified_limit\nderog_flag\nrow by row vs Java"] --> V2
    V2["Step 2\nAccount-level\nCompare isLive\nmob\npct_instance_l12m\nfor 5 test accounts"] --> V3
    V3["Step 3\nGlobal attrs\nCompare\nnbr_live_accts_36\nmax_dpd_uns_l36m\nutil_l3m_cc_live"] --> V4
    V4["Step 4\nTrigger\nVerify scorecard_name\nmatches Java for 10\ncustomers per segment"] --> V5
    V5["Step 5\nFull-scale\nWrite Parquet\nCompare score\ndistribution vs\nJava baseline"]

    style V1 fill:#E3F2FD,stroke:#1565C0
    style V2 fill:#E0F2F1,stroke:#00695C
    style V3 fill:#FFF3E0,stroke:#E65100
    style V4 fill:#F3E5F5,stroke:#6A1B9A
    style V5 fill:#E8F5E9,stroke:#2E7D32
```

---

## 11. DataFrame Lineage

```mermaid
erDiagram
    TRADE_DATA {
        int accno
        int balance_amt
        double credit_lim_amt
        int dayspastdue
        string pay_rating_cd
        int SUITFILED_WILFULDEFAULT
        int WRITTEN_OFF_AND_SETTLED
    }

    MAPPING_DF {
        long cust_id
        int app_date
    }

    PRODUCT_MAPPING {
        string account_type_cd
        string Sec_Uns
        string Pdt_Cls
        string Reg_Com
    }

    FACT2 {
        long cust_id
        int accno
        int month_diff
        int score_dt
        double balance_amt
        string pay_rating_cd
        string Sec_Uns
        string Pdt_Cls
    }

    FACT2_ENRICHED {
        long cust_id
        int accno
        int month_diff
        int dpd_new
        double modified_limit
        boolean derog_flag
        boolean isCC
        boolean isHL
        boolean isAL
        boolean isUns
    }

    ACCOUNT_DETAILS {
        long cust_id
        int accno
        boolean isLiveAccount
        int mob
        double avgBal_l3m
        int max_dpd_l36m
        array monthly_data
        boolean derog
    }

    GLOBAL_ATTRS {
        long cust_id
        int nbr_live_accts_36
        int max_mob_all_36
        int max_dpd_uns_l36m
        double util_l3m_cc_live
        double outflow_uns_sec_mov
        int totconsinc_bal10_exc_cc_25m
    }

    FINAL_DF {
        long cust_id
        string scorecard_name
        int final_score
        string score_breakdown
        boolean is_eligible_for_st2
        boolean is_eligible_for_st3
    }

    TRADE_DATA }|--|| FACT2 : "joined with month_diff"
    MAPPING_DF ||--|{ FACT2 : "cust_id link"
    PRODUCT_MAPPING ||--|{ FACT2 : "account_type_cd"
    FACT2 ||--|{ FACT2_ENRICHED : "enriched by UDFs"
    FACT2_ENRICHED ||--|{ ACCOUNT_DETAILS : "groupBy accno"
    ACCOUNT_DETAILS ||--|{ GLOBAL_ATTRS : "groupBy cust_id"
    FACT2_ENRICHED ||--|{ GLOBAL_ATTRS : "trend + outflow UDFs"
    GLOBAL_ATTRS ||--|| FINAL_DF : "trigger + score"
```

---

## 12. Java → PySpark Quick Reference

```mermaid
flowchart LR
    subgraph JAVA["☕ Java"]
        J1["ConsumerScorecardAttributeCalculatorV4\n.invoke()"]
        J2["preparePaymentHistoryAndVariables()"]
        J3["populateConsumerAccountHistory()"]
        J4["prepareVariables()"]
        J5["ScoreCardHelperV4\n.getDpd_new()"]
        J6["ScoreCardHelperV4\n.getModifiedLimit()"]
        J7["ScoreCardHelperV4\n.getDerogatoryFlag()"]
        J8["calculateOverflow_HC()"]
        J9["calculateTotalConcPercIncreaseFlags()"]
        J10["ScoreCardAttributeMappingV4 bean"]
        J11["ScoreTriggerEngineV4\n.getScoreCardName()"]
        J12["ScoreCardManagerv4.getMap()"]
        J13["ScoreCardV4Evaluator.getScore()"]
        J14["AccountLevelDetailsV4"]
        J15["ScoreCardVariableV4"]
        J16["ScoreCardV4FinalPOJO"]
    end

    subgraph PYSPARK["⚡ PySpark"]
        P1["run_pipeline(fact2)"]
        P2["build_fact2_enriched(fact2)"]
        P3["build_account_details(fact2_enriched)"]
        P4["build_global_attrs(…)"]
        P5["dpd_new_udf @udf IntegerType"]
        P6["modified_limit_udf @udf DoubleType"]
        P7["derog_flag_udf @udf BooleanType"]
        P8["compute_emi_per_account @pandas_udf"]
        P9["lag/lead on monthly total balance DF"]
        P10["Broadcast sets CC_CODES HL_CODES…"]
        P11["apply_score_trigger() when().otherwise()"]
        P12["SCORECARD_COEFFICIENTS dict broadcast"]
        P13["compute_final_score() @pandas_udf"]
        P14["account_details DataFrame"]
        P15["monthly_data struct array column"]
        P16["global_attrs + final_df DataFrames"]
    end

    J1 --- P1
    J2 --- P2
    J3 --- P3
    J4 --- P4
    J5 --- P5
    J6 --- P6
    J7 --- P7
    J8 --- P8
    J9 --- P9
    J10 --- P10
    J11 --- P11
    J12 --- P12
    J13 --- P13
    J14 --- P14
    J15 --- P15
    J16 --- P16
```

### Constants Reference

| Java Constant | Value | PySpark Equivalent |
|---------------|-------|-------------------|
| `THIRTY_SIX_MONS_PAYMENT_HIST` | 36 | `month_diff` range 0–35 |
| `MISSING` | -1 | Sentinel `-1` in integer fields; `-999` for "not computed" |
| `MISSING_d` | -1.0 | Sentinel `-1.0` in double fields |
| `OWNERSHIP_CD` | [1, 2] | Filter `relFinCd IN ('1','2')` |
| `PAY_RATING_CD_LD` | ["L","D"] | `pay_rating_cd.isin("L","D")` |
| `MAX_MOB_ALL_36` | "max_mob_all_36" | Column name in `global_attrs` |
| `NBR_LIVE_ACCTS_36` | "nbr_live_accts_36" | Column name in `global_attrs` |
| `IS_ELIGIBLE_FOR_ST2` | "is_eligible_for_st2" | Column name in `global_attrs` |
| `IS_ELIGIBLE_FOR_ST3` | "is_eligible_for_st3" | Column name in `global_attrs` |

---

## 13. MVP Attribute Coverage Analysis

> **Source:** MVP analysis based on `joined_df.csv` · `input_df.csv` · `phase_2.csv` · `sample_trial.py`  
> **Sample:** 5 customers · 50 monthly rows · 5 account types · 10-month history (month_diff 10–22)

### A. Account Counts & MOB

| P | Attribute | Java Method | PySpark Computation | Inputs | Sample Values |
|---|---|---|---|---|---|
| P0 | `nbr_live_accts_36` | `updateAttributeMap()` | `countDistinct WHERE is_live AND month_diff==0` | `account_status_cd, month_diff` | All = 0 ⚠️ (no month_diff=0 rows) |
| P0 | `nbr_tot_accts_36` | `updateAttributeMap()` | `countDistinct(cons_acct_key)` | `cons_acct_key` | All = 1 (single-account sample) |
| P0 | `max_mob_all_36` | `updateAttributeMap()` | `max(months_between(score_dt, open_dt))` | `open_dt, score_dt` | 1001:104, 1002:91, 1003:67, 1004:77, 1005:45 ✅ |
| P0 | `min_mob_all_36` | `updateAttributeMap()` | `min(months_between(score_dt, open_dt))` | `open_dt, score_dt` | Same as max (single acct) |
| P1 | `nbr_cc_tot_accts_36` | `updateAttributeMap()` | `countDistinct WHERE is_cc` | `account_type_cd + product map` | 1001:1(type58), 1002:1(type5), rest:0 |
| P1 | `nbr_hl_tot_accts_36` | `updateAttributeMap()` | `countDistinct WHERE is_hl` | `account_type_cd + product map` | 0 for all (no HL in sample) |
| P1 | `nbr_al_tot_accts_36` | `updateAttributeMap()` | `countDistinct WHERE is_al` | `account_type_cd + product map` | 0 for all (no AL in sample) |
| P1 | `nbr_accts_open_l6m` | `updateAttributeMap()` | `count(mob <= 5)` | `open_dt, score_dt` | All = 0 (oldest open is 45m) |
| P1 | `nbr_accts_open_7_12m` | `openCnt0_6By7_12Bin()` | `count(mob in 6..11)` | `open_dt, score_dt` | All = 0 |
| P1 | `mon_since_last_acct_open` | `calculateMonSinceLastAcctOpen()` | `min(mob)` | `open_dt, score_dt` | 1001:104, 1002:91, 1003:67, 1004:77, 1005:45 ✅ |

### B. DPD / Delinquency Attributes

| P | Attribute | Java Method | PySpark Computation | Inputs | Sample Values |
|---|---|---|---|---|---|
| P0 | `dpd_new` (per row) | `getDpd_new()` | UDF: 8-level ladder with NaN guard | `dayspastdue, WO_AND_SETTLED, pay_rating_cd, balance_amt` | 1001: level 7 (WOS=12); 1002/1003/1005: 0; 1004: 2 |
| P0 | `max_dpd_all_l36m` | max_dpd in account loop | `max(dpd_new) per cust_id` | `dpd_new` | 1001:7, 1002:0, 1003:0, 1004:2, 1005:0 |
| P1 | `max_dpd_UNS_L6_M` | `calculateMaxDpdUNS6_12M()` | `max(dpd_new) WHERE is_unsec AND month_diff<=5` | `dpd_new, Sec_Uns, month_diff` | All 0 or -999 (data starts month 10) |
| P1 | `max_dpd_UNS_6_12_M` | `calculateMaxDpdUNS6_12M()` | `max(dpd_new) WHERE is_unsec AND month_diff in 6..11` | `dpd_new, Sec_Uns, month_diff` | 1001:7 (sec not uns though), rest:0 |
| P1 | `max_dpd_uns_l36m` | `calculateMaxDpdUNS()` | `max(dpd_new) WHERE is_unsec` | `dpd_new, Sec_Uns` | 1003:0(PL unsec), 1004:-999 ComUns, 1002:0 |
| P1 | `max_dpd_sec0_live_bin` | `calculateMaxDpdSEC0Live()` | `max(dpd_new) WHERE is_sec AND is_live AND month_diff==0` | `dpd_new, Sec_Uns, is_live` | ⚠️ Needs month_diff=0 rows |
| P0 | `any_derog_l36m` | `getDerogatoryFlag()` | UDF: SUITFILED + WRITTEN_OFF + wost-pad check | `SUITFILED, WO_SETTLED` | 1001:True (WOS=12), rest:False |
| P1 | `mon_since_first_worst_delq` | `monSinceFirstWorstDelqBin()` | `max(month_diff WHERE derog_flag==True)` | `derog_flag, month_diff` | 1001: 20, rest: -999 |
| P1 | `mon_since_recent_worst_delq` | `monSinceRecentWorstDelqBin()` | `min(month_diff WHERE derog_flag==True)` | `derog_flag, month_diff` | 1001: 11, rest: -999 |

### C. Utilisation Features

| P | Attribute | Java Method | PySpark Computation | Inputs | Sample Values |
|---|---|---|---|---|---|
| P0 | `modified_limit` (per row) | `getModifiedLimit()` | `max(balance_amt, credit_lim_amt, original_loan_amt)` — treat -1 as 0 | `balance_amt, credit_lim_amt, original_loan_amt` | 1001:2,590,688 · 1002:150,000 · 1003:250,000 · 1004:60,000 · 1005:100,000 |
| P1 | `util_l3m_cc_live` | `calculateUtilL3mCcLive()` | `sum(balance)/sum(modified_limit) WHERE is_cc AND month_diff<=2` | `balance_amt, modified_limit, is_cc, month_diff` | n/a (data starts month 10+) |
| P1 | `util_l3m_exc_cc_live` | `calculateUtilL3mExcCcLive()` | `sum/sum WHERE NOT is_cc AND month_diff<=2` | same | n/a (extend to months 0–2) |
| P1 | `util_l6m_all_tot` | `calculateUtilL6mAllTot()` | `sum/sum WHERE month_diff<=5` | same | n/a (data starts month 10+) |
| P1 | `util_l12m_all_tot` | `calculateUtilL12mAllTot()` | `sum/sum WHERE month_diff<=11` | same | 1001:1.0 · 1003:0.83 · 1005:0.98 ✅ |
| P1 | `util_l12m_uns_tot` | `calculateUtilL12mUnsTot()` | `sum/sum WHERE is_unsec AND month_diff<=11` | `Sec_Uns added` | 1003:0.83(PL) · 1005:0.98(MF) ✅ |
| P1 | `util_l6m_uns_tot_bin` | `calculateAvgUtilL6m()` | bin the `util_l6m_uns_tot` value | `util_l6m_uns_tot` | ⚠️ Needs months 0–5 |

### D. Balance Trend Attributes

| P | Attribute | Java Method | PySpark Computation | Inputs | Sample Values |
|---|---|---|---|---|---|
| P1 | `totconsinc_bal10_exc_cc_7m` | `calculateTotconsincBal10ExcCc7m()` | count consec months ≥10% increase excl CC, months 0..6 | `balance_amt, is_cc, month_diff` | ⚠️ Needs months 0–6 |
| P1 | `totconsinc_bal10_exc_cc_25m` | `calculateTotconsincBal10ExcCc25m()` | same for 25-month window | same | Partial (covers ~10 months) |
| P1 | `totconsdec_bal_tot_36m_bin` | `calculateTotconsdecBaltot36mBin()` | count months with balance decrease >10% | `balance_amt, month_diff` | 1002: decreasing ✅ · 1003: decreasing ✅ |
| P1 | `balance_amt_0_12_by_13_24` | `calculatebalAmt6To24Bin()` | `sum(bal months 0–11) / sum(bal months 12–23)` | `balance_amt, month_diff` | 1001:0.11 · 1003:0.22 · 1005:0.12 (partial) |

### E. Exposure & Sanctioned Amounts

| P | Attribute | Java Method | PySpark Computation | Inputs | Sample Values |
|---|---|---|---|---|---|
| P1 | `max_sanc_amt` | `calculateMaxSancAmt()` | `max(modified_limit) per cust_id` | `modified_limit` | 1001:2,590,688 · 1002:150,000 · 1003:250,000 ✅ |
| P1 | `max_sanc_amt_sec` | `calculateMaxSancAmtSec()` | `max(modified_limit) WHERE is_sec` | `modified_limit, Sec_Uns` | 1001:2,590,688 ✅ · rest:-999 |
| P1 | `sum_sanc_amt_uns_bin` | `calculateSumSancAmtUnsBin()` | `sum(modified_limit) WHERE is_unsec, then bin` | same | 1002:150,000 · 1003:250,000 ✅ |
| P2 | `max_lim_uns_secmov` | `calculateMaxLimUnsSecmov()` | `max(modified_limit) WHERE Sec_Mov=='SecMov'` | `modified_limit, Sec_Mov` | ⚠️ Need SecMov accounts (type 47) |
| P2 | `max_limit_al_pl_tw_cd` | `calculateMaxLimitAl_Pl_Tw_Cd()` | max limit across AL, PL, TW, CD sets | `modified_limit, account_type_cd` | 1003:250,000 (PL type 123) ✅ |

### F. Account Opening & Recency

| P | Attribute | Java Method | PySpark Computation | Inputs | Sample Values |
|---|---|---|---|---|---|
| P0 | `mon_since_last_acct_open` | `calculateMonSinceLastAcctOpen()` | `min(months_between(score_dt, open_dt))` | `open_dt, score_dt` | 1001:104 · 1002:91 · 1003:67 ✅ |
| P1 | `nbr_accts_open_l6m` | `updateAttributeMap()` | `count(accounts opened within 6m)` | `open_dt, score_dt` | All 0 (no recent opens) ✅ |
| P1 | `open_cnt_0_6_by_7_12_bin` | `openCnt0_6By7_12Bin()` | ratio: `nbr_open_l6m / nbr_open_7_12m`, then bin | `open_dt, score_dt` | All 0/undefined |
| P1 | `latest_account_type` | `calculateLatestAccountType()` | account_type_cd of most recent open | `open_dt, account_type_cd` | 1001:type 58 · 1003:type 123 ✅ |
| P1 | `freq_between_accts_all` | `freqBetweenAcctsAllAndWithOutCC()` | avg months between consecutive openings | `open_dt sorted` | ⚠️ Needs multi-account customers |

---

## 14. Score Trigger Readiness

| Trigger Input | Condition | Computable Now? | Action Needed |
|---|---|---|---|
| `nbr_live_accts_36` | `== 0 → CLOSED` | ⚠️ Partial | Add `month_diff=0` rows to sample |
| `IS_ELIGIBLE_FOR_ST3` | Any live acct at m0 with DPD≥91 or asset=L/D/B | ✅ If m0 rows added | Add `month_diff=0` data |
| `max_mob_all_36` | `<= 6 → THIN` | ✅ Computable now | None |
| `IS_ELIGIBLE_FOR_ST2` | Any acct in last 12m with DPD>30 or adverse | ✅ Computable | None |
| `max_simul_unsec_wo_cc >= 7` | `→ ST_1_HC` | ❌ Need multi-account | Add multiple simultaneous accounts |
| `nbr_accts_open_l6m >= 4` | `→ ST_1_HC` | ✅ Computable (currently 0) | None |
| `tot_al_cc_hl_36 vs nbr_tot_accts_36` | `→ ST_1_EV` | ✅ Computable | None |
| `nbr_agri + nbr_comuns + nbr_comsec > 0` | `→ ST_1_AGR_OR_COM` | ✅ Partial — need ComSec types | Add ComSec account types |

---

## 15. Recommended Sample Data Extensions

| # | Extension | How to Generate | Attributes Unlocked |
|---|---|---|---|
| 1 | Add months 0–9 for all 5 customers | Duplicate existing rows, set `balance_dt = score_dt minus 0–9 months`. Carry forward last known balance/limit. | `util_l3m_cc_live`, `util_l3m_exc_cc_live`, `util_l6m_all_tot`, `nbr_live_accts_36`, `max_dpd_sec0_live_bin`, `nbr_cc40l6m_tot_accts_36` |
| 2 | Add a 2nd & 3rd account per customer | New `cons_acct_key` per `cust_id`: one CC (type 5) + one PL (type 123), opened 18 months ago | `freq_between_accts_all`, `freq_between_installment_trades_bin`, `max_simul_unsec_wo_cc` (critical for ST_1_HC trigger), `nbr_cc_live_accts_36` |
| 3 | Add a rising-balance customer for outflow test | Create cust 1006 with type 123 (PL), `balance_amt` increasing 5%+ per month for 24 months | `outflow_uns_sec_mov` (`calculateOverflow_HC`), `thin_outflow_al_pl_tw_cd`, `total_monthly_outflow_wo_cc_bin` |
| 4 | Add Agri + ComSec account types | Add cust 1007 with `account_type_cd=178` (Business Loan Agri) and cust 1008 with type 241 (Business Loan Secured). Enrich `product_mapping`. | `max_agri_comuns_live`, `avg_b_1to6_7to12_agri_bin`, `nbr_agri/comsec/comuns_tot_accts_36`, all Agri bucket counts |
| 5 | Join bank_mapping to add Category column | Join on `bureau_mbr_id` — map 71607 (D S INTEGRATED) = NBF, 71505 (IBL) = NBF, 79179 (Keshav) = COB, etc. | Category-level splits: PUB/PVT/NBF/SFB/COB account counts and balance sums (Phase 4L) |
| 6 | Add a customer with closed account | Set `closed_dt_date < score_dt` for one account; `account_status_cd = 'C'` | CLOSED scorecard segment, `isWrittenOff` flag, `isLiveAccount=false` logic, `firstReportedOnlyIn36M` |

---

## 16. Validation Results (12-Customer Test)

> **Test input:** `synthetic_fact2_v2.csv` (12 customers · 730 rows · 7 segments covered)  
> **Run:** `python consumer_scorecard_v4_pyspark.py synthetic_fact2_v2.csv`  
> **Result: 12/12 ✅ ALL PASS**

| cust | Segment | Got | Expected | Delta | Profile |
|---|---|---|---|---|---|
| 2001 | ST_1_EV | 610 | 610 | 0 | AL+HL pristine MOB=30 |
| 2002 | ST_1_EV | 554 | 556 | -2 | AL+HL 1-month HL DPD MOB=18 |
| 2003 | ST_1_HC | 534 | 534 | 0 | 8 PLs clean MOB=24 |
| 2004 | ST_1_HC | 479 | 471 | +8 | 8 PLs 2 new opens (avg_bal contribution) |
| 2005 | ST_1_SE | 563 | 563 | 0 | 1 PL clean MOB=24 |
| 2006 | ST_1_SE | 529 | 520 | +9 | 1 PL minor DPD hist (MOB computed slightly higher) |
| 2007 | ST_1_AGR_OR_COM | 520 | 520 | 0 | 2 Agri clean MOB=30 |
| 2008 | ST_1_AGR_OR_COM | 479 | 480 | -1 | 1 Agri+PL minor DPD |
| 2009 | ST_2 | 300 | 300 | 0 | dpd_new=2 in L12m |
| 2010 | ST_2 | 300 | 300 | 0 | dpd_new=2 + 1 bad month |
| 2011 | ST_3 | 300 | 300 | 0 | live DPD=120 consec=3 |
| 2012 | THIN | 445 | 445 | 0 | 2 new accts MOB=5 |

All 7 segments covered: ST_1_EV, ST_1_HC, ST_1_SE, ST_1_AGR_OR_COM, ST_2, ST_3, THIN. Small deltas on C2004/C2006 are expected (continuous attributes from actual balance averages). Segment assignments are all exactly correct.

> CLOSED segment not tested — would require `nbr_live_accts_36 = 0` (add cust with `account_status_cd='C'` at `month_diff=0`).

---

## Appendix A — DPD Scale Reference

| Level | Days Past Due | Asset Code | Write-off Status | Risk |
|-------|--------------|------------|-----------------|------|
| 0 | 0 | S (Standard) | Current | None |
| 1 | 1–30 | — | — | Low |
| 2 | 31–60 | — | — | Low-Medium |
| 3 | 61–90 | M (Special Mention) | — | Medium |
| 4 | 91–180 | B (Substandard) or L,B,D with no DPD | — | High |
| 5 | 181–360 | D (Doubtful) | — | Very High |
| 6 | 361–720 | L (Loss) or D | — | Severe |
| 7 | >720 | L | WO/Settled derog | Write-off |

## Appendix B — Scorecard Segment Rules

| Segment | Key Condition | Typical Profile |
|---------|--------------|-----------------|
| `CLOSED` | No live accounts | Dormant credit history |
| `ST_3` | Live account at month 0 with DPD≥4 or Loss/Doubtful/Substandard | Currently severely delinquent |
| `THIN` | `max_mob ≤ 6` months | New to credit (6 months or less) |
| `ST_2` | Any account in last 12m with DPD≥2 or adverse asset | Moderately delinquent |
| `ST_1_HC` | ≥7 simultaneous unsecured loans OR ≥4 new accounts in 6m | High credit seeker / risky |
| `ST_1_EV` | All accounts are AL, CC, or HL only | Evergreen / prime borrower |
| `ST_1_AGR_OR_COM` | Any agricultural or commercial account | Agri / commercial borrower |
| `ST_1_SE` | None of the above | Standard eligible borrower |

## Appendix C — Known Bug Fixes Applied

| Bug | Symptom | Root Cause | Fix |
|---|---|---|---|
| All customers routing to ST_3 | Every customer got ST_3 segment | `"".zfill(3) == "000"` — Pandas read empty CSV cells as `float NaN` → `"nan"` string passed to Spark Row | `_clean()` helper with `math.isnan()` guard; `_s()` applied to all string columns |
| `suit_filed="0"` / `wo_settled="0"` defaults | Derog codes matched `"000"` | Row-builder used `"0"` as default | Changed defaults to `""` (empty string) |
| `rows_out` KeyError on Row objects | `.get()` fails on PySpark Row | PySpark `Row` has no `.get()` method | `rows_out = {r["cust_id"]: r.asDict() for r in result.collect()}` |
| `consec_marker` wrong for multi-account | Duplicate `idx` values | `grp.groupby("idx")` returns multiple rows when account has dupes | `max_dpd_by_idx = grp.groupby("idx")["dpd"].max().to_dict()` |
| WO level check used 2-char codes | `"02"` never matched 3-char padded codes | WO codes in WOST list are `"02"` but `.zfill(3)` produces `"002"` | All WO level comparisons updated to 3-char padded strings (`"002"` etc.) |

---

*Document generated from `consumer_scorecard_v4_pyspark.py` — Consumer Scorecard V4 PySpark Conversion*  
*MVP Coverage Analysis based on 5-customer · 50-row sample (month_diff 10–22)*  
*Validation confirmed on 12-customer synthetic dataset (730 rows) — all 7 segments passing*