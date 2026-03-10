"""
Generate synthetic_fact2_v2.csv  — 12 customers covering all 8 scorecard paths
with meaningful score variation within each segment.

Customer Map
────────────────────────────────────────────────────────────────────────────────
ID    Segment          Profile                               Expected Score
────────────────────────────────────────────────────────────────────────────────
2001  ST_1_EV          AL(047) + HL(058), pristine, MOB=30   ~610
2002  ST_1_EV          AL + HL, 1-month HL DPD, MOB=18       ~556
2003  ST_1_HC          8 live PLs, clean, MOB=24             ~534
2004  ST_1_HC          8 live PLs, 2 opened recently         ~479
2005  ST_1_SE          1 PL, clean, long MOB=24              ~563
2006  ST_1_SE          1 PL, 1-month DPD hist, MOB=12        ~520
2007  ST_1_AGR_OR_COM  2 Agri loans, clean, MOB=30           ~520
2008  ST_1_AGR_OR_COM  1 Agri + 1 PL, minor DPD, MOB=15     ~480
2009  ST_2             PL with dpd_new=2 (DPD=40) in last 12m    ~300
2010  ST_2             PL with dpd_new=2 + 1 bad month       ~300
2011  ST_3             Live PL DPD=120, ConsecMark=3         ~300
2012  THIN             New borrower, 2 accts, MOB=5          ~445
────────────────────────────────────────────────────────────────────────────────

Notes on trigger safety:
- All ST_1_* customers: no live DPD≥4 (no ST_3), no DPD≥2 in 12m (no ST_2),
  MOB>6 (no THIN), nbr_live_accts_36>0 (no CLOSED)
- ST_2 customers: DPD occurred only historically, NOT at month_diff=0 (no ST_3)
- THIN: MOB<=6 triggers before ST_1_* checks
"""

import csv, os
from datetime import date

SCORE_DT   = 20240301
OUT_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "synthetic_fact2_v2.csv")

COLS = [
    "cust_id","accno","bureau_mbr_id","account_type_cd",
    "open_dt","closed_dt","score_dt","balance_dt","month_diff",
    "balance_amt","credit_lim_amt","original_loan_amt",
    "past_due_amt","dayspastdue","pay_rating_cd","account_status_cd",
    "actual_pymnt_amt","SUITFILED_WILFULDEFAULT","WRITTEN_OFF_AND_SETTLED",
    "Sec_Uns","Reg_Com","Sec_Mov","Pdt_Cls","Category","relFinCd",
]

def balance_dt(month_diff):
    score_d = date(int(str(SCORE_DT)[:4]), int(str(SCORE_DT)[4:6]), int(str(SCORE_DT)[6:8]))
    y, m = score_d.year, score_d.month - month_diff
    while m <= 0:
        m += 12; y -= 1
    return int(date(y, m, 1).strftime("%Y%m%d"))

def row(cust_id, accno, actype, months,
        bal_start, bal_step,
        credit_lim=-1, orig_loan=-1,
        dpd_schedule=None,          # dict: {month_diff: dpd_value}
        pr_schedule=None,           # dict: {month_diff: pay_rating}
        status="O", open_dt=20210101, closed_dt=99991231,
        sec_uns="UnSec", reg_com="Regular", sec_mov="UnSecMov",
        pdt_cls="RegUns", category="PVT",
        wo="", sfw="", bur=1001, rel="1"):
    """Emit `months` rows for one account."""
    rows = []
    dpd_sch = dpd_schedule or {}
    pr_sch  = pr_schedule  or {}
    for m in range(months):
        bal = max(500.0, bal_start - m * bal_step)
        dpd = dpd_sch.get(m, 0)
        pr  = pr_sch.get(m, "S")
        rows.append({
            "cust_id":                  cust_id,
            "accno":                    accno,
            "bureau_mbr_id":            bur,
            "account_type_cd":          str(actype),
            "open_dt":                  open_dt,
            "closed_dt":                closed_dt,
            "score_dt":                 SCORE_DT,
            "balance_dt":               balance_dt(m),
            "month_diff":               m,
            "balance_amt":              bal,
            "credit_lim_amt":           float(credit_lim),
            "original_loan_amt":        float(orig_loan),
            "past_due_amt":             0.0,
            "dayspastdue":              dpd,
            "pay_rating_cd":            pr,
            "account_status_cd":        status,
            "actual_pymnt_amt":         bal * 0.04,
            "SUITFILED_WILFULDEFAULT":  sfw,
            "WRITTEN_OFF_AND_SETTLED":  wo,
            "Sec_Uns":                  sec_uns,
            "Reg_Com":                  reg_com,
            "Sec_Mov":                  sec_mov,
            "Pdt_Cls":                  pdt_cls,
            "Category":                 category,
            "relFinCd":                 rel,
        })
    return rows

all_rows = []

# ─────────────────────────────────────────────────────────────────────────────
# C2001  ST_1_EV — AL + HL, pristine, MOB=30  → expected ~610
#   Trigger: all accounts are AL(047) or HL(058) only
#   Score: 550 + 30*2 = 610
# ─────────────────────────────────────────────────────────────────────────────
all_rows += row(2001, 201001, "047", 30,
                bal_start=500000, bal_step=8000, orig_loan=720000,
                open_dt=20210901, sec_uns="Sec", reg_com="Regular",
                sec_mov="SecMov", pdt_cls="RegSec", category="PSB")
all_rows += row(2001, 201002, "058", 30,
                bal_start=2800000, bal_step=40000, orig_loan=3000000,
                open_dt=20210901, sec_uns="Sec", reg_com="Regular",
                sec_mov="SecMov", pdt_cls="RegSec", category="PSB")

# ─────────────────────────────────────────────────────────────────────────────
# C2002  ST_1_EV — AL + HL, 1-month HL late payment, MOB=18  → expected ~556
#   Score: 550 + 18*2 - 30*1 = 556
# ─────────────────────────────────────────────────────────────────────────────
all_rows += row(2002, 202001, "047", 18,
                bal_start=420000, bal_step=12000, orig_loan=620000,
                open_dt=20221001, sec_uns="Sec", reg_com="Regular",
                sec_mov="SecMov", pdt_cls="RegSec", category="PSB")
all_rows += row(2002, 202002, "058", 18,
                bal_start=1900000, bal_step=40000, orig_loan=2100000,
                open_dt=20221001,
                dpd_schedule={8: 20},   # DPD 1-30 at month 8 → dpd_new=1 on HL
                sec_uns="Sec", reg_com="Regular",
                sec_mov="SecMov", pdt_cls="RegSec", category="PSB")

# ─────────────────────────────────────────────────────────────────────────────
# C2003  ST_1_HC — 8 live PLs(123), all clean, MOB=24  → expected ~534
#   Trigger: max_simul_unsec_wo_cc = 8 >= 7
#   Score: 500 + 24*1.5 + 0*(-25) = 536  (avg_bal term small)
# ─────────────────────────────────────────────────────────────────────────────
for i in range(8):
    all_rows += row(2003, 203000+i, "123", 24,
                    bal_start=120000, bal_step=3000, orig_loan=145000,
                    open_dt=20220301, category="NBFC")

# ─────────────────────────────────────────────────────────────────────────────
# C2004  ST_1_HC — 8 live PLs, 2 opened recently (MOB<=6), 1 minor DPD  → ~479
#   Trigger: max_simul_unsec_wo_cc = 8 >= 7
#   Score: 500 + 24*1.5 - 25*1 - 15*2 = 471
# ─────────────────────────────────────────────────────────────────────────────
for i in range(6):   # 6 old PLs MOB=24
    all_rows += row(2004, 204000+i, "123", 24,
                    bal_start=120000, bal_step=3000, orig_loan=145000,
                    open_dt=20220301, category="NBFC")
for i in range(2):   # 2 new PLs MOB=4 (opened within 6m)
    all_rows += row(2004, 204010+i, "123", 4,
                    bal_start=100000, bal_step=1000, orig_loan=100000,
                    open_dt=20231101, category="NBFC")
# Also add 1 old PL with dpd_new=1 in history (month 20) — not recent enough for ST_2
all_rows += row(2004, 204020, "123", 24,
                bal_start=90000, bal_step=2000, orig_loan=100000,
                open_dt=20220301,
                dpd_schedule={20: 15},   # DPD 1-30 at month 20 → dpd_new=1 but outside 12m
                category="NBFC")

# ─────────────────────────────────────────────────────────────────────────────
# C2005  ST_1_SE — single PL, clean, MOB=24  → expected ~563
#   Trigger: default (not HC/EV/AGR)
#   Score: 520 + 24*1.8 = 563
# ─────────────────────────────────────────────────────────────────────────────
all_rows += row(2005, 205001, "123", 24,
                bal_start=350000, bal_step=8000, orig_loan=400000,
                open_dt=20220301, category="PVT")

# ─────────────────────────────────────────────────────────────────────────────
# C2006  ST_1_SE — single PL, had dpd=1 at month 14 (outside 12m), MOB=12  → ~520
#   Score: 520 + 12*1.8 - 22*1 = 520   (dpd_new=1 → max_dpd_uns_l36m=1)
# ─────────────────────────────────────────────────────────────────────────────
all_rows += row(2006, 206001, "123", 16,
                bal_start=280000, bal_step=9000, orig_loan=310000,
                open_dt=20221001,
                dpd_schedule={14: 25},   # DPD=25 at month 14 → dpd_new=1, outside L12m
                category="SFB")

# ─────────────────────────────────────────────────────────────────────────────
# C2007  ST_1_AGR_OR_COM — 2 Agri loans(167), clean, MOB=30  → expected ~520
#   Trigger: has_agri_or_com → nbr_agri_tot_accts_36 = 2
#   Score: 480 + 2*5 + 30*1 = 520
# ─────────────────────────────────────────────────────────────────────────────
all_rows += row(2007, 207001, "167", 30,
                bal_start=90000, bal_step=2000, orig_loan=120000,
                open_dt=20210901,
                sec_uns="Sec", reg_com="AgriPSL",
                sec_mov="SecMov", pdt_cls="RegSec", category="PSB")
all_rows += row(2007, 207002, "167", 30,
                bal_start=70000, bal_step=1500, orig_loan=100000,
                open_dt=20210901,
                sec_uns="Sec", reg_com="AgriPSL",
                sec_mov="SecMov", pdt_cls="RegSec", category="PSB")

# ─────────────────────────────────────────────────────────────────────────────
# C2008  ST_1_AGR_OR_COM — 1 Agri + 1 PL, dpd_new=1 on PL, MOB=15  → expected ~480
#   Score: 480 + 1*5 + 15*1 - 20*1 = 480
# ─────────────────────────────────────────────────────────────────────────────
all_rows += row(2008, 208001, "167", 15,
                bal_start=75000, bal_step=2500, orig_loan=100000,
                open_dt=20230101,
                sec_uns="Sec", reg_com="AgriPSL",
                sec_mov="SecMov", pdt_cls="RegSec", category="PSB")
all_rows += row(2008, 208002, "123", 15,
                bal_start=60000, bal_step=2000, orig_loan=80000,
                open_dt=20230101,
                dpd_schedule={13: 20},   # DPD 1-30 at month 13 → dpd_new=1
                category="NBFC")

# ─────────────────────────────────────────────────────────────────────────────
# C2009  ST_2 — PL with dpd_new=2 (DPD=40) within last 12m  → expected ~300
#   Trigger: is_eligible_for_st2 (dpd_new>=2 in l12m)... wait need dpd>=2
#   Actually ST_2 trigger needs dpd_new>=2. Use dpd=40 (dpd_new=2)
#   Score: 350 - 30*2 - 20*0 = 290 → clamped 300
#   Use dpd=40 at month 5 (within 12m) → dpd_new=2, 1 bad month
#   Score: 350 - 30*2 - 20*1 = 270 → 300
#   For ~320: use dpd=25 (dpd_new=1) at month 5
#   Score: 350 - 30*1 - 20*0 = 320
# ─────────────────────────────────────────────────────────────────────────────
all_rows += row(2009, 209001, "123", 24,
                bal_start=220000, bal_step=5000, orig_loan=280000,
                open_dt=20220301,
                dpd_schedule={5: 40},    # DPD=40 at month 5 → dpd_new=2, within 12m → ST_2
                category="SFB")
all_rows += row(2009, 209002, "130", 20,
                bal_start=100000, bal_step=3000, orig_loan=130000,
                open_dt=20220701, category="PVT")

# ─────────────────────────────────────────────────────────────────────────────
# C2010  ST_2 — PL with dpd_new=2 within 12m, 1 count of nbr_30_24m  → ~300
#   Score: 350 - 30*2 - 20*1 = 270 → clamped 300
# ─────────────────────────────────────────────────────────────────────────────
all_rows += row(2010, 210001, "123", 24,
                bal_start=180000, bal_step=5000, orig_loan=240000,
                open_dt=20220301,
                dpd_schedule={6: 45},    # DPD=45 at month 6 → dpd_new=2, within 12m
                category="PVT")
all_rows += row(2010, 210002, "130", 18,
                bal_start=80000, bal_step=2000, orig_loan=100000,
                open_dt=20221001, category="NBFC")

# ─────────────────────────────────────────────────────────────────────────────
# C2011  ST_3 — live PL with DPD=120 at m=0 (dpd_new=4), 3 consec bad months
#   Score: 250 - 40*4 - 50*3 = 250-160-150 = -60 → clamped 300
# ─────────────────────────────────────────────────────────────────────────────
all_rows += row(2011, 211001, "123", 20,
                bal_start=300000, bal_step=5000, orig_loan=350000,
                open_dt=20220601,
                dpd_schedule={0: 120, 1: 100, 2: 75},  # consec 3 months
                category="PVT")
all_rows += row(2011, 211002, "058", 20,
                bal_start=2500000, bal_step=40000, orig_loan=2800000,
                open_dt=20220601,
                sec_uns="Sec", reg_com="Regular",
                sec_mov="SecMov", pdt_cls="RegSec", category="PSB")

# ─────────────────────────────────────────────────────────────────────────────
# C2012  THIN — 2 new accounts, MOB=5 (max_mob<=6 → THIN)  → expected ~445
#   Score: 400 + 2*10 + 5*5 = 445
# ─────────────────────────────────────────────────────────────────────────────
all_rows += row(2012, 212001, "123", 5,
                bal_start=80000, bal_step=2000, orig_loan=80000,
                open_dt=20231001, category="PVT")
all_rows += row(2012, 212002, "130", 5,
                bal_start=50000, bal_step=1500, orig_loan=50000,
                open_dt=20231001, category="NBFC")

# ─────────────────────────────────────────────────────────────────────────────
# Write CSV
# ─────────────────────────────────────────────────────────────────────────────
# with open(OUT_FILE, "w", newline="") as f:
#     writer = csv.DictWriter(f, fieldnames=COLS)
#     writer.writeheader()
#     writer.writerows(all_rows)

# print(f"✅  Written {len(all_rows)} rows → {OUT_FILE}")

# Summary
from collections import Counter
cust_counts = Counter(r["cust_id"] for r in all_rows)
acct_counts = Counter((r["cust_id"], r["accno"]) for r in all_rows)

print(f"    Customers : {len(cust_counts)}")
print(f"    Accounts  : {len(set((r['cust_id'],r['accno']) for r in all_rows))}")
print(f"    Total rows: {len(all_rows)}")
print()
print("  Expected results:")
print("  ─────────────────────────────────────────────────────────────────")
print("  cust  segment            profile                         score")
print("  ─────────────────────────────────────────────────────────────────")
print("  2001  ST_1_EV            AL+HL pristine MOB=30           ~610")
print("  2002  ST_1_EV            AL+HL 1-month HL DPD MOB=18     ~556")
print("  2003  ST_1_HC            8 PLs clean MOB=24              ~534")
print("  2004  ST_1_HC            8 PLs 2 new opens               ~471")
print("  2005  ST_1_SE            1 PL clean MOB=24               ~563")
print("  2006  ST_1_SE            1 PL minor DPD hist MOB=12      ~520")
print("  2007  ST_1_AGR_OR_COM    2 Agri clean MOB=30             ~520")
print("  2008  ST_1_AGR_OR_COM    1 Agri+PL minor DPD             ~480")
print("  2009  ST_2               dpd_new=2 in L12m (DPD=40 at m5)        ~300")
print("  2010  ST_2               dpd_new=2 + 1 bad month         ~300")
print("  2011  ST_3               live DPD=120 consec=3           ~300")
print("  2012  THIN               2 new accts MOB=5               ~445")
print("  ─────────────────────────────────────────────────────────────────")