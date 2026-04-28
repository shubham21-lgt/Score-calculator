# Refactor Design: `build_global_attrs` → Modular Business Functions

## Current state
- **`build_global_attrs`** is **2,509 lines** — one monolithic function that builds ~50 intermediate DataFrames and joins them all into one customer-level result.

## Refactor strategy

Group all logic into **9 business-domain modules**, each containing 2–6 sub-functions. Every sub-function takes the same inputs (`exploded`, `ad`, etc.) and returns a single-purpose DataFrame keyed on `cust_id`.

The orchestrator `build_global_attrs` becomes a **thin coordinator** (~80 lines) that:
1. Builds the shared base DataFrames (`ad`, `exploded`)
2. Calls each business module
3. Joins all results into `global_attrs`
4. Returns

## Execution hierarchy (top-down)

```
build_global_attrs (orchestrator)
│
├── 1. SHARED FOUNDATIONS  (build first, used by everything)
│   ├── prepare_account_master(account_details)               → ad
│   └── prepare_monthly_exploded(ad)                          → exploded
│
├── 2. ACCOUNT INVENTORY MODULE  (counts of accounts by category)
│   ├── count_accounts_by_product_type(ad)                    → counts_36, live_counts
│   ├── count_accounts_by_open_window(exploded)               → open_cnt_6_7_12
│   ├── count_pl_le50k_accounts(ad)                           → nbr_pl_le50_df
│   └── count_agri_diff_accounts(ad)                          → nbr_agri_diff_df
│
├── 3. CREDIT UTILIZATION MODULE  (balance / limit ratios)
│   ├── calculate_credit_utilization_all(exploded)            → util_all_df
│   ├── calculate_credit_utilization_unsecured(exploded)      → util_uns_df
│   ├── calculate_credit_utilization_excluding_cc(exploded)   → util_exc_cc_df
│   ├── calculate_credit_utilization_cc_only(exploded)        → util_cc_df
│   └── calculate_max_sanction_secured(ad)                    → max_sanc_amt_sec_df
│
├── 4. DELINQUENCY MODULE  (DPD-related metrics)
│   ├── derive_worst_delinquency_bins(exploded, ad)           → worst_delq_bin
│   ├── count_consecutive_dpd_months(exploded)                → consec_df
│   ├── identify_max_dpd_secured_current(exploded)            → max_dpd_sec0_live_df
│   ├── count_dpd_months_current(exploded)                    → nbr_0_0m_df
│   ├── count_dpd_months_last_24m(exploded)                   → nbr_0_24m_live_df
│   ├── max_dpd_months_unsecured_24m(exploded)                → max_nbr_0_24m_uns_df
│   ├── derive_recent_delinquency_recency(ad)                 → mon_recent_x
│   └── compute_delinquency_balance_ratios(exploded)          → dlq_ratio_df
│
├── 5. EXPOSURE & BALANCE MODULE  (current balances, limits, max amounts)
│   ├── calculate_current_exposure_summary(exploded)          → exp_df
│   ├── identify_max_limit_per_customer(ad)                   → max_lim_df
│   ├── identify_max_balance_per_customer(ad)                 → max_bal_df
│   ├── calculate_agri_balance_ratio(exploded, ad)            → agri_bal_ratio_df
│   ├── calculate_balance_ratio_window_0_12_vs_13_24(exploded)→ bal_ratio_df
│   ├── calculate_balance_amt_12_24_window(exploded)          → bal_amt_12_24_df
│   └── derive_months_since_max_balance_24m(ad)               → mon_since_max_bal_df
│
├── 6. INCOME-CONSEC MODULE  (consecutive-balance-increase patterns)
│   ├── compute_consec_balance_increase_excluding_cc(exploded) → consinc_bal10_exc_cc_df
│   ├── compute_consec_balance_increase_cc(exploded)          → consinc_bal5_cc_df
│   ├── compute_consec_increase_all_7m_window(exploded)       → consinc_all7_df
│   └── compute_consec_utilization_increase(exploded)         → totconsinc_util1_df
│
├── 7. TREND MODULE  (balance-trend percentages over time)
│   ├── calculate_balance_trend_excluding_cc(exploded)        → trend_exccc_df
│   ├── calculate_balance_trend_all(exploded)                 → trend_all_df
│   └── compute_trend_pct_changes(trend_*)                    → trend_df
│
├── 8. CASHFLOW MODULE  (monthly principal outflows / payments)
│   ├── calculate_outflow_by_loan_type(exploded, ad)          → outflow_hl_lap_df, outflow_al_tw_df, outflow_pl_df, outflow_cd_df
│   ├── calculate_outflow_total_excluding_cc(...)             → total_outflow_wo_cc
│   ├── calculate_outflow_overflow_amounts(exploded, ad)      → outflow_df, outflow_plcdtw_df
│   └── calculate_outflow_binary_flags(...)                   → outflow_bin_df
│
├── 9. ACCOUNT TIMING & SIMULTANEITY MODULE  (open-date metrics)
│   ├── calculate_account_opening_frequency(ad, open_dt_all)  → freq_df
│   ├── derive_account_opening_recency(ad, open_dt_all)       → recency_df
│   ├── count_simultaneous_accounts_opened(ad)                → simul_df
│   ├── count_simultaneous_loans_by_type(ad)                  → simul_gl_df, simul_uns_all_df, simul_plcd_df
│   ├── identify_max_simultaneous_pl_count(ad)                → in simul_plcd_df
│   ├── identify_minimum_mob_unsecured_no_cc(ad)              → min_mob_uns_exc_cc_df
│   └── derive_latest_account_type(ad)                        → latest_acct_type_df
│
└── 10. CARDS & SPECIAL CASES MODULE
    ├── compute_cc_40pct_utilization_metrics(exploded)        → cc40_df
    ├── compute_cc_40pct_l6m_metrics(exploded)                → cc40_6m_df
    ├── compute_agri_comuns_live_count(ad)                    → agri_comuns_live_df
    ├── compute_derogatory_months_count(ad)                   → derog_months
    ├── compute_reporting_months_count(exploded)              → rpt_df
    └── compute_bank_concentration(ad, bank_map)              → bank_df
```

## Final assembler

```python
def assemble_global_attributes(*all_module_dfs) -> DataFrame:
    """Final left-join of all per-customer attribute DataFrames.
    Coalesces specified zero-fill columns and applies post-rename dedup."""
    ...
```

## Design principles applied

1. **Business-domain naming** — every function name describes WHAT it computes, not HOW
2. **Single responsibility** — each function returns ONE keyed DataFrame
3. **Plug-and-play** — every function takes only what it needs (mostly `exploded`/`ad`)
4. **Minimal coupling** — functions don't share state; they share only inputs
5. **Independent testability** — call any function in isolation with sample data
6. **Logic preservation** — every line from current `build_global_attrs` maps to exactly one function

## What's untouched

Per your instructions: only `build_global_attrs` is being refactored. These remain identical:
- `load_inputs`, `build_fact2`, `build_fact2_enriched`, `build_account_details`
- `add_product_flags`, all UDFs (`dpd_new_udf`, `modified_limit_udf`, `derog_flag_udf`)
- `compute_trigger_eligibility`, `apply_score_trigger`, `compute_final_score`
- `run_pipeline`, `clean_numeric_date`
