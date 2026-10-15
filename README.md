# Multi-site SUD project — OMOP longitudinal master datasets (MCW and UW Madison)

**For:** Tim and the data science team · **Built:** October 2026

This repository builds one analysis-ready longitudinal dataset per site from each site's OMOP CDM v5.3 delivery, with a single shared pipeline. Both datasets have the same 225 columns in the same order. The clinical design follows the lab's UW–Madison Stata master-data pipeline. Every variable and rule is documented in **[DATA_DICTIONARY.md](DATA_DICTIONARY.md)**.

---

## The datasets

| | MCW | UW Madison |
|---|---|---|
| File | `mcw_masterdata.parquet` | `uw_masterdata.parquet` |
| Rows | 17,300,719 | 9,814,764 |
| Encounters / patients | 454,009 / 238,342 | 187,225 / 103,680 |
| Cohort encounters (before the has-vitals rule) | 502,636 | 190,499 |
| Years | 2010 – January 2026 | 2008 – June 2025 (delivery effectively ends January 2025) |
| ED-only / inpatient encounters | 81.8% / 18.2% | 66.6% / 33.4% |
| Size in memory | 12.4 GB | 7.0 GB |
| `site` code | `1` | `5` |
| OMOP delivery | `MCW_0618_upload/omop_data_files_20260617` | `UW/09232026` (device file dated 7 Oct 2026) |

The parquet files are written by each site's notebook to the folder it runs in on the lab server. **They are patient-level data (identifiers, timestamps, death dates) and are not in this repository.**

### Grain

One row per **`(person_id, visit_occurrence_id, time)`**, `time` to the minute. A row is any time point at which something was recorded — an ADT segment, a vitals set, a lab result, an oxygen-device chart, an order, a medication administration — so columns not recorded at that minute are missing. Encounter-level columns (demographics, outcomes, encounter class, SUD determinations, Elixhauser, quality flags) repeat on every row of the encounter.

| Column group | Columns |
|---|---|
| Keys, time, location | `site`, `person_id`, `visit_occurrence_id`, `time`, `hours_since_admit`, `loc_cat`, `first_vitals_time`, `last_vitals_time`, `day_of_week` |
| Demographics, outcomes | `sex`, `race`, `ethnicity`, `age`, `died_ever`, `died01`, `disch_disp`, `death_date` |
| Vitals, mental status | `hr` … `gcs_total`, `height_cm`, `weight_kg`, `bmi`, `bmi_derived` |
| Quality flags | `single_vitals_set`, `max_vitals_gap_h`, `has_vitals_gap`, `long_encounter` |
| Respiratory support | `vent`, `trach`, `hfnc`, `nasal_cannula`, `roomair`, `other_o2device`, `noninvasive_vent`, `fio2_final` |
| Labs | 51 analytes, `albumin` … `wbc` |
| Orders | `echo`, `ekg`, `trans_*`, `xr_*`, `ct_*` |
| Medications | 19 flags: `abx_*`, `opioid_*`, `benzodiazepine`, `sedative_other_*`, `steroid_*`, `vasoactive`, `sofa_card_*`, `mat_oud`, `aud_med`, `naloxone`, `nrt`, `paralytic`, `thiamine` |
| Encounter class | `enc_class`, `visit_source_prefix`, `enc_*` |
| SUD determinations | `icd_*`, `nlp_*`, `nlp_*_prob`, `sud_*` roll-ups, `dx_sud_*` |
| Comorbidities | 31 `elix_*`, 31 `elix_*_prior`, `elix_count`, `elix_count_prior` |

---

## Quick start

```python
import pandas as pd

mcw = pd.read_parquet("mcw_masterdata.parquet")
uw  = pd.read_parquet("uw_masterdata.parquet")
both = pd.concat([mcw, uw], ignore_index=True)        # same columns, same order; site 1 = MCW, 5 = UW

# read only what you need — the MCW file is 12 GB in memory
vitals = pd.read_parquet("uw_masterdata.parquet",
                         columns=["site", "person_id", "visit_occurrence_id", "time", "hr", "sbp", "o2sat"])
```

Filters that the build deliberately leaves to the analysis:

```python
m = both[both.long_encounter == 0]        # encounters spanning > 90 days (MCW 1,220, UW 108)
m = m[m.has_vitals_gap == 0]              # Stata rule: any >= 24 h gap between vitals (MCW 28,273, UW 1,021)
m = m[m.single_vitals_set == 0]           # all vitals at one minute — no trajectory
inpatient = m[m.enc_class == 2]           # 1 = ED only, 2 = inpatient (incl. ED-to-inpatient and observation)
```

Two conventions to know before writing any code:

- **Event flags are `1` or missing, never `0`** — devices, orders, medications. Use `col == 1` or `col.notna()`; fill with 0 only if "no record" should mean "not given".
- **Encounter-level columns** can be taken from the first row: `m.groupby("visit_occurrence_id").first()`.

---

## How the datasets are built

Both sites run the **same module files and the same 13 notebook steps**. The site is chosen by one line (`os.environ["OMOP_SITE"] = "MCW"` or `"UW"`); everything that differs between the sites is a difference in the *source data* — concept ids, unit labels, code formats, where a construct is charted — held in `pipeline_config.py` as `*_BY_SITE` settings. The processing rules are shared.

| Step | Domain | Main source |
|---:|---|---|
| 1 | Load; drop excluded visit types (UW outpatient, laboratory, generic hospital) | all tables |
| 2 | Demographics, outcomes, encounter class, SUD determinations | `person`, `visit_occurrence`, `death`, site cohort file |
| 3 | Elixhauser comorbidities (verbatim from the Stata do-file) | `condition_occurrence` |
| 4 | Location category | `visit_detail` |
| 5 | Vitals, GCS, AVPU, orientation, height/weight/BMI | `measurement` (+ UW `observation`) |
| 6 | Oxygen devices, Stata up-coding hierarchy | `device_exposure` |
| 7 | Labs: censored results, non-result rows, unit harmonisation, bounds, rounding | `measurement` |
| 8 | Nursing scales (empty at both sites) | `measurement`, `observation` |
| 9 | Orders: imaging, ECG, echo, transfusion | `procedure_occurrence`, `device_exposure` |
| 10 | Medication flags from the multicenter's shared lists | `drug_exposure` (administration records) |
| 11 | Master assembly and derived columns | Steps 2–10 |
| 12 | Final audit | — |
| 13 | Save in the shared 225-column schema | — |

**Cohort.** Adults (≥ 18; age top-coded at 86) with at least one core vital sign (HR, RR, BP, SpO₂, temperature, AVPU or orientation). The Stata inpatient restriction, single-vitals-set drop and 24-hour-gap drop are **switched off** — ED-only encounters are the population this project is about — and the last two are carried as flags instead.

**Assembly.** Labs, devices, orders and medications are outer-joined onto the ADT + vitals timeline at the minute, so every row inside the encounter's visit window (± 24 h) is kept; a row that shares a minute merges, otherwise it adds a time point. Each notebook checks that every domain row reached the master (Audit 11-pre), that the key is unique, and that the saved columns equal the shared list (Audit 13).

Each notebook audits every step before moving on and ends with a **sign-off checklist**; every item is ✅ for the runs in this repository.

---

## Read this before analysing

The full list is in [DATA_DICTIONARY.md §7](DATA_DICTIONARY.md#7-caveats-for-analysts). The ones that change results:

1. **`died_ever` is patient-level, not in-hospital mortality.** Following the Stata rule, it marks the patient's last encounter whenever the death occurred: 42% (MCW) and 45% (UW) of these deaths are more than a year after discharge. For in-hospital death use `disch_disp == 6`.
2. **Take SUD prevalence from the cohort files, not the master.** Encounters without a core vital sign are not in the master. At MCW they are 9.7% of the cohort but hold half the `nlp_opioid` encounters (7.05% in the master against 12.7% in the file).
3. **NLP determinations are permissive.** All NLP classes are cut at probability 0.02; the cohort files contain screened-positive encounters only, with no negatives. `nlp_*_prob` is missing — not low — where the model did not score an encounter.
4. **Medication time differs by site.** MCW flags carry the administration minute; UW administrations are dated only, so UW flags sit at 00:00 of the day (on the admission day, at the encounter's first time point).
5. **Lab results outside the test's range.** When a result is too low for the lab to measure, the lab reports text such as `<0.01` instead of a number. These are stored at half the limit (a negative blood alcohol as 0.005 g/dL, an undetectable troponin as 0.005 ng/mL); results above the range are stored at the limit (`>600` → 600). They mean "not detected" or "at least", not a measured amount: `blood_alcohol > 0` is true for every negative test, so use a cut-off above the reporting limit (e.g. ≥ 0.01 g/dL).
6. **Respiratory support** uses the Stata hierarchy (trach > BiPAP > HFNC > CPAP > nasal cannula > other > room air > vent), so `vent` = 1 only where no other device is charted that minute. UW almost never charts room air on its own (`roomair` on 48 rows).

---

## Open items for the team

**Medication classes — judgement calls left as they were.** The multicenter's shared medication lists contained entries that are not drugs of their class (plain acetaminophen under `opioid_noniv`, ondansetron and famotidine under `steroid_iv`, …). These are corrected identically at both sites — 41 changes, each with its reason in `cfg.MED_KEY_CHANGES` and DATA_DICTIONARY §5. These five are for the team to decide:

| Item | Current handling | Administration rows MCW / UW |
|---|---|--:|
| Methadone | sets an opioid flag **and** `mat_oud` (pain and OUD use not separated) | 15,630 / 53,032 |
| Buprenorphine | `mat_oud`, including pain forms (patch, buccal film) | 22,157 / 46,216 |
| Naltrexone | sets both `mat_oud` and `aud_med` | 3,799 / 8,418 |
| Gabapentin | not flagged (off-label AUD treatment and a misuse signal; a new flag would change the schema) | 296,839 / 253,070 |
| Epidural opioid–anaesthetic mixes | not flagged by any list | 90 / 0 |

Chlorpheniramine as a sedative (0 / 116 rows) and the unidentified `PAR` under `abx_noniv` (0 / 0) are also listed for review.

---

## Repository contents

The notebooks import the modules from their own folder, so everything sits side by side.

| File | Role |
|---|---|
| `README.md` | This file |
| `DATA_DICTIONARY.md` | Every column, rule and site difference |
| `MCW_Master_Build_Audit_Final.ipynb` | MCW build and audit, executed (aggregate output only) |
| `UW_Master_Build_Audit_Final.ipynb` | UW build and audit, executed (aggregate output only) |
| `pipeline_config.py` | All settings: concept maps, clinical rules, `*_BY_SITE` source differences, shared schema, `PIPELINE_VERSION` |
| `pipeline_encounter.py` | Demographics, outcomes, encounter class, SUD flags, ADT / `loc_cat` |
| `pipeline_timeseries.py` | Vitals, oxygen devices, labs, nursing, orders, medications |
| `pipeline_master.py` | Master assembly, derived columns, downcast, shared schema |
| `pipeline_audit.py` | The audit tables shown in the notebooks |
| `pipeline_viz.py` | Charts drawn from aggregate audit tables |
| `catchall_40493498.py` | MCW: routes the catch-all concept 40493498 (twelve instruments) to AVPU and orientation |
| `uw_mental_status.py` | UW: AVPU and orientation from `observation` |
| `elixhauser.py` | Elixhauser flags, transcribed from the Stata do-file |
| `cohort_flags.py` | Loads each site's cohort file (SUD determinations) |
| `sud_codes.py` | Loads `Substance_ICD_Codes.txt` for the `dx_sud_*` cross-check |
| `read_tables.py` | UW reader for Spark-style parquet folders (reads every part file in a table's folder) |
| `read_tables_mcw.py` | MCW reader — MCW's local file, kept in the MCW working folder |
| `deident.py` | De-identification: display guard, small-cell masking, `fmt_count` |
| `deid_check.py` | Scans an executed notebook for anything that looks like a small count — run before every commit |
| `discover_measurements.py` | Profiles a new site's `measurement` table to set up its concept maps |
| `uw_investigate.py`, `uw_gap_checks.py` | Stand-alone UW follow-up checks run during the build review; not needed to build |

Needed at run time but **not committed**: the OMOP deliveries, the site cohort files (MCW `master_enc_lst_w_pred_prob_20260617.txt`, UW `cwalk_df`), and the output parquet files. `Substance_ICD_Codes.txt` (the project SUD code list) must sit in the working folder; the build stops without it.

Suggested `.gitignore`:

```
*.parquet
*.csv
master_enc_lst_*.txt
cwalk_df/
__pycache__/
.ipynb_checkpoints/
```

---

## Re-running a build

1. Put the modules, the site's notebook and `Substance_ICD_Codes.txt` in one folder on the lab server (MCW also needs `read_tables_mcw.py`). Python 3.11 with pandas, numpy, pyarrow and matplotlib.
2. Check `DATA_DIR` in Step 1 and the cohort-file path in `pipeline_config.py` (`COHORT_FLAG_FILE`, `COHORT_FLAG_FILE_UW`).
3. Restart the kernel and run all cells. Step 1 asserts the site and `PIPELINE_VERSION == "2026-10-13d"`, so a stale module set stops the run immediately.
4. MCW is the memory-heavy build (the master is 24 GB in memory before the downcast). The notebook slims the large source tables before assembly and the merges run in float32; run it on the large-memory server.
5. Work through the audits and the sign-off checklist. Then, before committing the executed notebook:

   ```
   python deid_check.py MCW_Master_Build_Audit_Final.ipynb UW_Master_Build_Audit_Final.ipynb
   ```

   It must report `clean` for both.

Changing a rule means changing `pipeline_config.py` (or a module), bumping `PIPELINE_VERSION`, updating the assert in both notebooks, and re-running **both** sites so they stay aligned.

---

## De-identification of the notebooks

The executed notebooks are published here, so all displayed output is aggregate (`deident.py`, applied automatically from Step 1):

- no `person_id`, `visit_occurrence_id`, `PSEUDO_PAT_ID` / `PSEUDO_ENC_ID`, and no line-level patient rows;
- no calendar dates on patient rows — the two example trajectories show hours elapsed only;
- counts from 1 to 10 shown as `<11`, in tables and in build logs;
- age in bins, top-coded.

`deid_check.py` reports both notebooks clean. Masking is cell by cell; there is no complementary suppression, so a masked count could in principle be recovered by subtraction from a total in the same table.

---

## Versions

| Item | Version |
|---|---|
| Shared pipeline | `2026-10-13d` (`cfg.PIPELINE_VERSION`) |
| OMOP CDM | v5.3 |
| Python | 3.11 |
| Runs in this repository | both sites, executed in order from a fresh kernel on pipeline `2026-10-13d`; UW with the `device_exposure` file dated 7 Oct 2026 (2,177,320 rows, including the tracheostomy rows) |
