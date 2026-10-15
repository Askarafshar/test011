# Data dictionary — OMOP longitudinal master datasets (MCW and UW Madison)

Shared pipeline version **`2026-10-13d`** · OMOP CDM v5.3 · one dictionary for both sites

| File | Rows | Encounters | Patients | Columns | Time span of rows |
|---|--:|--:|--:|--:|---|
| `mcw_masterdata.parquet` | 17,300,719 | 454,009 | 238,342 | 225 | 2010 – January 2026 |
| `uw_masterdata.parquet` | 9,814,764 | 187,225 | 103,680 | 225 | 2008 – June 2025 (delivery effectively ends January 2025) |

Both files have the same 225 columns, with the same names, in the same order (`cfg.SHARED_MASTER_COLUMNS`). The `#` in the tables below is the column's position in the file.

**Contents**

1. [Reading this dictionary](#1-reading-this-dictionary)
2. [How a row is built](#2-how-a-row-is-built)
3. [Column reference](#3-column-reference)
   - [3.1 Identifiers, time and location](#31-identifiers-time-and-location) · [3.2 Demographics and outcomes](#32-demographics-and-outcomes) · [3.3 Vitals and mental status](#33-vitals-and-mental-status) · [3.4 Encounter quality flags](#34-encounter-quality-flags) · [3.5 Respiratory support](#35-respiratory-support) · [3.6 Nursing](#36-nursing) · [3.7 Labs](#37-labs) · [3.8 Orders](#38-orders) · [3.9 Medications](#39-medications) · [3.10 Encounter class](#310-encounter-class) · [3.11 SUD determinations](#311-sud-determinations) · [3.12 Elixhauser comorbidities](#312-elixhauser-comorbidities)
4. [Lab processing, step by step](#4-lab-processing-step-by-step)
5. [Medication flags in detail](#5-medication-flags-in-detail)
6. [Site differences at a glance](#6-site-differences-at-a-glance)
7. [Caveats for analysts](#7-caveats-for-analysts)
8. [Appendix — value labels and code maps](#8-appendix--value-labels-and-code-maps)

---

## 1. Reading this dictionary

**Grain.** One row per `(person_id, visit_occurrence_id, time)`, `time` to the minute; the key is unique. Rows are sorted by `visit_occurrence_id`, then `time`.

**What a row is.** A time point at which something was recorded for the encounter: an ADT segment starting, a vitals set, a lab result, an oxygen-device chart, an order, a medication administration. A column that was not recorded at that time point is missing. Nothing is carried forward between time points except where a column says so (`loc_cat`, the encounter-level columns, and `bmi` within vitals rows).

**Encounter-level columns** (demographics, outcomes, encounter class, SUD determinations, Elixhauser, quality flags) carry the same value on every row of the encounter.

**Two kinds of flag.**

| Kind | Values | Columns | How to test |
|---|---|---|---|
| Event flag — something happened at this time point | `1` or missing (never `0`) | devices, orders, medications | `col == 1` or `col.notna()` |
| Encounter flag | `0` / `1`, never missing | `died_ever`, `died01`, `enc_*`, `icd_*`, `nlp_*`, `sud_*`, `dx_sud_*`, `elix_*`, `single_vitals_set`, `has_vitals_gap`, `long_encounter` | `col == 1` |

**Storage types.** Identifiers are `int64`. A 0/1 column with no missing value is `int8`. Every other numeric column is `float32` (values are flags or measurements carried to at most 3 decimals, so nothing meaningful is lost). Time columns are `datetime64`. `visit_source_prefix` is a string.

**Coverage.** Where a table gives "% enc MCW / UW", it is the share of the site's **cohort encounters** with at least one value in that domain's frame — 502,636 at MCW and 190,499 at UW (all `visit_occurrence` encounters after the visit-type exclusions) — as reported in the build notebooks (Audits 5a, 7b, 9). Medication coverage is the share of **master encounters** (Audit 12d(ii)). `—` means the column is empty at that site.

**Names.** `cfg.X` is setting `X` in `pipeline_config.py`; a module name (`pipeline_timeseries.py`, …) is the file that applies the rule. Every rule below is applied identically at both sites unless the text says *MCW* or *UW*; site differences are always differences in the source data (concept ids, unit labels, code formats, where a construct is charted), held in `cfg.*_BY_SITE` settings.

---

## 2. How a row is built

The pipeline follows the lab's UW–Madison Stata master-data pipeline, re-sourced from OMOP. Both sites run the same modules and the same notebook steps; the site is chosen by `OMOP_SITE` in Step 1.

| Step | What it produces | Source tables | Grain |
|---:|---|---|---|
| 1 | Load every table once; drop excluded visit types | all | — |
| 2 | Demographics, outcomes, encounter class, SUD determinations | `person`, `visit_occurrence`, `death`, site cohort file, `condition_occurrence` | encounter |
| 3 | Elixhauser comorbidities (current and prior) | `condition_occurrence` | encounter |
| 4 | Location category (`loc_cat`) | `visit_detail` | encounter × time |
| 5 | Vitals, GCS, AVPU, orientation, anthropometrics | `measurement`; mental status MCW `measurement` 40493498, UW `observation` | encounter × time |
| 6 | Oxygen devices | `device_exposure` | encounter × time |
| 7 | Labs | `measurement` | encounter × time |
| 8 | Nursing scales | `measurement`, `observation` | encounter × time |
| 9 | Orders (imaging, ECG, echo, transfusion) | `procedure_occurrence`, `device_exposure` | encounter × time |
| 10 | Medication flags | `drug_exposure` (administration records) | encounter × time (UW: × day) |
| 11 | Master assembly and derived columns | Steps 2–10 | encounter × time |
| 12 | Final audit | — | — |
| 13 | Save in the shared schema | — | — |

**Cohort.**

1. **Visit types.** UW's outpatient (9202; 2,396), laboratory (32036; 353) and generic hospital (38004515; 3,836) visits, and 4 encounters of visit types that map to no encounter class, are dropped before anything is built — 6,589 of 197,088 UW encounters (`cfg.VISIT_CONCEPT_EXCLUDE_BY_SITE`, `cfg.EXCLUDE_UNMAPPED_VISIT_TYPES`). MCW's exclusion list is empty.
2. **Adults.** Age at encounter start ≥ 18 (`cfg.MIN_AGE`).
3. **A time series.** The encounter must carry at least one *core vital sign* — HR, RR, SBP, DBP, SpO₂, temperature, AVPU or orientation (`VITAL_SIGN_COLS` in `pipeline_master.py`; `cfg.REQUIRE_VITALS`). Encounters without one are left out: 48,627 at MCW (9.7% of the cohort) and 3,274 at UW (1.7%).
4. **Stata rules switched off, flagged instead.** The Stata inpatient restriction (`loc_cat` ∈ ward/ICU/IMC; `cfg.APPLY_INPATIENT_FILTER`), the single-vitals-set drop (`cfg.REQUIRE_MULTIPLE_VITALS`) and the ≥24 h vitals-gap drop (`cfg.APPLY_VITALS_GAP_FILTER`) are all **off**: ED-only encounters are the population this project is about. The last two are carried as flags (§3.4) so they can be applied at analysis time.

**Rules shared by every time-varying domain.**

- **Timestamps** are floored to the minute (`cfg.TIME_RESOLUTION`). Rows with no usable timestamp are dropped. One exception: UW medication administrations carry a date but no time, so they fall back to 00:00 of that date (`cfg.TIME_FALLBACK_TO_DATE_BY_SITE`).
- **One value per variable per minute.** When two rows give the same variable at the same minute, the first is kept, as Stata's `duplicates drop …, force` does (most labs lose under 1.5% of values this way; `co2`, which pools serum and blood-gas concepts, loses the most — 5.8% at MCW, 8.3% at UW). Mental status, devices and medications instead keep the **worst** value at the minute (highest AVPU, least oriented, any device / medication present).
- Rows that carry no value after cleaning are dropped.

**Master assembly** (`pipeline_master.build_master`).

1. ADT segments and vitals are joined on the minute (outer join); `loc_cat` is filled forward, then backward where the next segment is ward, ICU, ED or IMC.
2. The has-vitals rule keeps encounters with at least one core vital; `first_vitals_time`, `last_vitals_time` and the quality flags are computed from the core-vitals time points.
3. Devices, nursing, labs, orders and medications are **outer-joined** on the minute (`cfg.MASTER_DOMAIN_JOIN`): a row merges into an existing time point when the minute matches and adds a new time point otherwise. Only encounters already in the master are joined, and only rows inside the encounter's visit window widened by 24 hours on each side (`cfg.DOMAIN_WINDOW_HOURS`). Rows at or before an encounter's last TAC/EMH ("drop") ADT segment are removed.
4. Day-resolution rows (medications at UW, date-only orders) stamped 00:00 *before* the encounter's first time point are moved onto that first time point (`cfg.DATE_GRAIN_DOMAINS`), so the admission day does not open a row hours before arrival.
5. Demographics are joined (inner join), then the derived columns are computed (`hours_since_admit`, `fio2_final`, `noninvasive_vent`, `died01`, `day_of_week`, `long_encounter`), Elixhauser flags are joined (missing → 0) and `site` is stamped.
6. `downcast_master` stores the frame compactly (§1) and `harmonize_schema` puts the columns in the shared order, adding empty any shared column the site cannot produce (MCW: `dialysis`; UW: `sao2`, `dialysis`, `bands_pct`). The save cell stops if a column outside the shared list appears.

---

## 3. Column reference

### 3.1 Identifiers, time and location

| # | Column | Type | Values | Definition and processing |
|---|---|---|---|---|
| 1 | `site` | int | `1` = MCW, `5` = UW Madison | Stamped from `cfg.SITE_CODE`; the Stata site coding. |
| 2 | `person_id` | int64 | OMOP `person_id` | Patient identifier from the site's OMOP delivery. **Direct identifier** — not for display. |
| 3 | `visit_occurrence_id` | int64 | OMOP `visit_occurrence_id` | Encounter identifier; joins back to `visit_occurrence` (checked in Audit 13). **Direct identifier.** |
| 4 | `time` | datetime | minute | The time point. Source timestamps floored to the minute; UW medications at 00:00 of the administration day (§3.9). |
| 5 | `hours_since_admit` | float | hours, may be negative | `(time − first_vitals_time)` in hours. Anchored on the **first core vital sign, not on registration**: rows before it (ED/triage ADT segments, labs drawn before the first vitals set) are negative. |
| 6 | `loc_cat` | float | `1` ward · `2` ICU · `3` OR · `4` ED · `6` procedural · `8` IMC/step-down · `99` unclassified | Location at this time point. Each `visit_detail` segment is classified from `visit_detail_source_value` by `cfg.LOC_DEPT_PATTERNS` (first match wins, most specific first), falling back to `visit_detail_concept_id` (`cfg.LOC_CONCEPT_FALLBACK`). Stata cleaning sequence: drop everything up to the last "drop" segment (TAC / EMH / transitional care / employee health, `-1`); drop ED segments after the first inpatient segment; drop an inpatient segment sandwiched between OR and another inpatient unit for ≤ 1 h; at one minute keep the most acute (ICU > IMC > ward > OR > procedural > ED). In the master the value is carried forward, then backward where the next segment is ward/ICU/ED/IMC. Missing where the encounter has no ADT segment (possible at MCW). Unclassified: 0.0% MCW, 0.18% UW. |
| 7 | `first_vitals_time` | datetime | encounter-level | Earliest time point carrying a core vital sign (HR, RR, SBP, DBP, SpO₂, temperature, AVPU, orientation). Anchor of `hours_since_admit`. |
| 8 | `last_vitals_time` | datetime | encounter-level | Latest such time point. `died01` is placed here. |
| 161 | `day_of_week` | int | `0` Monday … `6` Sunday | Day of week of `time`. |

### 3.2 Demographics and outcomes

All encounter-level, built in Step 2 (`pipeline_encounter.build_demographics`). Value labels are verbatim from the lab's Stata harmonised label defines (§8).

| # | Column | Type | Values | Definition and processing |
|---|---|---|---|---|
| 9 | `sex` | int | `0` declined/unknown · `1` female · `2` male | `person.gender_concept_id` via `cfg.SEX_MAP` (8532 → 1, 8507 → 2, else 0). |
| 10 | `race` | int | `0` declined/unknown · `1` AI/AN · `2` Asian · `3` Black · `4` NHPI · `5` White · `6` other | `person.race_concept_id` via `cfg.RACE_MAP`; unmapped → 0. The Stata labels also define `7` multiracial and `8` Hispanic, but no OMOP race concept maps to them, so they never occur; ethnicity is its own column. |
| 11 | `ethnicity` | int | `0` declined/unknown · `1` Hispanic/Latino · `2` non-Hispanic | `person.ethnicity_concept_id` via `cfg.ETHNICITY_MAP` (38003563 → 1, 38003564 → 2). |
| 12 | `age` | float | years, 18–86 | Whole years at encounter start: `floor((visit_start_datetime − birth_datetime) / 365.25)`, or calendar-year difference when no birth datetime. Encounters under 18 are excluded; ages above 86 are top-coded to 86 (`cfg.MIN_AGE`, `cfg.AGE_TOP_CODE`), so 86 means "86 or older". |
| 13 | `died_ever` | int | 0 / 1 | **Patient-level death placed on one encounter — the Stata rule, not in-hospital mortality.** A patient "died" if the `death` table has a row for them or any of their encounters was discharged Expired. The flag is set on that patient's **last encounter by start time**, whenever the death happened, and on any encounter discharged Expired (`disch_disp = 6`). Timing of these deaths against the encounter (Audit 2h(ii)): during the encounter or Expired discharge 12.4% MCW / 16.0% UW; 1–30 days after discharge 13.9% / 12.3%; 31–365 days 31.1% / 26.2%; more than a year later **42.3% / 45.4%**. In the master: 13,929 encounters (3.07%) at MCW and 13,147 (7.02%) at UW. |
| 14 | `died01` | int | 0 / 1 | `1` on the single row where `time == last_vitals_time` of an encounter with `died_ever = 1`; `0` everywhere else. Inherits the patient-level meaning of `died_ever`. |
| 125 | `disch_disp` | float | `1`–`8`, or missing | Discharge disposition from `visit_occurrence.discharge_to_source_value`, Stata coding: `1` acute-care transfer · `2` hospice · `3` home/self-care · `4` home health · `5` facility (SNF, rehab, nursing home, assisted living, residential) · `6` expired · `7` other/unknown/still hospitalised · `8` against medical advice. *MCW* — two-letter prefix (`HO:Home / Self Care` → `HO` → 3; `cfg.DISCH_DISP_MAP`). *UW* — numeric NUBC/UB-04 status code (`01` → 3, `20` → 6, …; `cfg.DISCH_DISP_MAP_UW`), and for the 15.8% of rows with no code, ordered text patterns on the description (`cfg.DISCH_TEXT_PATTERNS_UW`; hospice is tested before home). Anything unresolved stays missing rather than being called "other". Maps in §8. |
| 126 | `death_date` | datetime | date, or missing | `death.death_date` for the patient (first row per patient), on every encounter of a patient with a death record. Missing for patients with no death record, including those flagged only by an Expired discharge. **A calendar date of a patient event** — not for display. |

### 3.3 Vitals and mental status

From `measurement` (Step 5, `pipeline_timeseries.build_vitals`). Concept ids are the same at both sites except where two are given. Values outside `cfg.VITAL_RANGES` are set to missing after conversion.

| # | Column | Unit / values | Concepts (MCW · UW) | Processing | % enc MCW / UW |
|---|---|---|---|---|--:|
| 15 | `hr` | beats/min | 3027018 | Kept 1–300. | 89.99 / 94.85 |
| 16 | `rr` | breaths/min | 3024171 | Kept 1–70. | 89.53 / 96.97 |
| 17 | `sbp` | mmHg | 3004249 | Kept 30–300. At UW systolic and diastolic share one source string and are told apart only by concept. | 90.01 / 98.15 |
| 18 | `dbp` | mmHg | 3012888 | Kept 1–250. | 90.01 / 98.15 |
| 19 | `o2sat` | % | 40762499 (SpO₂), 3016502 (O₂ saturation, blood gas) | Kept 11–100. | 86.24 / 95.60 |
| 20 | `temp_c` | °C, 1 decimal | 3020891 | Both sites chart Fahrenheit. Values already in 32–44 are taken as °C; others are converted `(F − 32) × 5/9`; rounded to 1 decimal; kept 32–44. | 89.62 / 95.58 |
| 21 | `avpu` | `0` alert · `1` voice · `2` pain · `3` unresponsive | MCW: catch-all concept 40493498 · UW: `observation` concepts 21494983, 21494988, 36031661, 40757642, 3018668 | **Charted AVPU first, worst value at the minute.** *MCW* — the catch-all holds twelve instruments with different scales; each row is routed by its flowsheet row name (`measurement_source_value`) and scored by that instrument's own mapper (`catchall_40493498.py`): Arousal Level, level-of-consciousness / mental-status text, "Rate patient's altered level of consciousness", NIHSS 1a (already 0–3) and "Consciousness" (an inverted 2/1/0 scale, flipped). CIWA visual, CAM-ICU, NIHSS visual, speech and behaviour rows are routed away and never reach AVPU. *UW* — only the Stata flowsheet rows (`FEATURE 4: ALTERED LEVEL OF CONSCIOUSNESS (RETIRED)`, `MENTAL STATUS`, `LEVEL OF CONSCIOUSNESS`, `RATE PATIENT'S LEVEL OF CONSCIOUSNESS (4)`, `VISUAL CHECK`), scored with the Stata regexes (`cfg.UW_AVPU_PATTERNS`): alert / WDL / easily aroused → 0; to voice / drowsy / lethargic → 1; to pain or stimulus / difficult to arouse / stupor → 2; eyes do not open / unresponsive / coma / unarousable / paralyzed → 3; the worst term in a response wins. **Then the Stata GCS-eye rule**: GCS eye 4/3/2/1 sets AVPU 0/1/2/3 where AVPU is missing *or less severe*. Share of values that were charted rather than taken from GCS eye: 59.2% MCW, 91.7% UW. | 58.24 / 37.55 |
| 22 | `disoriented` | `0` oriented · `1` disoriented | MCW: catch-all 40493498 orientation rows · UW: `observation` row `ORIENTATION LEVEL` | Orientation is charted per domain (person, place, time, situation), often a minute or two apart, so each domain is gathered over a **2-minute backward window** (Stata `rangestat`). `0` when "oriented ×4" is charted or 3–4 domains are positive, with no negative domain; `1` when "disoriented ×4" or "confused" is charted, any domain is negative, or only 0–2 domains are positive. *MCW* also reads `CONFUS`/`DISORIENT` in mental-status text as not oriented (Stata rule). Where still missing, **GCS verbal** decides: 5 → 0; 3–4 → 1 (3–4 also overrides a 0). | 58.82 / 32.09 |
| 23 | `gcs_eye` | 1–4 | 3016335 | Numeric value; else the leading integer of the text (`4-->(E4) SPONTANEOUS` → 4); else a text map (`spontaneous` 4, `to speech/voice` 3, `to pain` 2, `none` 1). Kept 1–4. | 53.81 / 16.86 |
| 24 | `gcs_motor` | 1–6 | 3008223 | As `gcs_eye`; text map `obeys commands` 6 … `none` 1. Kept 1–6. | 53.79 / 16.85 |
| 25 | `gcs_verbal` | 1–5 | 3009094 | As `gcs_eye`; text map `oriented` 5 … `none` 1. Kept 1–5. *UW*: intubated patients are scored 1 with no "T" modifier (57.5% of verbal = 1 rows coincide with mechanical ventilation) — condition on `vent`/`trach`. | 53.77 / 16.84 |
| 26 | `gcs_total` | 3–15 | 3007194 | Delivered total (leading integer of the text where needed). Kept 3–15 — a 0 is a placeholder, not a score. Not recomputed from the components. | 53.69 / 16.83 |
| 27 | `height_cm` | cm, 1 decimal | 3036277 | Delivered in inches at both sites despite the cm concept: values 20–100 are converted ×2.54 (`cfg.HEIGHT_TO_CM_BY_SITE`). Kept 100–250. Median 172.7 cm at both sites. | 37.38 / 37.11 |
| 28 | `weight_kg` | kg, 1 decimal | MCW 3025315 · UW 3013762 | *MCW* delivers **pounds**: 40–1,000 lb ×0.45359237. *UW* delivers **ounces**: 700–20,000 oz ×0.028349523 (`cfg.WEIGHT_TO_KG_BY_SITE`). Kept 20–400. Median 80.7 kg at both sites. | 47.55 / 53.05 |
| 29 | `bmi` | kg/m², 1 decimal | 3038553 | Delivered BMI is never overwritten. Where it is missing, `weight_kg / (height_cm/100)²`, with height and weight each **carried forward then backward within the encounter** first (they are almost never charted at the same minute); inputs must be 100–250 cm and 20–400 kg. Kept 10–100. Present on vitals rows only, so the row-level distribution is person-time weighted (per-encounter median 27.1 at MCW). | 44.12 / 37.75 |
| 30 | `bmi_derived` | `1` computed · `0` delivered · missing = no BMI | — | Provenance of `bmi` at that row. | 44.12 / 37.75 |

`fio2` (MCW 3026238 · UW 4353936) and `o2flow` (MCW 3014080, 3005629 · UW 4141684) are built here too — FiO₂ given as a fraction (0.21–<1) is ×100 and rounded; an FiO₂ value ≤ 15 in 0.5 steps fills a missing O₂ flow (flow charted in the FiO₂ row); FiO₂ kept 21–100, flow 0–60 L/min — but they are **not** delivered: they feed `fio2_final` (§3.5) and are dropped. Coverage: FiO₂ 0.28% / 7.50%, O₂ flow 0.60% / 19.04% of encounters (MCW / UW). MCW's sparsity was confirmed with the site as genuine.

### 3.4 Encounter quality flags

The first three are computed at assembly over the core-vitals time points, before the other domains join; `long_encounter` over all of the encounter's rows at the end. None of them drops anything — filter at analysis time.

| # | Column | Values | Definition | Encounters flagged |
|---|---|---|---|---|
| 31 | `single_vitals_set` | 0 / 1 | `1` when every core vital of the encounter sits at one minute (`first_vitals_time == last_vitals_time`): a single moment, no trajectory. Exclude with `master[master.single_vitals_set == 0]`. | 100,312 MCW · about 17,500 UW (build log) |
| 32 | `max_vitals_gap_h` | float, hours (2 decimals) | Largest gap between consecutive core-vitals time points in the encounter. `0` when all vitals share one minute. | — |
| 33 | `has_vitals_gap` | 0 / 1 | `1` when `max_vitals_gap_h ≥ 24` (`cfg.VITALS_GAP_HOURS`) — the Stata rule that treats such an encounter as two stays. Exclude with `master[master.has_vitals_gap == 0]`. | 28,273 MCW · 1,021 UW |
| 34 | `long_encounter` | 0 / 1 | `1` when the encounter's rows span more than 90 days, first to last time point (`cfg.LONG_ENCOUNTER_DAYS`). Most are genuine long `visit_occurrence` records (UW: 103 of 108); at MCW most are long visit windows with few records. Exclude with `master[master.long_encounter == 0]`. | 1,220 MCW · 108 UW |

### 3.5 Respiratory support

From `device_exposure` (Step 6, `pipeline_timeseries.build_o2device`). Device rows are point charts: a start time and no end time.

1. **Which rows.** *MCW* rows written `Oxygen Device:<text>`; the text after the colon is matched. *UW* rows written as the bare device name (`Nasal Cannula`, `Trach Mask`), i.e. rows with no colon that name an oxygen device; UW's coded non-oxygen rows (`CODE:VOCABULARY:…` — IV pumps, compression devices, HCPCS supplies) are ignored (`cfg.O2_DEVICE_ROW_FORMAT_BY_SITE`).
2. **Patterns** (`cfg.O2_DEVICE_PATTERNS`, upper-cased text): `vent` VENTILATOR · MECHANICAL VENT · ETT · ENDOTRACHEAL; `trach` TRACH · TRACH MASK · TRACHEOSTOMY · OXYTRACH; `bipap` BIPAP · BI-PAP · NON-INVASIVE VENT / POSITIVE; `hfnc` HIGH FLOW · HFNC · OPTIFLOW · VAPOTHERM; `cpap` CPAP · SUPERNOVA; `nasal_cannula` NASAL CANNULA; `roomair` ROOM AIR · ROOM · NONE (as the whole text); `other_o2device` OXYMASK · NON-REBREATHER · FACE MASK · VENTURI / VENTI MASK · AEROSOL · FACE TENT · T-PIECE · POM MASK · BAG VALVE MASK / BVM · OTHER. A minute with several device rows, or a multi-device string (`Nasal Cannula;Room Air`), sets every device it names.
3. **Up-coding** (Stata hierarchy, `cfg.O2_DEVICE_PRECEDENCE`): **trach > BiPAP > HFNC > CPAP > nasal cannula > other > room air > vent** — exactly one device survives per minute (Audit 6a: 0 rows with more than one).

| # | Column | Values | Definition |
|---|---|---|---|
| 35 | `vent` | 1 / missing | Mechanical ventilation charted at this minute **and no other device**. Vent is the lowest rank in the Stata hierarchy, so `Mechanical Ventilation;CPAP` resolves to CPAP and a ventilated tracheostomy patient charted as such appears under `trach`. |
| 36 | `trach` | 1 / missing | Tracheostomy device (trach mask, OxyTrach). Highest rank. |
| 37 | `hfnc` | 1 / missing | High-flow nasal cannula. `High Flow Nasal Cannula` also matches the nasal-cannula pattern; up-coding keeps HFNC. |
| 38 | `nasal_cannula` | 1 / missing | Standard nasal cannula. |
| 39 | `roomair` | 1 / missing | Room air (no supplemental oxygen) and nothing ranked higher at the minute. *UW*: room air is charted almost only beside another device, so `roomair` is set on 48 rows. |
| 40 | `other_o2device` | 1 / missing | Masks, non-rebreather, Venturi/Venti mask, aerosol, face tent, T-piece, POM mask, bag-valve mask, and UW's `Other`. |
| 160 | `noninvasive_vent` | 1 / missing | `max(bipap, cpap)` — BiPAP and CPAP are combined here and not delivered separately. |
| 159 | `fio2_final` | %, integer | Best available FiO₂ at the minute, in order: measured FiO₂ (§3.3); else from O₂ flow, `4 × flow + 21`, capped at 70; else `21` when `roomair = 1`; rounded to a whole number. Missing otherwise. At MCW measured FiO₂ and flow are sparse, so most values are the room-air 21; at UW they come from measured FiO₂ and flow, since room air is almost never charted alone. |

Device mix (share of device time points; Audit 6): **MCW** (3,029,486 time points, 234,669 encounters) room air 67.9%, nasal cannula 15.8%, vent 6.0%, trach 3.5%, HFNC 2.8%, other 2.4%, BiPAP 1.2%, CPAP 0.5%. **UW** (1,413,262 time points, 40,732 encounters; device file dated 7 Oct) nasal cannula 41.7%, vent 33.7%, other 6.7%, trach 6.5%, CPAP 4.4%, BiPAP 4.0%, HFNC 2.9%, room air 48 rows.

### 3.6 Nursing

| # | Column | Values | Definition |
|---|---|---|---|
| 41 | `dialysis` | 1 / missing — **empty at both sites** | Dialysis flowsheet rows matched by text (`DIALYSIS`, `DIALYSATE`, `ULTRAFILTRATION`, `TRANSMEMBRANE PRESSURE`). Rows whose source value is a diagnosis, procedure or lab code (`Z99.2:Dependence on renal dialysis`, CPT `4054F`, LOINC lab names) are excluded first (`cfg.NURSING_CODE_ROW_PATTERN`) — a dialysis-status diagnosis is not a bedside observation. Neither delivery has dialysis flowsheet rows, so the column is empty; it is kept for schema compatibility. |

The nursing step also looks for Braden, CIWA-Ar, AUDIT-C, central line, Foley, LVAD and urine output (`cfg.NURSING_SOURCE_PATTERNS`). None is in either OMOP extract as a flowsheet instrument (Audit 8b), and none is in the shared schema.

### 3.7 Labs

From `measurement` (Step 7, `pipeline_timeseries.build_labs`). The full order of operations is in §4. "Rules beyond the shared steps" lists what applies to that analyte in addition to the steps every lab goes through (censored text, non-result rows, first value per minute, rounding). Decimals `—` = not rounded. Every conversion fires only on the declared unit; "guarded" means only when the value is implausible before and plausible after (§4).

| # | Column | Unit | Decimals | MCW concepts | UW concepts | Rules beyond the shared steps | % enc MCW / UW |
|--:|---|---|:-:|---|---|---|--:|
| 42 | `albumin` | g/dL | 1 | 3024561, 40763912 | 3024561, 40763913, 3028286 | 0 → missing; kept 0.5–7.0; convert: g/L ×0.1; mg/dL or blank ×0.001 — guarded | 24.42 / 36.37 |
| 43 | `alk_phos` | U/L | 0 | 3035995 | 3035995 | 0 → missing; convert: µkat/L ×60 | 23.40 / 28.88 |
| 44 | `ammonia` | µmol/L † | 0 | 3011958 | 3011958 | `<x` → x/2; 0 → missing | 1.48 / 2.93 |
| 45 | `bands_pct` | % | 0 | 3004809 | — (not in UW map) | kept 0–100 | 0.78 / — |
| 46 | `basophils_pct` | % | 0 | 3009797, 3022096, 3013869 | 3013869, 37398606, 3009797, 3027651 | MCW: where missing, 100 × absolute count / `wbc` (absolute concepts MCW 3013429, 3006315, 3027651); kept 0–100 | 35.90 / 54.18 |
| 47 | `bili_total` | mg/dL | 1 | 3024128, 3028833 | 4230543, 3024128 | convert: µmol/L ×0.05844 | 24.34 / 38.51 |
| 48 | `blasts_pct` | % | 0 | 3023643, 3025159 | 3023643 | kept 0–100 | 0.07 / 0.04 |
| 49 | `blood_alcohol` | g/dL | — | 3012920 | 3012920 | `<x` → x/2; `NOT DETECTED`/`BELOW DETECTION`/`NEG…` → 0.005; convert: mg/dL ×0.001, mmol/L ×0.004607 — guarded | 9.98 / 12.28 |
| 50 | `bun` | mg/dL | 0 | 3013682, 3004295 | 3013682 | 0 → missing; convert: mmol/L urea ×2.8 | 38.24 / 62.36 |
| 51 | `calcium` | mg/dL | 1 | 3006906, 3032503 | 3006906 | 0 → missing; convert: mmol/L ×4.008 | 40.22 / 60.00 |
| 52 | `chloride` | mmol/L | 0 | 3014576, 3018572, 3035285 | 3014576, 2212265, 3018572 | 0 → missing | 41.90 / 62.16 |
| 53 | `co2` | mmol/L | 0 | 3016293, 3014094, 3008152, 3027273, 3025817, 3006576, 3009609, 3010140 | 3015632, 3008152, 3027273, 3014094 | 0 → missing | 41.91 / 62.19 |
| 54 | `creatinine` | mg/dL | 1 | 3016723, 3051825 | 3016723, 4324383, 3051825 | 0 → missing; convert: µmol/L ×0.011312 | 41.99 / 62.73 |
| 55 | `crp` | mg/dL † | 1 | 3020460 | 3020460 | `<x` → x/2 | 1.49 / 2.72 |
| 56 | `d_dimer` | mg/L FEU | — | 3051714, 4322628 | 3051714 | `<x` → x/2; convert: ng/mL or µg/L ×0.001; blank unit ×0.001 — guarded | 2.02 / 2.88 |
| 57 | `eosinophils_pct` | % | 0 | 3010457, 3006504 | 3010457, 37399255, 3015956, 3012610 | MCW: where missing, 100 × absolute count / `wbc` (absolute concepts MCW 3028615, 3013115, 3009932); kept 0–100 | 35.77 / 54.24 |
| 58 | `esr` | mm/h | 0 | 3013707, 3015183, 42528622, 3045567 | 3015183, 3013707 | `<x` → x/2 | 1.29 / 2.59 |
| 59 | `ferritin` | ng/mL | — | 3001122, 3015242 | 3001122 | 0 → missing; convert: pmol/L ×0.445 | 1.67 / 1.40 |
| 60 | `fibrinogen` | mg/dL | — | 3016407, 3016628 | 3016407 | — | 0.87 / 1.69 |
| 61 | `gluc_poc` | mg/dL | 0 | 3014053, 3000483, 3034962 | 3034962, 4230393 | 0 → missing; convert: mmol/L ×18.016 | 14.46 / 17.67 |
| 62 | `gluc_ser` | mg/dL | 0 | 3004501 | 3004501, 3000483 | 0 → missing; convert: mmol/L ×18.016 | 39.96 / 62.31 |
| 63 | `hb` | g/dL | 1 | 3000963, 40758903, 3006239, 3027484 | 3000963 | 0 → missing; convert: g/L ×0.1, mmol/L ×1.611 | 41.92 / 61.50 |
| 64 | `hct` | % | 1 | 3023314, 3009542, 3034976 | 3023314 | 0 → missing; convert: fraction ×100 | 41.93 / 61.74 |
| 65 | `inr` | ratio | 1 | 3022217, 3032080, 3051593 | 3022217 | 0 → missing | 9.49 / 15.55 |
| 66 | `lactate` | mmol/L | 1 | 3014111, 3047181, 3008037, 4152980 | 3047181 | 0 → missing | 6.38 / 15.23 |
| 67 | `ldh` | U/L | — | 3022250, 3016436 | 3022250, 3016436 | 0 → missing | 1.45 / 2.40 |
| 68 | `lipase` | U/L | 0 | 3004905 | 3004905 | `<x` → x/2; 0 → missing | 10.93 / 9.21 |
| 69 | `lymphocytes_pct` | % | 0 | 3037511, 3002030 | 3037511, 3038058, 3002006, 3003540 | MCW: where missing, 100 × absolute count / `wbc` (absolute concepts MCW 3004327, 3019198, 3003215, 3028022); kept 0–100 | 35.94 / 54.25 |
| 70 | `magnesium` | mg/dL | 1 | 3001420 | 3001420 | 0 → missing; convert: mmol/L ×2.431 | 13.01 / 26.68 |
| 71 | `mcv` | fL | 1 | 3023599, 3024731 | 3023599, 3024731 | 0 → missing | 41.83 / 61.31 |
| 72 | `monocytes_pct` | % | 0 | 3011948, 3019069 | 3011948, 37393321, 3022407 | MCW: where missing, 100 × absolute count / `wbc` (absolute concepts MCW 3033575, 3001604, 3034107); kept 0–100 | 35.92 / 54.21 |
| 73 | `neutrophils_pct` | % | 0 | 3008342, 3018010 | 3008342, 3027368, 3030945 | both sites: where missing, 100 × absolute count / `wbc` (absolute concepts MCW 3013650, 3017732, 3017501; UW 3013650, 37398605, 3017501); kept 0–100; convert: absolute /µL ×0.001 before the back-fill | 35.75 / 54.35 |
| 74 | `pco2_art` | mmHg | 0 | 3027946 | 3027946 | 0 → missing; kept 5–200 | 2.48 / 5.18 |
| 75 | `pco2_ven` | mmHg | — | 3021447 | 3021447 | 0 → missing; kept 5–200 | 2.08 / 6.38 |
| 76 | `ph_art` | pH | 2 | 3019977 | 3019977 | 0 → missing | 2.48 / 5.18 |
| 77 | `ph_ven` | pH | — | 3012544 | 3012544 | 0 → missing | 2.48 / 6.39 |
| 78 | `phosphate` | mg/dL | 1 | 3011904 | 3011904, 4017907 | 0 → missing; convert: mmol/L ×3.097 | 7.54 / 16.29 |
| 79 | `platelet_count` | K/µL | 0 | 3024929, 3007461 | 3024929, 4267147, 3042234 | 0 → missing | 41.84 / 61.35 |
| 80 | `po2_art` | mmHg | 0 | 3027801 | 3027801 | 0 → missing | 2.48 / 4.95 |
| 81 | `po2_ven` | mmHg | — | 3024354 | 3024354 | 0 → missing | 2.02 / 6.38 |
| 82 | `potassium` | mmol/L | 1 | 3023103, 3005456, 3041354 | 3023103, 3005456 | 0 → missing | 41.78 / 62.15 |
| 83 | `procalcitonin` | ng/mL | — | 44817130, 3046279 | 3046279 | `<x` → x/2 | 1.14 / 2.94 |
| 84 | `ptt` | s | 1 | 3018677, 3041232 | 3018677, 3044009 | 0 → missing | 5.44 / 7.52 |
| 85 | `rdw` | % | 1 | 3019897, 3002385 | 3019897, 4281085 | 0 → missing; kept 8–40; rows with unit `FL` dropped | 41.82 / 61.29 |
| 86 | `sao2` | % | — | 3028653 | — (not in UW map) | 0 → missing; rows with unit `G/DL` dropped | 0.70 / — |
| 87 | `sgot` | U/L | 0 | 3013721, 36305398, 3037081, 3005435 | 3013721, 3037081, 4263457 | 0 → missing; convert: µkat/L ×60 | 24.15 / 39.24 |
| 88 | `sgpt` | U/L | 0 | 3027388, 3005755, 3006923, 46235106 | 3006923, 3005755, 4146380 | 0 → missing; convert: µkat/L ×60 | 24.41 / 39.25 |
| 89 | `sodium` | mmol/L | 0 | 3019550, 3000285, 3041473 | 3019550, 3000285 | 0 → missing | 41.91 / 62.18 |
| 90 | `total_protein` | g/dL | 1 | 3020630 | 3020630, 4119650 | 0 → missing | 24.35 / 35.24 |
| 91 | `troponin` | ng/mL | 3 | 3019800, 3033745, 40769783 | 3032971, 3021337 | `<x` → x/2; kept 0–∞; `DELTA` source rows dropped; convert: ng/L or pg/mL ×0.001 (MCW high-sensitivity 40769783) | 11.70 / 12.37 |
| 92 | `wbc` | K/µL | 1 | 3000905, 3010813, 3046900 | 4298431, 3000905, 3010813, 3046900 | 0 → missing; >1000 → missing | 41.84 / 61.44 |

† Not harmonised: the delivered values sit on the conventional US unit (medians: ammonia 45 / 36, CRP 2.6 / 3.2 at MCW / UW). 

The absolute WBC differential counts (`*_abs`) are read only to back-fill the percentages and are not delivered.

### 3.8 Orders

From `procedure_occurrence` and `device_exposure` (Step 9, `pipeline_timeseries.build_orders`). The code is the part of the source value before the first colon, upper-cased, matched against CPT/HCPCS patterns (`cfg.ORDER_CPT_PATTERNS`). Event flags: `1` at the order time, missing otherwise.

- *UW* codes most orders in its own `CUSTOM` vocabulary with real order times and bills a minority under CPT4 as **date-only** rows. Imaging codes are normalised first (`R71046` → `71046`, `71046A` → `71046`; `cfg.ORDER_CODE_NORMALIZE_BY_SITE`), and UW-only codes are added (`cfg.ORDER_CODE_EXTRA_BY_SITE`): `EKG0008/0009/0014` → `ekg`; `ECH####` → `echo`; `07001/07032/07034` (CT angiography of the head) → CT head with contrast; `NURTRT0021` → `trans_rbc`; `BLB0017` → `trans_ffp`; `BLB0019` → `trans_plt`. The generic code `999997` is not matched.
- *MCW* uses CPT/HCPCS throughout; blood products also arrive as HCPCS P-codes in `device_exposure`, which is scanned too (at both sites).
- A date-only order (00:00) that duplicates a timed order of the same type in the same encounter is dropped (`cfg.ORDERS_DROP_DATE_ONLY_DUPLICATES`; 79,816 rows at UW). Remaining 00:00 rows before the encounter's first time point move onto it (§2).

| # | Column | Codes | % enc MCW / UW |
|---|---|---|--:|
| 93 | `echo` | Echocardiogram — CPT 93306–93308, 93312–93318, 93320, 93321, 93325, 93350, 93351 (UW also `ECH####`) | 3.30 / 6.13 |
| 94 | `ekg` | 12-lead ECG — CPT 93000, 93005, 93010, 93040–93042 (UW also `EKG0008/0009/0014`) | 26.24 / 40.48 |
| 95 | `trans_ffp` | Plasma transfusion — HCPCS P9017, P9023, P9059, P9060 (UW also `BLB0017`) | 0.50 / 0.08 |
| 96 | `trans_plt` | Platelet transfusion — HCPCS P9019, P9031–P9035, P9070–P9073 (UW also `BLB0019`) | 0.53 / 0.06 |
| 97 | `trans_rbc` | Red-cell transfusion — CPT 36430; HCPCS P9016, P9021, P9022, P9038–P9040 (UW also `NURTRT0021`) | 2.19 / 3.70 |
| 98 | `xr_abd` | Abdominal X-ray — CPT 74000–74009, 74020–74022 | 1.15 / 1.84 |
| 99 | `xr_chest` | Chest X-ray — CPT 71010, 71015, 71020–71022, 71045–71048 (71010/71020 are the pre-2018 codes) | 24.24 / 31.92 |
| 100 | `ct_head` | CT head, with or without contrast — `max` of with (70460, 70470, 70496; UW CT angiography codes) and without (70450) | 9.66 / 15.86 |
| 101 | `ct_neck` | CT neck, with (70491, 70492, 70498) or without (70490) contrast | 0.96 / 1.24 |
| 102 | `ct_chest` | CT chest, with (71260, 71270, 71275) or without (71250) contrast | 4.19 / 6.38 |
| 103 | `ct_abd` | CT abdomen, with (74160, 74170, 74175, 74177, 74178) or without (74150, 74176) contrast | 7.44 / 1.17 |
| 104 | `ct_contrast` | Any CT with contrast (head, neck, chest or abdomen) | 8.59 / 7.64 |
| 105 | `ct_nocontrast` | Any CT without contrast | 11.09 / 16.47 |

Cryoprecipitate (`trans_cryo`, P9012/P9013) is defined in the patterns but is not in the shared schema; neither delivery has such rows. If a future delivery does, the save cell will stop on a column outside the shared list.

### 3.9 Medications

From `drug_exposure` (Step 10, `pipeline_timeseries.build_meds`). Event flags: `1` at a time point with a qualifying administration, missing otherwise. Details, the full key lists and the list corrections are in §5.

- **Administration records only** (`drug_type_concept_id` 32818, `cfg.MED_DRUG_TYPES_BY_SITE`): MCW 12,843,521 of 12,940,551 rows (the 97,030 excluded rows are date-only, none a flagged medication); UW 27,382,180 of 29,059,656 (prescriptions excluded).
- **Time resolution differs by site.** *MCW*: the administration time, to the minute. *UW*: 26,959,445 administration rows carry a date only, so UW flags sit at **00:00 of the administration day** — and on the admission day, on the encounter's first time point (§2). A UW flag means "given on that day".
- **Key-list classes** (the consortium's shared lists, `cfg.MED_KEYS`): the cleaned medication name must match a list entry **exactly**, and the route must fit — `_iv` IV, `_po` oral/enteral, `_noniv` a known non-IV route; a row with no parseable route is accepted (`cfg.MED_ROUTE_WHEN_MISSING = "any"`; route parsed on 73.3% of MCW and 78.4% of UW rows).
- **Pattern classes** (no shared list; `cfg.MED_PATTERNS`): a regex on the raw `drug_source_value`, any route.

| # | Column | Class | Rule | % master enc MCW / UW |
|---|---|---|---|--:|
| 106 | `abx_iv` | Antibiotic, IV | key list + IV route | 7.77 / 12.14 |
| 107 | `abx_noniv` | Antibiotic, non-IV | key list + non-IV route | 5.84 / 9.33 |
| 108 | `aud_med` | Alcohol use disorder medication | pattern: naltrexone (not methylnaltrexone, not morphine-naltrexone), acamprosate, disulfiram | 0.47 / 0.75 |
| 109 | `benzodiazepine` | Benzodiazepine | pattern: lorazepam, midazolam, diazepam, alprazolam, clonazepam, chlordiazepoxide, temazepam, oxazepam, clorazepate, triazolam, clobazam | 11.87 / 21.25 |
| 110 | `mat_oud` | Medication for opioid use disorder | pattern: buprenorphine, methadone, naltrexone (not methylnaltrexone, not morphine-naltrexone) — all forms, including pain formulations | 1.08 / 2.09 |
| 111 | `naloxone` | Naloxone (reversal) | pattern: naloxone / Narcan, excluding buprenorphine-naloxone and pentazocine-naloxone | 0.62 / 1.32 |
| 112 | `nrt` | Nicotine replacement | pattern: NICOTINE (any product) | 2.93 / 7.11 |
| 113 | `opioid_iv` | Opioid, IV | key list + IV route | 13.96 / 22.56 |
| 114 | `opioid_noniv` | Opioid, non-IV | key list + non-IV route (includes the opioid-acetaminophen combinations; plain acetaminophen is **not** an opioid here) | 12.99 / 21.93 |
| 115 | `paralytic` | Neuromuscular blocker | pattern: rocuronium, vecuronium, cisatracurium, succinylcholine | 0.84 / 1.31 |
| 116 | `sedative_other_iv` | Non-benzodiazepine sedative, IV | key list + IV route (propofol, dexmedetomidine, ketamine, etomidate, methohexital, amobarbital, phenobarbital, midazolam) | 3.45 / 7.20 |
| 117 | `sedative_other_noniv` | Non-benzodiazepine sedative, non-IV | key list + non-IV route (z-drugs, sedating antihistamines, butalbital combinations, phenobarbital, …) | 2.62 / 4.73 |
| 118 | `sofa_card_dobutamine` | SOFA cardiovascular points | any dobutamine row → `2` | 0.16 / 0.20 |
| 119 | `sofa_card_dopamine` | SOFA cardiovascular points | dopamine rate (mcg/kg/min) ≤ 5 → `2`, ≤ 15 → `3`, > 15 → `4`; no rate → `2` | 0.04 / 0.21 |
| 120 | `sofa_card_epi_norepi` | SOFA cardiovascular points | epinephrine or norepinephrine rate ≤ 0.1 → `3`, > 0.1 → `4`; no rate → `3` | 2.23 / 3.63 |
| 121 | `steroid_iv` | Corticosteroid, IV | key list + IV route | 2.14 / 3.47 |
| 122 | `steroid_po` | Corticosteroid, oral | key list + oral/enteral route | 3.74 / 5.43 |
| 123 | `thiamine` | Thiamine | pattern: THIAMINE | 6.98 / 14.02 |
| 124 | `vasoactive` | Vasoactive agent | key list, any route (dopamine, ephedrine, epinephrine, midodrine, norepinephrine, phenylephrine, terlipressin, vasopressin) | 1.68 / 2.18 |

SOFA rates are read from `quantity` only where `dose_unit_source_value` is a mcg/kg/min unit (`cfg.RATE_UNIT_PATTERN`) — 26% of vasoactive rows at MCW, 7% at UW — so most rows take the lowest band, as the Stata does when the rate is missing. The SOFA columns hold the highest score at the time point.

### 3.10 Encounter class

Encounter-level (Step 2, `pipeline_encounter.build_encounter_class`).

| # | Column | Values | Definition |
|---|---|---|---|
| 127 | `enc_class` | `1` ED only · `2` inpatient | **Binary, by team decision**: inpatient includes direct admissions, ED-to-inpatient conversions and observation stays — "did the encounter stay beyond the ED?". *MCW* from the `visit_source_value` prefix (`ED` → 1; `IP`, `EI`, `OS`, `OB` → 2), concept as fallback; *UW* from `visit_concept_id` (9203 → 1; 9201, 262, 581385 → 2) (`cfg.ENC_CLASS_SOURCE_BY_SITE`). Cohort: 77.0% ED at MCW, 67.1% at UW; master: 81.8% and 66.6%. |
| 128 | `visit_source_prefix` | `ED` · `IP` · `EI` · `OS` · `""` | The four-way source class behind `enc_class`: ED only, direct inpatient, ED-to-inpatient, observation. Same meaning at both sites (UW's is derived from the concept id). |
| 129 | `enc_ed` | 0 / 1 | Source class ED only. |
| 130 | `enc_inpatient` | 0 / 1 | Source class direct inpatient admission. |
| 131 | `enc_ed_to_inpatient` | 0 / 1 | Source class ED-to-inpatient conversion. |
| 132 | `enc_observation` | 0 / 1 | Source class observation stay (none at UW). |
| 133 | `enc_any_inpatient` | 0 / 1 | `enc_class == 2`. |

### 3.11 SUD determinations

Encounter-level (Step 2). The project's SUD determinations come from each site's **cohort file**, not from OMOP: *MCW* `master_enc_lst_w_pred_prob_20260617.txt` (pipe-delimited, joined on `person_id` + `visit_occurrence_id`; the `_pred` suffix is dropped on load); *UW* the encounter crosswalk `cwalk_df` parquet (joined on `visit_occurrence_id`; columns renamed by `cfg.COHORT_RENAME_UW`). Every cohort encounter is in its site's file. The file is the screened-positive set (ICD **or** NLP), so it has no negative comparison group.

| # | Column | Values | Definition |
|---|---|---|---|
| 134 | `icd_alcohol` | 0 / 1 | ICD-rule alcohol use disorder (MCW `icd_alcohol_pred`; UW `ICD_Alcohol`). Blank in the file → 0. |
| 135 | `icd_opioid` | 0 / 1 | ICD-rule opioid use disorder (UW `ICD_Opioid`). |
| 136 | `icd_non_opioid` | 0 / 1 | ICD-rule other substance use disorder (UW `ICD_Other_Substance_Abuse`). |
| 137 | `nlp_alcohol` | 0 / 1 | NLP-model call from clinical notes (MCW `nlp_alcohol_pred`; UW `ALCOHOL_PRED`). Operating threshold 0.02 at both sites. |
| 138 | `nlp_opioid` | 0 / 1 | NLP call, opioid (UW `OPIOID_PRED`). |
| 139 | `nlp_non_opioid` | 0 / 1 | NLP call, non-opioid substance (UW `NON_OPIOID_PRED`). |
| 140 | `nlp_alcohol_prob` | float 0–1, or missing | NLP probability. **Missing means the model did not score the encounter** (ICD-only encounters), not a low score. |
| 141 | `nlp_opioid_prob` | float 0–1, or missing | As above, opioid. |
| 142 | `nlp_non_opioid_prob` | float 0–1, or missing | As above, non-opioid. |
| 143 | `sud_icd_any` | 0 / 1 | Any of the three `icd_*`. |
| 144 | `sud_nlp_any` | 0 / 1 | Any of the three `nlp_*`. |
| 145 | `sud_any` | 0 / 1 | `sud_icd_any` or `sud_nlp_any` (1 for every encounter in the file). |
| 146 | `sud_both` | 0 / 1 | `sud_icd_any` and `sud_nlp_any`. |
| 147 | `sud_alcohol_any` | 0 / 1 | `icd_alcohol` or `nlp_alcohol`. |
| 148 | `sud_opioid_any` | 0 / 1 | `icd_opioid` or `nlp_opioid`. |
| 149 | `sud_non_opioid_any` | 0 / 1 | `icd_non_opioid` or `nlp_non_opioid`. |

**Condition-table cross-check (`dx_sud_*`).** Built from `condition_occurrence` with the project code list `Substance_ICD_Codes.txt` (`cfg.SUD_CODE_FILE`: 342 codes in nine buckets, one of them a one-code `NOTSUD` placeholder that is skipped): the code before the first colon of `condition_source_value`, upper-cased with the decimal point removed, must match a listed code **exactly**; any condition row of the encounter counts, whatever its type. The build stops if the file is missing. It recovers 93.5% of the file's ICD flags at MCW and about 70% at UW (UW's ICD rule reads diagnoses, most likely billing codes, that `condition_occurrence` does not carry).

| # | Column | Values | Bucket |
|---|---|---|---|
| 150 | `dx_sud_alcohol` | 0 / 1 | Alcohol |
| 151 | `dx_sud_cannabis` | 0 / 1 | Cannabis |
| 152 | `dx_sud_cocaine` | 0 / 1 | Cocaine |
| 153 | `dx_sud_hallucinogens` | 0 / 1 | Hallucinogens |
| 154 | `dx_sud_opioid` | 0 / 1 | Opioid |
| 155 | `dx_sud_psychoactive_other` | 0 / 1 | Other psychoactive substance |
| 156 | `dx_sud_sedative_hypnotic` | 0 / 1 | Sedative / hypnotic |
| 157 | `dx_sud_stimulant` | 0 / 1 | Stimulant |
| 158 | `dx_sud_any` | 0 / 1 | Any of the eight buckets above (tobacco excluded) |

### 3.12 Elixhauser comorbidities

Encounter-level (Step 3, `elixhauser.py`), a verbatim transcription of the lab's Stata `elixhauser_dofile.do`: 31 comorbidities, each an ICD-9 and an ICD-10 regex applied to the code with its decimal point removed.

- **Vocabulary routing.** Where the source value names its vocabulary (UW `F10.20:ICD-10-CM:…`), ICD-9-CM rows meet only the ICD-9 patterns, ICD-10-CM rows only the ICD-10 patterns, and other vocabularies — SNOMED CT is 41% of UW rows — meet neither, so a numeric SNOMED code is never read as ICD-9. Where it does not (MCW `CODE:description`), the code's shape decides: a letter then a digit is ICD-10, except V/E codes, which are ICD-9.
- **`elix_*`** — `1` if any `condition_occurrence` row of the encounter matches, whatever its condition type; encounters with no match are `0`.
- **`elix_*_prior`** — `1` if the patient's **earliest** matching diagnosis, anywhere in their `condition_occurrence` history, is dated **strictly before** this encounter's start date (date granularity, as the Stata's `dofc`). The Stata anchors on `first_vitals_time`; both sites anchor on `visit_start_datetime`, which differs by hours at most and is available to any reader.
- **`elix_count`** / **`elix_count_prior`** — number of the 31 flags set (0–31).

| # (current / prior) | Columns | Comorbidity | % enc. current MCW / UW | % enc. prior MCW / UW |
|---|---|---|--:|--:|
| 162 / 194 | `elix_chf` / `elix_chf_prior` | Congestive heart failure | 4.45 / 3.22 | 3.89 / 2.96 |
| 163 / 195 | `elix_arrhythmia` / `elix_arrhythmia_prior` | Cardiac arrhythmias | 13.60 / 19.03 | 14.68 / 18.25 |
| 164 / 196 | `elix_valvdisease` / `elix_valvdisease_prior` | Valvular disease | 2.34 / 1.40 | 3.01 / 1.78 |
| 165 / 197 | `elix_pulmcirc` / `elix_pulmcirc_prior` | Pulmonary circulation disorders | 1.85 / 1.55 | 2.35 / 2.06 |
| 166 / 198 | `elix_periphvasc` / `elix_periphvasc_prior` | Peripheral vascular disorders | 1.43 / 1.65 | 1.51 / 1.64 |
| 167 / 199 | `elix_htnuncomp` / `elix_htnuncomp_prior` | Hypertension, uncomplicated | 12.67 / 10.63 | 12.93 / 11.72 |
| 168 / 200 | `elix_htncomp` / `elix_htncomp_prior` | Hypertension, complicated | 5.15 / 2.80 | 4.37 / 2.33 |
| 169 / 201 | `elix_paralysis` / `elix_paralysis_prior` | Paralysis | 0.91 / 0.69 | 0.94 / 0.59 |
| 170 / 202 | `elix_otherneuro` / `elix_otherneuro_prior` | Other neurological disorders | 5.31 / 5.63 | 6.47 / 6.67 |
| 171 / 203 | `elix_cpd` / `elix_cpd_prior` | Chronic pulmonary disease | 8.89 / 5.93 | 9.60 / 7.26 |
| 172 / 204 | `elix_diabuncomp` / `elix_diabuncomp_prior` | Diabetes, uncomplicated | 0.93 / 4.91 | 1.98 / 4.70 |
| 173 / 205 | `elix_diabcomp` / `elix_diabcomp_prior` | Diabetes, complicated | 0.31 / 2.93 | 0.78 / 2.97 |
| 174 / 206 | `elix_hypothyroid` / `elix_hypothyroid_prior` | Hypothyroidism | 0.33 / 1.73 | 0.58 / 1.79 |
| 175 / 207 | `elix_renalfail` / `elix_renalfail_prior` | Renal failure | 4.44 / 3.38 | 3.71 / 2.88 |
| 176 / 208 | `elix_liverdis` / `elix_liverdis_prior` | Liver disease | 4.86 / 7.00 | 6.41 / 8.31 |
| 177 / 209 | `elix_pud` / `elix_pud_prior` | Peptic ulcer disease (excl. bleeding) | 0.36 / 0.50 | 1.02 / 0.98 |
| 178 / 210 | `elix_aids` / `elix_aids_prior` | AIDS/HIV | 0.27 / 0.46 | 0.35 / 0.70 |
| 179 / 211 | `elix_lymphoma` / `elix_lymphoma_prior` | Lymphoma | 0.51 / 0.35 | 0.33 / 0.21 |
| 180 / 212 | `elix_metacancer` / `elix_metacancer_prior` | Metastatic cancer | 0.37 / 0.42 | 0.31 / 0.18 |
| 181 / 213 | `elix_tumor` / `elix_tumor_prior` | Solid tumour without metastasis | 2.34 / 2.81 | 1.67 / 1.58 |
| 182 / 214 | `elix_rheumarth` / `elix_rheumarth_prior` | Rheumatoid arthritis / collagen vascular | 0.94 / 0.61 | 0.91 / 0.58 |
| 183 / 215 | `elix_coag` / `elix_coag_prior` | Coagulopathy | 4.36 / 3.46 | 5.31 / 4.50 |
| 184 / 216 | `elix_obesity` / `elix_obesity_prior` | Obesity | 0.47 / 1.73 | 1.25 / 2.40 |
| 185 / 217 | `elix_wtloss` / `elix_wtloss_prior` | Weight loss | 0.91 / 2.46 | 1.48 / 3.26 |
| 186 / 218 | `elix_fluidelectro` / `elix_fluidelectro_prior` | Fluid and electrolyte disorders | 1.61 / 12.64 | 4.54 / 14.90 |
| 187 / 219 | `elix_lossanemia` / `elix_lossanemia_prior` | Blood loss anaemia | 0.30 / 0.40 | 0.74 / 0.69 |
| 188 / 220 | `elix_defanemia` / `elix_defanemia_prior` | Deficiency anaemia | 1.93 / 1.56 | 3.43 / 2.71 |
| 189 / 221 | `elix_alcohol` / `elix_alcohol_prior` | Alcohol abuse | 14.19 / 22.43 | 14.19 / 20.08 |
| 190 / 222 | `elix_drugs` / `elix_drugs_prior` | Drug abuse | 6.44 / 8.88 | 10.93 / 12.97 |
| 191 / 223 | `elix_psych` / `elix_psych_prior` | Psychoses | 2.14 / 3.15 | 3.71 / 4.60 |
| 192 / 224 | `elix_depression` / `elix_depression_prior` | Depression | 8.16 / 10.68 | 10.64 / 14.65 |
| 193 | `elix_count` | Number of current comorbidities (0–31) | — | — |
| 225 | `elix_count_prior` | Number of prior comorbidities (0–31) | — | — |

---

## 4. Lab processing, step by step

`build_labs` applies these steps in this order, at both sites. The build log prints a line for every step that changes something.

1. **Concept map.** Rows whose `measurement_concept_id` is in the site's map (`cfg.LAB_CONCEPTS` for MCW, `cfg.LAB_CONCEPTS_UW` for UW; §3.7) are kept and named. Order-level rows that carry a concept but no result have no value and fall out at the pivot.
2. **Derived rows dropped** (`cfg.LAB_SOURCE_EXCLUDE`). Troponin rows whose source string says `DELTA` are the change between two serial high-sensitivity results, stored by MCW under the result's own concept and minute; they are dropped (18,571 at MCW, none at UW).
3. **Censored results — the text decides** (`cfg.LAB_PARSE_CENSORED`). A `value_source_value` of `<x` becomes **x/2** for the detection-limit assays in `cfg.LAB_CENSORED_HALF` (blood alcohol, troponin, procalcitonin, CRP, d-dimer, ammonia, lipase, ESR) and x otherwise; `>x` becomes x. This is applied whether or not the site filled in the number: UW leaves it blank, MCW fills it with the limit. Text results for blood alcohol (`BELOW DETECTION`, `NOT DETECTED`, `NONE DETECTED`, `NEG…`) become 0.005 g/dL (`cfg.LAB_TEXT_VALUES`). This run: MCW 94,648 `<x`, 5,800 `>x`, 147 text; UW 39,451 `<x`, 1,908 `>x`, 1,156 text.
4. **Rows that are not results → missing** (`cfg.LAB_NONRESULT_PATTERN`, `cfg.LAB_ZERO_WITHOUT_TEXT_IS_MISSING`). Text such as `Delete`, `See comment`, `Not measured`, `Cancelled`, `QNS`, `N/A`, or a value of 0 with no text or with `operator_concept_id` 0. A genuine zero carries its text (`0`, `0.00`) and is kept. MCW delivers deleted results as 0: 93,978 rows. UW has none.
5. **Unit harmonisation** on the declared `unit_source_value` (`cfg.LAB_UNIT_CONVERSIONS`; canonical units `cfg.LAB_UNIT_CANONICAL`). For analytes whose label cannot be trusted (`cfg.LAB_UNIT_TRUST`: blood alcohol, d-dimer, albumin) the conversion is **guarded** — it fires only when the value is outside the plausible range now and inside it after. Rows whose unit says they are a different measurement are set to missing (`cfg.LAB_UNIT_EXCLUDE`: `sao2` rows in g/dL, `rdw` rows in fL). This run: MCW troponin ×0.001 44,787, blood alcohol 398, d-dimer 117, albumin 116, 15 `sao2` rows dropped; UW absolute neutrophils /µL → K/µL 207,308, blood alcohol 13,821, albumin 190, 163,990 RDW-SD rows dropped.
6. **Plausible bounds** (`cfg.LAB_PLAUSIBLE`): values outside → missing. `pco2_art`/`pco2_ven` 5–200, `albumin` 0.5–7.0, `rdw` 8–40, `troponin` ≥ 0.
7. **Pivot** to one column per analyte, first value per minute (§2).
8. **Rounding** (Stata rules): 0, 1, 2 or 3 decimals per analyte (§3.7). Troponin keeps 3 so its below-detection 0.005 ng/mL does not round to 0.
9. **Implausible zeros → missing** (`cfg.LAB_ZERO_IMPLAUSIBLE`, 36 analytes): an exact 0 after rounding, where 0 cannot occur (sodium, pH, haemoglobin, lactate, …). 31 values at UW; none left at MCW after step 4.
10. **WBC > 1,000 → missing.**
11. **Differential back-fill.** Where a percentage is missing and the absolute count and WBC are present, `pct = round(100 × abs / wbc)`; the absolute columns are then dropped; percentages outside 0–100 → missing.

**Lab notes per site** (from the unit-resolution records, Audit 7f(iv)):

- *MCW troponin* is three assays in turn in one column, in ng/mL: troponin I (mostly 2010–2015), troponin T (2012–2020) and high-sensitivity troponin I (concept 40769783, 2020 on, delivered in ng/L). There is no overlap between troponin T and high-sensitivity I to calibrate one against the other — compare within assay era. Below-detection results sit at 0.005 ng/mL (`<0.010`) and 0.003 ng/mL (high-sensitivity `<6` ng/L).
- *MCW `sao2`* is the oxyhaemoglobin fraction (LOINC 2030-5, concept 3028653), moved out of `hb`, where it had been read as severe anaemia.
- *UW troponin* is troponin I in ng/mL, results only for 2014–2015 (3021337) and from 2019 (3032971); troponin was ordered in every year, so the gaps are results not delivered.
- *UW `wbc`*: concept 4298431 carries WBC only through 2014; the blood WBC concepts added carry it after. *UW `phosphate`* adds the LRR concept 4017907. *UW `neutrophils_pct`* is back-filled from absolute counts (absent as a percentage in 2008–2013).
- *UW* has no lab results for `troponin` and `blood_alcohol` before 2014 or in 2016–2018, and none for `lactate` and the arterial blood gases before 2014 — limits of the extract.

---

## 5. Medication flags in detail

**Name cleaning** (`pipeline_timeseries.med_name`) turns `drug_source_value` into the bare product name the shared lists use: upper-case; drop a leading `CODE:` prefix; cut at the first strength number (`… 5 MG/ML` and after); drop parenthetical text; strip dose-form, salt and order-qualifier words (`cfg.MED_NAME_STRIP`: TAB, CAP, INJ, SOLN, PATCH, ER, XR, HCL, SULFATE, SODIUM, IN NS, RANGE, EXTENDED RELEASE, …); turn `/` and `,` into `-`; collapse spaces and hyphens. `OXYCODONE HCL 5 MG TAB` → `OXYCODONE`; `OXYCODONE-ACETAMINOPHEN 5-325 MG TAB` → `OXYCODONE-ACETAMINOPHEN`.

**Route** is read from `route_source_value`: IV = `IV`, `IVP`, `IVPB`, `INTRAVENOUS`, `CENTRAL LINE`, `PERIPHERAL`, …; oral/enteral = `PO`, `ORAL`, `NG`, `OG`, `G-TUBE`, `J-TUBE`, `SL`, `BUCCAL`, … (`cfg.ROUTE_IV_PATTERN`, `cfg.ROUTE_PO_PATTERN`). A row whose route matches neither is "unrouted" and passes every route rule.

**Shared key lists** (`cfg.MED_KEYS`, after the corrections below):

| Category | Route rule | Entries (exact cleaned names) |
|---|---|---|
| `vasoactive` | any route | DOPAMINE, EPHEDRINE, EPINEPHRINE, MIDODRINE, NOREPINEPHRINE, PHENYLEPHRINE, TERLIPRESSIN, VASOACTIVE, VASOPRESSIN |
| `abx_iv` | IV, or unrouted | AMIKACIN, AMPICILLIN, AMPICILLIN-SULBACTAM, AZITHROMYCIN, AZTREONAM, CEFAZOLIN, CEFEPIME, CEFOTAXIME, CEFOXITIN, CEFTAROLINE, CEFTAZIDIME, CEFTAZIDIME-AVIBACTAM, CEFTOLAZONE, CEFTOLOZANE, CEFTOLOZANE-TAZOBACTAM, CEFTRIAXONE, CEFUROXIME, CIPROFLOXACIN, CLINDAMYCIN, COLISTIMETHATE, COLISTIN, DAPTOMYCIN, DOXYCYCLINE, ERTAPENEM, ERYTHROMYCIN, GENTAMICIN, IMIPENEM, IMIPENEM-CILASTATIN, IMIPENEM-CILASTIN, LEVOFLOXACIN, LINEZOLID, MEROPENEM, METRONIDAZOLE, MINOCYCLINE, MOXIFLOXACIN, NAFCILLIN, NEOMYCIN-POLYMIXIN, OXACILLIN, PENICILLIN, PENTAMIDINE, PIPERACILLIN, PIPERACILLIN-TAZOBACTAM, POLYMYXIN, QUINUPRISTIN-DALFOPRISTIN, RIFAMPIN, STREPTOMYCIN, SULFACETAMIDE, SULFAMETHOXAZOLE, SULFAMETHOXAZOLE-TRIMETHOPRIM, TICARCILLIN, TICARCILLIN-CLAVULANATE, TICARCILLIN-POT, TIGECYCLINE, TOBRAMYCIN, VANCOMYCIN, ZOSYN |
| `abx_noniv` | non-IV, or unrouted | ALBENDAZOLE, AMOXICILLIN, AMOXICILLIN-POT, AMOXICILLIN-POTASSIUM, AMPICILLIN, ATOVAQUONE, ATOVAQUONE-PROGUANIL, AZITHROMYCIN, BACITRACIN, CB-183, CEFACLOR, CEFADROXIL, CEFAZOLIN, CEFDINIR, CEFIXIME, CEFPODOXIME, CEFPROZIL, CEFTAZIDIME, CEFTIBUTEN, CEFTRIAXONE, CEFUROXIME, CEPHALEXIN, CHLOROQUINE, CIPROFLOXACIN, CLARITHROMYCIN, CLINDAMYCIN, CLOTRIMAZOLE, DAPSONE, DEMECLOCYCLINE, DICLOXACILLIN, DOXYCYCLINE, ERTAPENEM, ERYTHROMYCIN, ETHAMBUTOL, FIDAXOMICIN, FIRST-VANCOMYCIN, FOSFOMYCIN, GENTAMICIN, ISONIAZID, IVERMECTIN, LEVOFLOXACIN, LINEZOLID, MEBENDAZOLE, MEFLOQUINE, METHENAMINE, METRONID-TETRACYC-BIS, METRONIDAZOLE, MINOCYCLINE, MOXIFLOXACIN, NEOMYCIN, NITAZOXANIDE, NITROFURANTOIN, NORFLOXACIN, PAR, PAROMOMYCIN, PENICILLIN, PRAZIQUANTEL, PRIMAQUINE, PYRAZINAMIDE, PYRIMETHAMINE, QUININE, RIFABUTIN, RIFAMPIN, RIFAPENTINE, SMT1996, STREPTOMYCIN, SULFADIAZINE, SULFAMETHOXAZOLE, SULFAMETHOXAZOLE-TMP, SULFAMETHOXAZOLE-TRIMETHOPRIM, SULFISOXAZOLE, TETRACYCLINE, TINIDAZOLE, TOBRAMYCIN, TRIMETHOPRIM, URIBEL, VANCOMYCIN |
| `opioid_iv` | IV, or unrouted | CODEINE, FENTANYL, HYDROMORPHONE, MEPERIDINE, METHADONE, MORPHINE, OPIUM, REMIFENTANIL, SUFENTANIL |
| `opioid_noniv` | non-IV, or unrouted | ACETAMINOPHEN-CODEINE, BUTALBITAL-ASA-CAFF-CODEINE, CHLORPHENIRAMINE-HYDROCODONE, CODEINE, CODEINE-GUAIFENESIN, FENTANYL, GUAIFENESIN-CODEINE, HYDROCODONE, HYDROCODONE-ACETAMINOPHEN, HYDROCODONE-HOMATROPINE, HYDROCODONE-IBUPROFEN, HYDROMORPHONE, LORTAB, MEPERIDINE, METHADONE, MORPHINE, MORPHINE-NALTREXONE, OPIUM, OXYCODONE, OXYCODONE-ACETAMINOPHEN, OXYCODONE-ASPIRIN, OXYMORPHONE, PAREGORIC, PROPOXYPHENE, TAPENTADOL, TRAMADOL, TRAMADOL-ACETAMINOPHEN |
| `sedative_other_iv` | IV, or unrouted | AMOBARBITAL, DEXMEDETOMIDINE, ETOMIDATE, KETAMINE, METHOHEXITAL, MIDAZOLAM, PHENOBARBITAL, PROPOFOL |
| `sedative_other_noniv` | non-IV, or unrouted | ACETAMINOPHEN-CAFF-BUTALBITAL, ACETAMINOPHEN-CAFFEINE-PYRILAMINE, BUTALBITAL-ACETAMINOPHEN-CAFFEINE, BUTALBITAL-APAP-CAFF-COD, BUTALBITAL-ASPIRIN-CAFFEINE, CHLORAL, CHLORPHENIRAMINE, DIPHENHYDRAMINE, DIPHENHYDRAMINE-ACETAMINOPHEN, DIPHENHYDRAMINE-APAP, DOXEPIN, DOXYLAMINE, ESZOPICLONE, MEPHOBARBITAL, MIDAZOLAM, PHENOBARBITAL, PROMETHAZINE, RAMELTEON, ZALEPLON, ZOLPIDEM |
| `steroid_iv` | IV, or unrouted | BETAMETHASONE, DEXAMETHASONE, HYDROCORTISONE, KENALOG, METHYLPREDNISOLONE, TRIAMCINOLONE |
| `steroid_po` | oral / enteral, or unrouted | BUDESONIDE, CORTISONE, DECADRON, DEXAMETHASONE, HYDROCORTISONE, METHYLPREDNISOLONE, PREDNISOLONE, PREDNISONE |

**Pattern classes** (`cfg.MED_PATTERNS`):

| Column | Regex on `drug_source_value` (upper-cased; any route) |
|---|---|
| `benzodiazepine` | `LORAZEPAM\|MIDAZOLAM\|DIAZEPAM\|ALPRAZOLAM\|CLONAZEPAM\|CHLORDIAZEPOXIDE\|TEMAZEPAM\|OXAZEPAM\|CLORAZEPATE\|TRIAZOLAM\|CLOBAZAM` |
| `naloxone` | `^(?!.*(?:BUPRENORPHINE\|PENTAZOCINE)).*(?:NALOXONE\|NARCAN)` |
| `mat_oud` | `^(?!.*\bMORPHINE\b.*NALTREXONE).*(?:BUPRENORPHINE\|METHADONE\|(?<!METHYL)NALTREXONE)` |
| `aud_med` | `^(?!.*\bMORPHINE\b.*NALTREXONE).*(?:(?<!METHYL)NALTREXONE\|ACAMPROSATE\|DISULFIRAM)` |
| `nrt` | `NICOTINE` |
| `paralytic` | `ROCURONIUM\|VECURONIUM\|CISATRACURIUM\|SUCCINYLCHOLINE` |
| `thiamine` | `THIAMINE` |

**Classes overlap by design.** Midazolam sets `benzodiazepine` and a `sedative_other_*` flag; methadone sets an opioid flag and `mat_oud`; naltrexone sets `mat_oud` and `aud_med`; an unrouted row of a drug listed under both routes (phenobarbital, fentanyl, …) sets both the `_iv` and the `_noniv` flag.

**SOFA cardiovascular** (`cfg.SOFA_CARD_RULES`): dobutamine any → 2; dopamine ≤ 5 → 2, ≤ 15 → 3, > 15 → 4; epinephrine/norepinephrine ≤ 0.1 → 3, > 0.1 → 4 (mcg/kg/min). Without a rate, the lowest band.

**Corrections to the shared lists** (`cfg.MED_KEY_CHANGES`). The consortium lists carried entries that are not drugs of their class — most likely components left behind when combination names were split — and the pattern classes caught some wrong drugs. They are corrected identically at both sites. "Rows" is the number of the site's administration rows each change touches (Audit 10f; before the route rule). Rows marked *team to decide* are judgement calls left as they were, for the team to settle with the other consortium sites.

| # | Flag(s) | Entry | Action | Reason | Rows MCW | Rows UW |
|--:|---|---|---|---|--:|--:|
| 1 | `opioid_noniv` | ACETAMINOPHEN | removed | not a drug of this class: non-opioid analgesic; the opioid combinations (oxycodone-, hydrocodone-, codeine-acetaminophen) stay | 584,140 | 456,322 |
| 2 | `opioid_noniv` | TYLENOL | removed | not a drug of this class: brand acetaminophen | 0 | 0 |
| 3 | `opioid_noniv` | FERROUS | removed | not a drug of this class: iron supplement | 25,634 | 36,936 |
| 4 | `opioid_noniv` | GLUCOSAMINE | removed | not a drug of this class: dietary supplement | 0 | 0 |
| 5 | `opioid_noniv` | HYOSCYAMINE | removed | not a drug of this class: anticholinergic antispasmodic | 1,172 | 0 |
| 6 | `opioid_noniv` | NEFAZODONE | removed | not a drug of this class: antidepressant | 115 | 48 |
| 7 | `opioid_noniv` | VILAZODONE | removed | not a drug of this class: antidepressant | 420 | 681 |
| 8 | `opioid_noniv` | BUPIVACAINE | removed | not a drug of this class: local anaesthetic on its own (epidural opioid mixes: see below) | 4,165 | 19,598 |
| 9 | `opioid_noniv` | CHLORPHENIRAMINE | removed | not a drug of this class: antihistamine on its own; hydrocodone-chlorpheniramine stays | 0 | 116 |
| 10 | `opioid_noniv` | LORATADINE-PSEUDOEPHEDRINE | removed | not a drug of this class: antihistamine-decongestant | 0 | 0 |
| 11 | `opioid_noniv` | BUTALBITAL | removed | not a drug of this class: barbiturate on its own; butalbital-codeine stays here, butalbital combinations are sedatives | 0 | 0 |
| 12 | `opioid_iv` | APOMORPHINE | removed | not a drug of this class: dopamine agonist, no opioid activity | 0 | 0 |
| 13 | `opioid_iv` | METHYLNALTREXONE | removed | not a drug of this class: peripheral opioid antagonist (opioid-induced constipation) | 254 | 2,196 |
| 14 | `sedative_other_noniv` | FLURBIPROFEN | removed | not a drug of this class: NSAID | <11 | 0 |
| 15 | `sedative_other_noniv` | NABUMETONE | removed | not a drug of this class: NSAID | <11 | 486 |
| 16 | `sedative_other_noniv` | OXAPROZIN | removed | not a drug of this class: NSAID | 0 | 36 |
| 17 | `sedative_other_noniv` | MECLOFENAMATE | removed | not a drug of this class: NSAID | 0 | 0 |
| 18 | `steroid_iv` | ONDANSETRON | removed | not a drug of this class: antiemetic (5-HT3 antagonist) | 135,312 | 397,032 |
| 19 | `steroid_iv` | PALONOSETRON | removed | not a drug of this class: antiemetic (5-HT3 antagonist) | <11 | 20 |
| 20 | `steroid_iv` | FAMOTIDINE | removed | not a drug of this class: H2 blocker | 101,592 | 47,688 |
| 21 | `steroid_po` | METHYLPHENIDATE | removed | not a drug of this class: stimulant (name confusion with methylprednisolone) | 2,990 | 9,269 |
| 22 | `abx_iv` | DROTRECOGIN | removed | not a drug of this class: activated protein C for sepsis, not an antibiotic | 0 | 0 |
| 23 | `abx_noniv` | CULTURELLE | removed | not a drug of this class: probiotic | 0 | 846 |
| 24 | `abx_noniv` | PENICILLAMINE | removed | not a drug of this class: chelating agent | 0 | 0 |
| 25 | `abx_noniv` | TRIMETHOBENZAMIDE | removed | not a drug of this class: antiemetic (name confusion with trimethoprim) | <11 | 0 |
| 26 | `opioid_noniv` | TAPENTADOL | moved from sedative_other_noniv | Schedule II opioid (mu agonist) | <11 | 177 |
| 27 | `opioid_noniv` | PROPOXYPHENE | moved from sedative_other_noniv | opioid analgesic (withdrawn in the US in 2010) | 0 | 0 |
| 28 | `opioid_noniv` | PAREGORIC | moved from sedative_other_noniv | camphorated tincture of opium | 0 | 0 |
| 29 | `sedative_other_iv` | PHENOBARBITAL | added | barbiturate; used for alcohol withdrawal | 13,021 | 17,668 |
| 30 | `sedative_other_noniv` | PHENOBARBITAL | added | barbiturate; used for alcohol withdrawal | 13,021 | 17,668 |
| 31 | `benzodiazepine` | TEMAZEPAM · OXAZEPAM · CLORAZEPATE · TRIAZOLAM · CLOBAZAM | added to the pattern | benzodiazepines missing from the pattern; oxazepam is used for alcohol withdrawal | 3,844 | 6,017 |
| 32 | `naloxone` | BUPRENORPHINE-NALOXONE · PENTAZOCINE-NALOXONE | excluded from the pattern | maintenance or analgesic combinations, not overdose reversal | 788 | 45,930 |
| 33 | `mat_oud · aud_med` | METHYLNALTREXONE | excluded from the pattern | peripheral opioid antagonist for constipation, not addiction treatment | 254 | 2,196 |
| 34 | `mat_oud · aud_med` | MORPHINE-NALTREXONE | excluded from the pattern | abuse-deterrent morphine (opioid analgesic), not addiction treatment | 0 | 0 |
| 35 | `opioid_iv · opioid_noniv · mat_oud` | METHADONE | kept — team to decide | treats pain and opioid use disorder; counts as opioid exposure and as MOUD | 15,630 | 53,032 |
| 36 | `mat_oud` | BUPRENORPHINE | kept — team to decide | MOUD forms and pain forms (patch, buccal film) are not separated | 22,157 | 46,216 |
| 37 | `mat_oud · aud_med` | NALTREXONE | kept — team to decide | treats both alcohol and opioid use disorder, so it sets both flags | 3,799 | 8,418 |
| 38 | `sedative_other_noniv` | CHLORPHENIRAMINE | kept — team to decide | sedating antihistamine, consistent with diphenhydramine, doxylamine and promethazine in the same list | 0 | 116 |
| 39 | `abx_noniv` | PAR | kept — team to decide | name could not be identified | 0 | 0 |
| 40 | not in any list | GABAPENTIN | not flagged — team to decide | off-label alcohol use disorder treatment and a misuse signal; a new flag would change the shared schema | 296,839 | 253,070 |
| 41 | not in any list | epidural opioid-anaesthetic mixes | not flagged — team to decide | opioid exposure (e.g. fentanyl-bupivacaine) that no exact name in the lists covers | 90 | 0 |

---

## 6. Site differences at a glance

| Item | MCW | UW |
|---|---|---|
| Delivery | `MCW_0618_upload/omop_data_files_20260617` | `UW/09232026` (device file dated 7 Oct 2026); cohort crosswalk `UW/09162026/cwalk_df` |
| Table format / reader | MCW's delivered files, `read_tables_mcw.py` | Spark-style parquet folders, `read_tables.py` (reads every part file in the table's folder) |
| Visit types excluded | none | outpatient 9202, laboratory 32036, hospital 38004515 (6,589 encounters) |
| `enc_class` source | `visit_source_value` prefix | `visit_concept_id` |
| Discharge disposition | two-letter prefix | NUBC code, text fallback |
| SUD cohort file | pipe-delimited, `person_id` + `visit_occurrence_id` | parquet, `visit_occurrence_id`, columns renamed |
| AVPU / orientation | catch-all `measurement` 40493498, routed by row name | `observation`, five concepts, Stata row list |
| Weight unit delivered | pounds | ounces |
| FiO₂ / O₂ flow | 3026238 / 3014080, 3005629 — sparse (0.28% / 0.60%) | 4353936 / 4141684 (7.5% / 19.0%) |
| Oxygen-device rows | `Oxygen Device:<text>` | bare device name |
| Lab issues handled | DELTA troponin rows; deleted results delivered as 0; high-sensitivity troponin in ng/L; `sao2` split from `hb` | RDW-SD under the RDW-CV concept; albumin rows in mg/dL; absolute neutrophils back-fill; WBC concept change after 2014 |
| Orders | CPT / HCPCS | `CUSTOM` codes plus normalised CPT |
| Medication time | administration time (minute) | administration date (day) |
| Columns empty | `dialysis` | `dialysis`, `sao2`, `bands_pct`; `roomair` on 48 rows |
| Encounters without a core vital (left out) | 48,627 (9.7%) — mostly inpatient-class radiology and procedure encounters with no bedside record | 3,274 (1.7%) — mostly ED, two-thirds from 2008 |

---

## 7. Caveats for analysts

1. **`died_ever` is a patient-level outcome**, placed on the last encounter (Stata rule). Fewer than a fifth of the flagged encounters are deaths during the stay; 42–45% fall more than a year after discharge. For in-hospital death use `disch_disp == 6`, or compare `death_date` with the encounter's own time points.
2. **The master is not the cohort.** Encounters with no core vital sign are left out. At MCW they hold 31% of the `died_ever` encounters and half of the `nlp_opioid` encounters (`nlp_opioid` 7.05% in the master against 12.7% in the file). **Take SUD prevalence from the cohort file, not from the master.**
3. **NLP flags are permissive.** Operating threshold 0.02; NLP-only is 76% of MCW and 56% of UW encounters; `nlp_alcohol` alone is 82% / 70%. The files contain screened-positive encounters only.
4. **Medication time resolution differs** — minutes at MCW, days at UW (§3.9). Align to day before comparing sites.
5. **Event flags are 1 or missing.** Fill with 0 per time point only if absence of a record should mean "not given / not done".
6. **Respiratory support.** `vent` ranks last in the Stata hierarchy. UW charts room air almost never on its own, so `roomair` and the room-air 21% in `fio2_final` are near-absent at UW; MCW's measured FiO₂ and flow are sparse, so its `fio2_final` is mostly the room-air value.
7. **Censored lab values.** A value at half a reporting limit (blood alcohol 0.005 g/dL, troponin 0.005 ng/mL) means "below detection"; a value at an upper limit means "at least". Treat them as categories, not measurements, where it matters.
8. **Troponin** spans several assays (MCW) and has delivery gaps (UW, before 2014 and 2016–2018). **D-dimer** FEU and DDU differ about two-fold by assay; **ferritin** has extreme values (UW to 686,367 ng/mL) — winsorise for modelling.
9. **`bmi`** is carried within the encounter on vitals rows, so its row-level distribution is person-time weighted.
10. **`hours_since_admit`** is anchored on the first core vital, and is negative before it.
11. **Quality filters are yours to apply**: `single_vitals_set`, `has_vitals_gap`, `long_encounter`; and set `cfg.APPLY_INPATIENT_FILTER = True` and rebuild to reproduce the Stata inpatient cohort.
12. **GCS verbal at UW** scores intubated patients 1 with no "T" modifier; condition on `vent`/`trach`.
13. **Instruments not delivered.** CIWA-Ar, AUDIT-C and Braden are absent at both sites; `dialysis` is empty at both.
14. **UW 2025** is a fragment: the delivery effectively ends in January 2025.
15. **Identifiers and dates.** The files are patient-level and carry `person_id`, `visit_occurrence_id`, timestamps and `death_date`: keep them on the lab server and out of any repository.

---

## 8. Appendix — value labels and code maps

**`loc_cat`** — `1` ward · `2` ICU · `3` OR · `4` ED · `6` procedural / interventional · `8` IMC / step-down · `99` unclassified (`-1` "drop" segments are removed with everything before them).

**`avpu`** — `0` alert · `1` responds to voice · `2` responds to pain · `3` unresponsive.

**`sex`** — `0` Declined/Unknown · `1` Female · `2` Male.
**`race`** — `0` Declined/Unknown · `1` American Indian or Alaska Native · `2` Asian/Mideast Indian · `3` Black/African American · `4` Pacific Islander/Hawaiian Native · `5` White/Caucasian · `6` Other (`7` Multiracial and `8` Hispanic are defined in the Stata labels but never produced).
**`ethnicity`** — `0` Declined/Unknown · `1` Hispanic/Latino · `2` Non-Hispanic/Latino.

**`disch_disp`** — `1` Acute care transfer · `2` Hospice · `3` Home/self-care · `4` Home health · `5` Facility (SNF/rehab/NH/AL/residential) · `6` Expired · `7` Other/unknown/still hospitalized · `8` Against medical advice.

*MCW prefix map* (`cfg.DISCH_DISP_MAP`): `IP` 1 · `HS` 2 · `HO` 3 · `HH` 4 · `SN`, `RH`, `NH`, `AL`, `RS` 5 · `EX` 6 · `OT`, `UN`, `SH` 7 · `AM` 8.

*UW NUBC map* (`cfg.DISCH_DISP_MAP_UW`): `01` 3 · `02`, `05`, `43`, `61`, `63`, `65`, `66` 1 · `03`, `04`, `62` 5 · `06` 4 · `07` 8 · `09`, `21`, `30`, `70` 7 · `20` 6 · `50`, `51` 2; the planned-readmission variants `81`–`95` take their base code's category. *UW text fallback, in order*: not applicable / entered in error / never arrived → missing; hospice → 2; against medical advice / eloped / left before exam / left without being seen → 8; expired / deceased → 6; home care / home health → 4; skilled nursing / assisted living / intermediate care / nursing facility / rehab → 5; psych / long-term care hospital / critical access / inpatient care / short-term general / VA → 1; other facility → 5; home or self care / home → 3; admit / still patient / jail / prison / clinic / discharge → 7.

**`enc_class`** — `1` ED only · `2` Inpatient (direct, ED-to-inpatient, observation).
