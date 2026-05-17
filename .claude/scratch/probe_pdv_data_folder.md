# PDV data folder audit

- **Date:** 2026-05-17
- **Folder:** `pdv_data` (id `684894fb8cee616eb53c9330`)
- **Subfolders walked:** 1 (incl. root)
- **Target:** live prod (`data.htmdec.org`), GET only, 0 writes
- **Probe:** `.claude/scratch/probe_pdv_data_folder.py`
- **Reproduce:**
  
  ```bash
  set -a; . ./.env.local; set +a
  .venv/bin/python .claude/scratch/probe_pdv_data_folder.py
  ```

**Compliant filename =** ends `.csv` **and** IGSN (`IGSN_PATTERN`) at start **and** stem ends `_ch<N>` (`_SHOT_STEM_TAIL_RE`). Definition is explicit and adjustable — see the probe docstring.

## 1. Whole-folder totals

| Metric                               | Count | % of items |
| ------------------------------------ | -----:| ----------:|
| Total items (all subfolders)         | 4814  | 100%       |
| `meta.data_type == "pdv_trace"`      | 2863  | 59.5%      |
| `meta.data_type` present (any value) | 2863  | 59.5%      |
| `meta.igsn` present                  | 2706  | 56.2%      |
| `meta.experiment_date` present       | 4492  | 93.3%      |
| Compliant filename                   | 1405  | 29.2%      |

## 2. Within `data_type == pdv_trace` (the population of interest)

| Metric                                           | Count    | % of pdv_trace |
| ------------------------------------------------ | --------:| --------------:|
| pdv_trace items                                  | 2863     | 100%           |
| …with `meta.igsn`                                | 2706     | 94.5%          |
| …with `meta.experiment_date`                     | 2856     | 99.8%          |
| …with igsn **and** experiment_date               | 2706     | 94.5%          |
| …with compliant filename                         | 1405     | 49.1%          |
| **Fully SOP** (igsn + exp_date + compliant name) | **1405** | **49.1%**      |
| **NOT fully SOP**                                | **1458** | **50.9%**      |

## 3. Non-compliant filename breakdown (pdv_trace only)

| Bucket          | Count | Meaning                           |
| --------------- | -----:| --------------------------------- |
| compliant       | 1405  | passes all 3 rules                |
| not_csv         | 160   | name does not end `.csv`          |
| no_igsn_prefix  | 1298  | `.csv` but no IGSN at start       |
| no_channel_tail | 0     | `.csv`+IGSN but stem not `_ch<N>` |

- **not_csv** examples: `JHAMAA00001-S2R3C3_2025-12-17_22-51-54_shot06_ch1.csv (1)`, `JHAMAA00001-S2R3C3_2025-12-17_22-52-03_shot07_ch1.csv (1)`, `JHAMAA00001-S2R3C3_2025-12-17_22-52-12_shot08_ch1.csv (1)`, `JHAMAA00001-S2R3C3_2025-12-17_22-52-21_shot09_ch1.csv (1)`, `JHAMAA00001-S2R3C3_2025-12-17_22-52-30_shot10_ch1.csv (1)`, `JHAMAA00001-S2R3C3_2025-12-17_22-52-39_shot11_ch1.csv (1)`
- **no_igsn_prefix** examples: `_2026-05-07_21-22-22_shot01_ch1.csv`, `_2026-05-07_21-22-39_shot02_ch1.csv`, `_2026-05-07_21-22-50_shot03_ch1.csv`, `_2026-05-07_21-23-02_shot04_ch1.csv`, `_2026-05-07_21-23-13_shot05_ch1.csv`, `_2026-05-07_21-23-25_shot06_ch1.csv`

## 4. data_type distribution (folder is mixed)

| meta.data_type | count |
| -------------- | -----:|
| pdv_trace      | 2863  |
| (unset)        | 1951  |
