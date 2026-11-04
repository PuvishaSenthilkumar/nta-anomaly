# Dataset

Download **UNSW-NB15** from <https://research.unsw.edu.au/projects/unsw-nb15-dataset>
(the site links to a OneDrive folder, so it needs a browser; it cannot be scripted reliably).

From the folder **"Training and Testing Sets"** copy these two files here:

```
data/UNSW_NB15_training-set.csv   (175,341 rows, 45 columns, header row)
data/UNSW_NB15_testing-set.csv    ( 82,332 rows, 45 columns, header row)
```

These are the partitioned, pre-labelled version (`label` 0/1 and `attack_cat`).
The four big `UNSW-NB15_1..4.csv` files (2.5M rows, 49 columns, no header) also work for the
batch/MapReduce side, but would need a different schema - not used by the scripts here.

No download handy? Generate synthetic data with the same layout to smoke-test the stack:

```bash
python scripts/make_sample.py --rows 3000 --out data      # writes data/UNSW_NB15_{training,testing}-set.csv
```
