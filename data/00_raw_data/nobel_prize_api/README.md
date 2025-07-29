# Download Nobel Prize dataset

## Description

The `download.py` script downloads the Nobel Prize dataset from the [Nobel Prize API](https://nobelprize.org/api/) and
saves it to two JSON files, described below.

## Usage

```bash
uv run download.py
```

This will generate two files:
- `laureates_raw.json`
- `prizes_raw.json`

The raw files are not included here for reasons of brevity and file size. However, you can easily
download them (respecting rate limits) using the script above.

## Preprocessing

To clean the data and generate the reference JSON file, run the following script:

```bash
uv run preprocess_data.py
```

This will generate the reference JSON file in `../../data/reference.json`.
