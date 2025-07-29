# Download Nobel Prize dataset

## Description

The `download.py` script downloads the Nobel Prize dataset from the [Nobel Prize API](https://nobelprize.org/api/) and saves it to a CSV file.

## Usage

```bash
python download.py
```

This will generate two files:
- `laureates_raw.json`
- `prizes_raw.json`

Once these two files are present in the local directory, you can run the `clean_laureates.py` script to clean the data and generate two new files:
- `laureates.json`
- `laureates_er.json`

The `laureates.json` file contains the cleaned Nobel laureates data, and the `laureates_er.json` file contains the data in a format suitable for entity resolution.

Similarly, run the `clean_prizes.py` script will This will clean the prizes data and generate the
following file that has the relevant, clean prize data.
- `prizes.json`
