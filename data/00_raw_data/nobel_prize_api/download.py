import json
import time

import requests


def fetch_json_data(url, params={}):
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers, params=params)
    return response.json()


def write_prize_data(nobel_prize_year=1901, year_to=2022):
    all_data = []
    offset = 0
    limit = 25
    total_count = None

    while total_count is None or offset < total_count:
        print(f"Fetching data from {offset} to {offset + limit}")
        url = f"http://api.nobelprize.org/2.1/nobelPrizes?offset={offset}&limit={limit}&nobelPrizeYear={nobel_prize_year}&yearTo={year_to}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return

        if total_count is None:
            total_count = data["meta"]["count"]

        all_data.extend(data["nobelPrizes"])
        offset += limit

        # Add a sleep time of 0.2 seconds between requests
        time.sleep(0.2)

    with open("prizes_raw.json", "w", encoding="utf-8") as f:
        json.dump({"nobelPrizes": all_data}, f, ensure_ascii=False, indent=4)

    print(f"Total Nobel Prizes written: {len(all_data)}")


def write_laureates_data(nobel_prize_year=1901, year_to=2022):
    all_data = []
    offset = 0
    limit = 25
    total_count = None

    while total_count is None or offset < total_count:
        print(f"Fetching data from {offset} to {offset + limit}")
        url = f"https://api.nobelprize.org/2.1/laureates?offset={offset}&limit={limit}&nobelPrizeYear={nobel_prize_year}&yearTo={year_to}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return

        if total_count is None:
            total_count = data["meta"]["count"]

        all_data.extend(data["laureates"])
        offset += limit

        # Add a sleep time of 0.2 seconds between requests
        time.sleep(0.2)

    with open("laureates_raw.json", "w", encoding="utf-8") as f:
        json.dump({"laureates": all_data}, f, ensure_ascii=False, indent=4)

    print(f"Total Laureates written: {len(all_data)}")


def main():
    write_prize_data()
    write_laureates_data()


if __name__ == "__main__":
    main()
