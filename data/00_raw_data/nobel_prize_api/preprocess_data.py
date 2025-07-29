# Extract only the relevant information from the laureates_raw.json file
import json


def extract_prize_info(prize):
    """Extract detailed prize information including affiliations"""
    prize_info = {
        "awardYear": prize.get("awardYear"),
        "category": prize.get("category", {}).get("en"),
        "portion": prize.get("portion"),
        "dateAwarded": prize.get("dateAwarded"),
        "motivation": prize.get("motivation", {}).get("en"),
        "prizeAmount": prize.get("prizeAmount"),
        "prizeAmountAdjusted": prize.get("prizeAmountAdjusted"),
        "affiliations": [],
    }

    # Extract affiliation information
    for affiliation in prize.get("affiliations", []):
        affiliation_info = {
            "name": affiliation.get("name", {}).get("en"),
            "nameNow": affiliation.get("nameNow", {}).get("en"),
            "city": affiliation.get("city", {}).get("en"),
            "country": affiliation.get("country", {}).get("en"),
            "cityNow": affiliation.get("cityNow", {}).get("en"),
            "countryNow": affiliation.get("countryNow", {}).get("en"),
            "continent": affiliation.get("continent", {}).get("en"),
        }
        prize_info["affiliations"].append(affiliation_info)

    return prize_info


def process_laureates_data(input_file, output_file):
    """Process laureates data and extract relevant information"""
    with open(input_file, "r") as f:
        data = json.load(f)

    # Subset of the data to only laureates who won nobel prizes in fields other than Peace or Literature
    relevant_nobel_laureates = []
    for laureate in data["laureates"]:
        if laureate["nobelPrizes"]:
            for prize in laureate["nobelPrizes"]:
                if prize["category"]["en"] != "Peace" and prize["category"]["en"] != "Literature":
                    # Extract detailed prize information using the new function
                    prizes = [
                        extract_prize_info(prize) for prize in laureate.get("nobelPrizes", [])
                    ]
                    record = {
                        "id": laureate.get("id", None),
                        "knownName": laureate.get("knownName", {}).get("en", None),
                        "givenName": laureate.get("givenName", {}).get("en", None),
                        "familyName": laureate.get("familyName", {}).get("en", None),
                        "fullName": laureate.get("fullName", {}).get("en", None),
                        "gender": laureate.get("gender", None),
                        "birthDate": laureate.get("birth", {}).get("date", None),
                        "birthPlaceCity": laureate.get("birth", {})
                        .get("place", {})
                        .get("city", {})
                        .get("en", None),
                        "birthPlaceCountry": laureate.get("birth", {})
                        .get("place", {})
                        .get("country", {})
                        .get("en", None),
                        "birthPlaceCityNow": laureate.get("birth", {})
                        .get("place", {})
                        .get("cityNow", {})
                        .get("en", None),
                        "birthPlaceCountryNow": laureate.get("birth", {})
                        .get("place", {})
                        .get("countryNow", {})
                        .get("en", None),
                        "birthPlaceContinent": laureate.get("birth", {})
                        .get("place", {})
                        .get("continent", {})
                        .get("en", None),
                        "deathDate": laureate.get("death", {}).get("date", None),
                        "prizes": prizes,
                    }
                    relevant_nobel_laureates.append(record)

    print(f"Extracted {len(relevant_nobel_laureates)} relevant laureates")

    # Output the data to a new json file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(relevant_nobel_laureates, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    process_laureates_data("laureates_raw.json", "../../data/reference.json")
