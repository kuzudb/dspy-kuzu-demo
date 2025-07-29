import json
from pathlib import Path

import polars as pl


def load_json(path: Path) -> list[dict]:
    with open(path, "r") as f:
        return json.load(f)


def write_json(path: Path, data: list[dict]) -> None:
    Path(path).unlink(missing_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def get_scholars_mapped_ids(filepath: Path) -> list[dict[str, str]]:
    """
    Get a list of dictionaries with the name and id of the scholars. This is easier to do in Polars.
    """
    df1 = pl.read_json(filepath).explode("children").unnest("children").drop("parents")
    df2 = pl.read_json(filepath).explode("parents").unnest("parents").drop("children")
    # Vertically stack the two dataframes and dedupe
    df = (
        pl.concat([df1, df2])
        .unique()
        .filter(pl.col("type") == "scholar")
        .with_row_index()
        .with_columns(pl.concat_str([pl.lit("s"), pl.col("index") + 1], separator="").alias("id"))
        .drop("index")
    )
    mapped_scholar_ids = df.select("name", "id").to_dicts()
    return mapped_scholar_ids


def build_lookup_laureates(mapped_ids: list[dict]) -> dict[str, str]:
    return {
        entry["source"]["name"]: "l" + str(entry["matched_record"]["id"]) for entry in mapped_ids
    }


def build_lookup_scholars(mapped_ids: list[dict[str, str]]) -> dict[str, str]:
    return {entry["name"]: str(entry["id"]) for entry in mapped_ids}


def add_ids(
    tree: list[dict],
    lookup_laureates: dict[str, str],
    lookup_scholars: dict[str, str],
) -> list[dict]:
    for entry in tree:
        for group in ["children", "parents"]:
            for obj in entry.get(group, []):
                if obj.get("type") == "laureate":
                    name = obj.get("name")
                    if name in lookup_laureates:
                        obj["id"] = lookup_laureates[name]
                elif obj.get("type") == "scholar":
                    name = obj.get("name")
                    if name in lookup_scholars:
                        obj["id"] = lookup_scholars[name]
    return tree


def main() -> None:
    source_data_dir = Path("./data/01_source_and_reference")
    previous_results_dir = Path("./data/02_entity_resolution")
    output_data_dir = Path("./data/03_merge_datasets")
    output_data_dir.mkdir(exist_ok=True)

    mapped_ids_laureates = load_json(previous_results_dir / "result.json")
    mapped_ids_scholars = get_scholars_mapped_ids(source_data_dir / "nobeltree.json")
    lookup_laureates = build_lookup_laureates(mapped_ids_laureates)
    lookup_scholars = build_lookup_scholars(mapped_ids_scholars)
    nobeltree_dicts = load_json(source_data_dir / "nobeltree.json")

    tree = add_ids(nobeltree_dicts, lookup_laureates, lookup_scholars)
    write_json(output_data_dir / "result.json", tree)


if __name__ == "__main__":
    main()
