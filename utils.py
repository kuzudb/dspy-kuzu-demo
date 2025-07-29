import json
from datetime import date, datetime

import kuzu
import ollama
import polars as pl
from pydantic import BaseModel, Field, field_validator, model_validator


def stack_and_dedup(filepath: str) -> pl.DataFrame:
    """
    Vertically stack the DataFrames for scholars and laureates, and deduplicate
    for use downstream.
    """
    df1 = pl.read_json(filepath).explode("children").unnest("children").drop("parents")
    df2 = pl.read_json(filepath).explode("parents").unnest("parents").drop("children")
    # Vertically stack the two dataframes and dedupe
    df = pl.concat([df1, df2]).unique().sort("name")
    return df


def embed_text(text: list[str], embedding_model: str) -> list[list[float]]:
    """
    Create a text embedding using an Ollama embedding model
    """
    response = ollama.embed(model=embedding_model, input=text)
    embeddings = response["embeddings"]
    return embeddings


def query_vector_index(
    conn: kuzu.Connection,
    query_vector: list[float],
    table_name: str,
    index_name: str,
    topk: int = 5,
) -> pl.DataFrame:
    """Query the vector index with a text query"""
    try:
        conn.execute("INSTALL vector; LOAD vector;")
    except RuntimeError:
        pass
        # print("Vector extension already installed and loaded.")
    res = conn.execute(
        f"""
        CALL QUERY_VECTOR_INDEX(
            '{table_name}',
            '{index_name}',
            $query_vector,
            $limit
        )
        RETURN
            node.id AS id,
            node.knownName AS knownName,
            node.fullName AS fullName,
            node.category AS category,
            node.awardYear AS year
        ORDER BY distance;
        """,
        {"query_vector": query_vector, "limit": topk},
    )
    return res.get_as_pl()  # type: ignore


class Laureate(BaseModel):
    """
    Class to parse and validate the data for Nobel laureates from the reference data obtained
    from the official Nobel Prize API.
    """

    id: str
    knownName: str = Field(alias="name")
    fullName: str
    gender: str
    birthDate: date | None = None
    deathDate: date | None = None
    birthPlaceCity: str | None = None
    birthPlaceCityNow: str | None = None
    birthPlaceState: str | None = None
    deathPlaceCityNow: str | None = None
    deathPlaceState: str | None = None
    birthPlaceCountry: str | None = None
    birthPlaceCountryNow: str | None = None

    @field_validator("birthDate", "deathDate", mode="before")
    def validate_date(cls, v):
        if v is None or v == "":
            return None
        try:
            # Replace malformed date parts with valid ones
            v = str(v).replace("-00-00", "-01-01").replace("-00", "-01")
            return datetime.strptime(v, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None

    @model_validator(mode="before")
    def add_l_to_id(cls, values):
        if "id" in values and not str(values["id"]).startswith("l"):
            values["id"] = f"l{values['id']}"
        return values

    @model_validator(mode="before")
    def extract_state_from_city(cls, values):
        # Extract state from birthPlaceCity
        if "birthPlaceCity" in values and values["birthPlaceCity"]:
            city_parts = values["birthPlaceCity"].split(", ")
            if len(city_parts) == 2:
                values["birthPlaceCity"] = city_parts[0]
                values["birthPlaceState"] = city_parts[1]

        # Extract state from deathPlaceCity if present
        if "deathPlaceCity" in values and values["deathPlaceCity"]:
            city_parts = values["deathPlaceCity"].split(", ")
            if len(city_parts) == 2:
                values["deathPlaceCity"] = city_parts[0]
                values["deathPlaceState"] = city_parts[1]
        return values

    class Config:
        populate_by_name = True


class Affiliation(BaseModel):
    """
    Class to parse and validate the data for Nobel laureate affiliations to organizations/institutions
    from the reference data obtained from the official Nobel Prize API.
    """

    laureateID: str
    nameNow: str | None = None
    cityNow: str | None = None
    stateNow: str | None = None
    countryNow: str | None = None
    continent: str | None = None

    @model_validator(mode="before")
    def add_l_to_id(cls, values):
        if "laureateID" in values and not str(values["laureateID"]).startswith("l"):
            values["laureateID"] = f"l{values['laureateID']}"
        return values

    @model_validator(mode="before")
    def extract_state_from_city(cls, values):
        # Extract state from affiliation_city
        if "cityNow" in values and values["cityNow"]:
            city_parts = values["cityNow"].split(", ")
            if len(city_parts) == 2:
                values["cityNow"] = city_parts[0]
                values["stateNow"] = city_parts[1]
            else:
                values["cityNow"] = city_parts[0]
                values["stateNow"] = None
        return values


def get_reference_laureates_df(filepath: str) -> pl.DataFrame:
    """
    Load the laureates from the reference data and return a DataFrame.
    """
    with open(filepath, "r") as f:
        laureates = [Laureate(**item) for item in json.load(f)]
    if laureates is None:
        raise ValueError("No laureates found in the file. Please check the filepath and ensure the file contains valid data.")
    laureates_clean = [person.model_dump(by_alias=True) for person in laureates]
    return pl.DataFrame(laureates_clean)


def get_prizes_df(filepath: str) -> pl.DataFrame:
    """
    Load the prizes and affiliations from the reference data and return a DataFrame.
    """
    df = (
        pl.read_json(filepath)
        .select("id", "prizes")
        .explode("prizes")
        .with_columns(
            pl.col("prizes")
            .struct.field("category")
            .str.replace("Physiology or Medicine", "Medicine")
            .str.replace("Economic Sciences", "Economics")
            .str.to_lowercase()
        )
    )
    df = df.with_columns(
        pl.concat_str([pl.lit("l"), pl.col("id")], separator="").alias("laureate_id"),
        pl.concat_str(
            [pl.col("prizes").struct.field("awardYear"), pl.col("category")], separator="_"
        ).alias("prize_id"),
        pl.col("prizes").struct.field("portion"),
        pl.col("prizes").struct.field("awardYear").cast(pl.Int64),
        pl.col("prizes").struct.field("dateAwarded").str.to_date("%Y-%m-%d"),
        pl.col("prizes").struct.field("motivation"),
        pl.col("prizes").struct.field("prizeAmount"),
        pl.col("prizes").struct.field("prizeAmountAdjusted"),
    ).drop("id", "prizes")
    return df


def get_affiliations_df(filepath: str) -> pl.DataFrame:
    """
    Load the affiliations from the reference data and return a DataFrame.
    """
    with open(filepath, "r") as f:
        laureates = json.load(f)
        affiliations = []
        for item in laureates:
            for prize in item["prizes"]:
                for affiliation in prize["affiliations"]:
                    affiliation["laureateID"] = item["id"]
                    affiliations.append(Affiliation(**affiliation))
    if affiliations is None:
        raise ValueError("No affiliations found, please check the filepath you're loading from")
    affiliations_clean = [affiliation.model_dump() for affiliation in affiliations]
    return pl.DataFrame(affiliations_clean).unique()


if __name__ == "__main__":
    # db = kuzu.Database("entity_vectors.kuzu")
    # conn = kuzu.Connection(db)

    # vector = embed_text(["bardeen physics"], "nomic-embed-text")[0]
    # res = query_vector_index(conn, vector, "Reference", "reference_index")
    # res
    # res = conn.execute("MATCH (n:Scholar) RETURN n.name, n.category, n.year")
    # print(res.get_as_pl())

    # print(
    #     get_reference_laureates_df("./data/01_source_and_reference/reference.json").select(
    #         "name", "birthPlaceCity"
    #     )
    # )

    # df = get_prizes_df("./data/01_source_and_reference/reference.json")
    # print(df)

    df = get_affiliations_df("./data/01_source_and_reference/reference.json")
    print(df)
