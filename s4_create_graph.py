from pathlib import Path

import kuzu
import polars as pl

import utils


def init_db(db_name: str, reset: bool = False) -> kuzu.Connection:
    """Initialize the database"""
    if reset:
        Path(db_name).unlink(missing_ok=True)
    db = kuzu.Database(db_name)
    conn = kuzu.Connection(db)
    return conn


def create_tables(conn: kuzu.Connection) -> None:
    """
    Create the graph schema using DDL
    """
    conn.execute(
        """
        CREATE NODE TABLE IF NOT EXISTS Scholar(
            id STRING PRIMARY KEY,
            scholar_type STRING,
            name STRING,
            fullName STRING,
            gender STRING,
            birthDate DATE,
            deathDate DATE
        )
        """
    )
    conn.execute(
        """
        CREATE NODE TABLE IF NOT EXISTS Prize(
            id STRING PRIMARY KEY,
            awardYear INT64,
            category STRING,
            dateAwarded DATE,
            motivation STRING,
            prizeAmount INT64,
            prizeAmountAdjusted INT64
        )
    """)
    conn.execute("CREATE NODE TABLE IF NOT EXISTS City(name STRING PRIMARY KEY, state STRING)")
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Country(name STRING PRIMARY KEY)")
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Continent(name STRING PRIMARY KEY)")
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Institution(name STRING PRIMARY KEY)")
    # Relationships
    conn.execute("CREATE REL TABLE IF NOT EXISTS MENTORED(FROM Scholar TO Scholar)")
    conn.execute("CREATE REL TABLE IF NOT EXISTS BORN_IN(FROM Scholar TO City)")
    conn.execute("CREATE REL TABLE IF NOT EXISTS DIED_IN(FROM Scholar TO City)")
    conn.execute("CREATE REL TABLE IF NOT EXISTS IS_CITY_IN(FROM City TO Country)")
    conn.execute("CREATE REL TABLE IF NOT EXISTS AFFILIATED_WITH(FROM Scholar TO Institution)")
    conn.execute("CREATE REL TABLE IF NOT EXISTS WON(FROM Scholar TO Prize, portion STRING)")
    conn.execute("CREATE REL TABLE IF NOT EXISTS IS_COUNTRY_IN(FROM Country TO Continent)")


# --- Nodes ---


def merge_laureate_nodes(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    """
    Merge laureate node info from reference data source into the database.
    """
    res = conn.execute(
        """
        LOAD FROM $df
        WITH DISTINCT id, name, fullName, gender, birthDate, deathDate
        MERGE (s:Scholar {id: id})
        SET s.name = name,
            s.scholar_type = 'laureate',
            s.fullName = fullName,
            s.gender = gender,
            s.birthDate = birthDate,
            s.deathDate = deathDate
        WITH s,
        CASE
            WHEN starts_with(id, 'l') THEN 'laureate'
            WHEN starts_with(id, 's') THEN 'scholar'
            ELSE NULL
        END AS scholar_type
        SET s.scholar_type = scholar_type
        RETURN count(s) AS num_laureates
        """,
        parameters={"df": df},
    )
    num_laureates = res.get_as_pl()["num_laureates"][0]
    print(f"{num_laureates} laureate nodes ingested")


def merge_scholar_nodes(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        WHERE type = 'scholar'
        MERGE (s:Scholar {id: id})
        SET s.scholar_type = 'scholar',
            s.name = name
        RETURN count(s) AS num_scholars
    """,
        parameters={"df": df},
    )
    num_scholars = res.get_as_pl()["num_scholars"][0]
    print(f"{num_scholars} scholar nodes ingested")


def merge_prize_nodes(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        MERGE (p:Prize {id: prize_id})
        SET p.awardYear = awardYear,
            p.category = category,
            p.dateAwarded = dateAwarded,
            p.motivation = motivation,
            p.prizeAmount = prizeAmount,
            p.prizeAmountAdjusted = prizeAmountAdjusted
        RETURN count(p) AS num_prizes
    """,
        parameters={"df": df},
    )
    num_prizes = res.get_as_pl()["num_prizes"][0]
    print(f"{num_prizes} prize nodes ingested")


def merge_laureate_prize_rels(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        MATCH (s:Scholar {id: laureate_id})
        MATCH (p:Prize {id: prize_id})
        MERGE (s)-[r:WON]->(p)
        SET r.portion = portion
        RETURN count(DISTINCT r) AS num_laureate_prize_rels
    """,
        parameters={"df": df},
    )
    num_laureate_prize_rels = res.get_as_pl()["num_laureate_prize_rels"][0]
    print(f"{num_laureate_prize_rels} laureate-prize relationships ingested")


def merge_city_country_nodes(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        WHERE birthPlaceCity IS NOT NULL
        MERGE (c:City {name: birthPlaceCity})
        SET c.state = birthPlaceState
        RETURN count(DISTINCT c) AS num_cities
        """,
        parameters={"df": df},
    )
    num_cities = res.get_as_pl()["num_cities"][0]
    print(f"{num_cities} city nodes ingested")

    res = conn.execute(
        """
        LOAD FROM $df
        WHERE birthPlaceCountryNow IS NOT NULL
        MERGE (co:Country {name: birthPlaceCountryNow})
        RETURN count(DISTINCT co) AS num_countries
        """,
        parameters={"df": df},
    )
    num_countries = res.get_as_pl()["num_countries"][0]
    print(f"{num_countries} country nodes merged")


def merge_institution_nodes(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        WHERE nameNow IS NOT NULL
        MERGE (i:Institution {name: nameNow})
        RETURN count(DISTINCT i) AS num_institutions
    """,
        parameters={"df": df},
    )
    num_institutions = res.get_as_pl()["num_institutions"][0]
    print(f"{num_institutions} institution nodes merged")


def merge_city_affiliation_nodes(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        WHERE cityNow IS NOT NULL
        WITH DISTINCT cityNow, stateNow
        MERGE (ci:City {name: cityNow})
        SET ci.state = stateNow
        RETURN count(DISTINCT ci) AS num_cities
    """,
        parameters={"df": df},
    )
    num_cities = res.get_as_pl()["num_cities"][0]
    print(f"{num_cities} city nodes merged")


def merge_continent_affiliation_nodes(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        WHERE continent IS NOT NULL
        WITH DISTINCT continent
        MERGE (co:Continent {name: continent})
        RETURN count(DISTINCT co) AS num_continents
    """,
        parameters={"df": df},
    )
    num_continents = res.get_as_pl()["num_continents"][0]
    print(f"{num_continents} continent nodes merged")


# --- Relationships ---


def merge_mentored_rels(conn: kuzu.Connection, filepath: str) -> None:
    # Obtain a mapping of name to id
    df = utils.stack_and_dedup(filepath)
    name_id_map_df = df.unique().select("name", "id")

    df_rels = pl.read_json(filepath).explode("children").explode("parents")
    df_rels = df_rels.with_columns(
        pl.col("parents").struct.field("name").alias("parent_name"),
        pl.col("children").struct.field("name").alias("child_name"),
    ).drop("parents", "children")

    # Join for parent_id
    df_rels = df_rels.join(
        name_id_map_df, left_on="parent_name", right_on="name", how="left"
    ).rename({"id": "parent_id"})
    # Join for child_id
    df_rels = df_rels.join(
        name_id_map_df, left_on="child_name", right_on="name", how="left"
    ).rename({"id": "child_id"})

    res = conn.execute(
        """
        LOAD FROM $df_rels
        MATCH (s1:Scholar {id: parent_id})
        MATCH (s2:Scholar {id: child_id})
        MERGE (s1)-[r:MENTORED]->(s2)
        RETURN count(r) AS num_mentored_rels
    """,
        parameters={"df_rels": df_rels},
    )
    num_mentored_rels = res.get_as_pl()["num_mentored_rels"][0]
    print(f"{num_mentored_rels} mentored relationships ingested")


def merge_laureate_birthplace_rels(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        WHERE birthPlaceCity IS NOT NULL
        MATCH (s:Scholar {id: id})
        MATCH (c:City {name: birthPlaceCity})
        MERGE (s)-[r:BORN_IN]->(c)
        RETURN count(DISTINCT r) AS num_laureate_place_rels
    """,
        parameters={"df": df},
    )
    num_city_country_rels = res.get_as_pl()["num_laureate_place_rels"][0]
    print(f"{num_city_country_rels} laureate birthplace relationships ingested")


def merge_city_country_rels(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        WHERE birthPlaceCity IS NOT NULL AND birthPlaceCountryNow IS NOT NULL
        MATCH (c:City {name: birthPlaceCity})
        MATCH (co:Country {name: birthPlaceCountryNow})
        MERGE (c)-[r:IS_CITY_IN]->(co)
        RETURN count(DISTINCT r) AS num_city_country_rels
    """,
        parameters={"df": df},
    )
    num_city_country_rels = res.get_as_pl()["num_city_country_rels"][0]
    print(f"{num_city_country_rels} city-country relationships ingested")


def merge_laureate_affiliation_rels(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        WHERE nameNow IS NOT NULL
        MATCH (s:Scholar {id: laureateID})
        MATCH (i:Institution {name: nameNow})
        MERGE (s)-[ra:AFFILIATED_WITH]->(i)
        RETURN count(DISTINCT ra) AS num_laureate_affiliation_rels
    """,
        parameters={"df": df},
    )
    num_laureate_affiliation_rels = res.get_as_pl()["num_laureate_affiliation_rels"][0]
    print(f"{num_laureate_affiliation_rels} laureate-affiliation relationships ingested")


def merge_country_affiliation_rels(conn: kuzu.Connection, df: pl.DataFrame) -> None:
    res = conn.execute(
        """
        LOAD FROM $df
        WHERE countryNow IS NOT NULL AND continent IS NOT NULL
        MATCH (co:Country {name: countryNow})
        MATCH (con:Continent {name: continent})
        MERGE (co)-[rc:IS_COUNTRY_IN]->(con)
        RETURN count(DISTINCT rc) AS num_country_affiliation_rels
    """,
        parameters={"df": df},
    )
    num_country_affiliation_rels = res.get_as_pl()["num_country_affiliation_rels"][0]
    print(f"{num_country_affiliation_rels} country-continent-affiliation relationships ingested")


def main(source_filepath: str, reference_filepath: str) -> None:
    conn = init_db("nobel.kuzu", reset=True)
    create_tables(conn)

    df = utils.get_reference_laureates_df(reference_filepath)
    source_and_laureates_df = utils.stack_and_dedup(source_filepath)
    prizes_df = utils.get_prizes_df(reference_filepath)
    affiliations_df = utils.get_affiliations_df(reference_filepath)

    # Nodes
    merge_laureate_nodes(conn, df)
    merge_prize_nodes(conn, prizes_df)
    merge_scholar_nodes(conn, source_and_laureates_df)
    merge_city_country_nodes(conn, df)
    merge_institution_nodes(conn, affiliations_df)
    merge_city_affiliation_nodes(conn, affiliations_df)
    merge_continent_affiliation_nodes(conn, affiliations_df)
    # Relationships
    merge_laureate_birthplace_rels(conn, df)
    merge_city_country_rels(conn, df)
    merge_mentored_rels(conn, source_filepath)
    merge_laureate_prize_rels(conn, prizes_df)
    merge_laureate_affiliation_rels(conn, affiliations_df)
    merge_country_affiliation_rels(conn, affiliations_df)

    # Test query to see if the mentored relationships are ingested correctly
    # Neils Bohr was mentored by 3 people, but his son, Aage was mentored by Neils himself
    res = conn.execute(
        """
        MATCH (s:Scholar)<-[r:MENTORED]-(:Scholar)
        WHERE s.fullName CONTAINS "Bohr"
        RETURN s.fullName, count(r) AS num_mentored_rels
        """
    )
    print(res.get_as_pl())


if __name__ == "__main__":
    source_filepath = "./data/03_merge_datasets/result.json"
    reference_filepath = "./data/01_source_and_reference/reference.json"
    main(source_filepath, reference_filepath)
