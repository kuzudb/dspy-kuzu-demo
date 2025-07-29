from pathlib import Path

import kuzu
import polars as pl

import utils

CATEGORY_MAPPING = {
    "Physics": "Physics",
    "Chemistry": "Chemistry",
    "Physiology or Medicine": "Medicine",
    "Economic Sciences": "Economics",
}


def init_db(db_name: str) -> kuzu.Connection:
    """Initialize the Kuzu database"""
    db = kuzu.Database(db_name)
    conn = kuzu.Connection(db)
    return conn


# --- Data loading ---


def load_laureate_data(filepath: str) -> pl.DataFrame:
    """
    Load laureate data from a dataframe and create a primary key column for use downstream.
    """
    df = utils.stack_and_dedup(filepath)
    df = df.filter(pl.col("type") == "laureate")
    df = df.with_columns(
        pl.concat_str(
            [
                pl.col("name").str.to_lowercase(),
                pl.col("category").str.to_lowercase(),
                pl.col("year"),
            ],
            separator=" ",
        ).alias("pk"),
    )
    return df


def load_nobel_official_data(filepath: str) -> pl.DataFrame:
    """
    Load laureate data from a dataframe and create a primary key column for use downstream.
    """
    df = pl.read_json(filepath).explode("prizes").unnest("prizes")
    df = df.with_columns(
        pl.concat_str(
            [
                pl.col("fullName").str.to_lowercase(),
                pl.col("category").str.to_lowercase(),
                pl.col("awardYear"),
            ],
            separator=" ",
        ).alias("pk"),
    )
    return df


# --- Graph schema creation ---


def create_laureate_node_table(conn: kuzu.Connection) -> None:
    """Create the Scholar node table from laureate data"""
    conn.execute(
        """
        CREATE NODE TABLE Scholar(
            name STRING,
            type STRING,
            category STRING,
            year STRING,
            pk STRING PRIMARY KEY,
            vector FLOAT[768]
            )
        """
    )
    print("Laureate node table created successfully")


def create_reference_table(conn: kuzu.Connection) -> None:
    """Create the Reference table from official Nobel Prize API with vector support"""
    conn.execute(
        """
        CREATE NODE TABLE IF NOT EXISTS Reference (
            id STRING,
            knownName STRING,
            givenName STRING,
            familyName STRING,
            fullName STRING,
            gender STRING,
            birthDate STRING,
            birthPlaceCity STRING,
            birthPlaceCountry STRING,
            birthPlaceCityNow STRING,
            birthPlaceCountryNow STRING,
            birthPlaceContinent STRING,
            deathDate STRING,
            awardYear STRING,
            category STRING,
            pk STRING PRIMARY KEY,
            vector FLOAT[768]
        )
        """
    )
    print("Reference table created successfully")


# --- Data ingestion ---


def add_embeddings_to_df(df: pl.DataFrame, embedding_model: str, colname: str) -> pl.DataFrame:
    """Add embeddings to a dataframe based on pk column"""
    texts = df.select(colname).to_series().to_list()
    embeddings = utils.embed_text(texts, embedding_model)
    df = df.with_columns(pl.Series(embeddings).alias("vector"))
    return df


def ingest_laureate_data(conn: kuzu.Connection, df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.concat_str(
            [
                pl.col("name").str.to_lowercase(),
                pl.coalesce(pl.col("category").str.to_lowercase(), pl.lit("no prize")),
            ],
            separator=" ",
        ).alias("pk"),
    ).unique(subset=["pk"])
    conn.execute(
        """
        COPY Scholar FROM df
        """
    )
    print(f"Inserted {len(df)} laureate nodes")  # type: ignore
    return df


def ingest_reference_data(conn: kuzu.Connection, df: pl.DataFrame) -> pl.DataFrame:
    """Ingest data into the Reference node table"""
    print(df.filter(pl.col("pk").str.contains("bardeen")).head())
    df = df.unique(subset=["pk"])
    conn.execute(
        """
        COPY Reference FROM (
            LOAD FROM df
            RETURN 
              id,
              knownName,
              givenName,
              familyName,
              fullName,
              gender,
              birthDate,
              birthPlaceCity,
              birthPlaceCountry,
              birthPlaceCityNow,
              birthPlaceCountryNow,
              birthPlaceContinent,
              deathDate,
              awardYear,
              category,
              pk,
              vector
        )
        """
    )
    print(f"Inserted {len(df)} reference nodes")  # type: ignore
    return df


def create_similarity_relationship_table(conn: kuzu.Connection) -> None:
    """Create the SIMILAR_TO relationship table from Scholar to Reference nodes"""
    conn.execute(
        """
        CREATE REL TABLE IF NOT EXISTS SIMILAR_TO(
            FROM Scholar TO Reference,
            similarity_score FLOAT
        )
        """
    )
    print("SIMILAR_TO relationship table created successfully")


def create_scholar_reference_similarities(conn: kuzu.Connection) -> None:
    """Create similarity relationships from Scholar nodes to most similar Reference nodes"""
    # Get all Scholar nodes with their vectors
    scholars = conn.execute(
        """
        MATCH (s:Scholar)
        RETURN s.pk AS pk, s.vector AS vector
        """
    ).get_as_pl()
    
    print(f"Creating similarity relationships for {len(scholars)} scholars to Reference nodes...")
    
    # Collect all similarity relationships in a list
    similarity_relationships = []
    
    # For each scholar, find top 3 most similar Reference nodes
    for i, row in enumerate(scholars.iter_rows(named=True)):
        pk = row["pk"]
        vector = row["vector"]
        
        # Query vector index to find most similar Reference nodes
        similar_references = conn.execute(
            """
            CALL QUERY_VECTOR_INDEX(
                'Reference',
                'reference_index',
                $query_vector,
                3
            )
            RETURN 
                node.pk AS similar_pk,
                distance
            ORDER BY distance
            """,
            {"query_vector": vector}
        ).get_as_pl()
        
        # Take top 3 most similar Reference nodes
        similar_references = similar_references.head(3)
        
        # Add to relationships list
        for similar_row in similar_references.iter_rows(named=True):
            similar_pk = similar_row["similar_pk"]
            distance = similar_row["distance"]
            similarity_score = 1.0 - distance
            
            similarity_relationships.append({
                "source_pk": pk,
                "target_pk": similar_pk,
                "similarity_score": similarity_score
            })
        
        if (i + 1) % 50 == 0:
            print(f"Processed {i + 1}/{len(scholars)} scholars")
    
    # Create DataFrame and batch insert relationships
    if similarity_relationships:
        df_similarities = pl.DataFrame(similarity_relationships)
        
        conn.execute(
            """
            LOAD FROM $df_similarities
            MATCH (s:Scholar {pk: source_pk})
            MATCH (r:Reference {pk: target_pk})
            MERGE (s)-[rel:SIMILAR_TO]->(r)
            SET rel.similarity_score = similarity_score
            """,
            parameters={"df_similarities": df_similarities}
        )
        
        print(f"Created {len(similarity_relationships)} Scholar->Reference similarity relationships")


def create_vector_index(conn: kuzu.Connection, table_name: str, index_name: str) -> None:
    """Create vector index on the given table and column name"""
    try:
        conn.execute("INSTALL vector; LOAD vector;")
    except RuntimeError:
        print("Vector extension already installed and loaded.")
    conn.execute(
        f"""
        CALL CREATE_VECTOR_INDEX(
            '{table_name}',
            '{index_name}',
            'vector'
        );
        """
    )
    print(f"Vector index created for {table_name} table.")


if __name__ == "__main__":
    DATA_DIR = "./data/01_source_and_reference"
    DB_NAME = "entity_vectors.kuzu"
    EMBEDDING_MODEL = "nomic-embed-text"

    Path(DB_NAME).unlink(missing_ok=True)
    conn = init_db(DB_NAME)

    # Create tables
    create_laureate_node_table(conn)
    create_reference_table(conn)
    create_similarity_relationship_table(conn)

    # Insert scholar data from source file
    df_laureate = load_laureate_data(f"{DATA_DIR}/nobeltree.json")
    df_laureate = add_embeddings_to_df(df_laureate, EMBEDDING_MODEL, "pk")
    df_laureate = ingest_laureate_data(conn, df_laureate)
    print(f"Generated {len(df_laureate)} embeddings for laureate data")

    # Insert reference data from source file
    df_reference = load_nobel_official_data(f"{DATA_DIR}/reference.json")
    df_reference = add_embeddings_to_df(df_reference, EMBEDDING_MODEL, "pk")
    df_reference = ingest_reference_data(conn, df_reference)
    print(f"Generated {len(df_reference)} embeddings for reference data")

    # John Bardeen won the Physics prize twice, in 1956 and 1972. But we only store the record once for his name and category.
    # Similarly, Marie Curie won the Physics prize once and the Chemistry prize once. We'd expect to see 2 records for her.
    res = conn.execute(
        """
        MATCH (r:Reference)
        WHERE r.pk CONTAINS "bardeen" OR r.pk CONTAINS "curie"
        RETURN r.fullName, r.category, r.awardYear, r.birthDate, r.deathDate
        ORDER BY r.fullName, r.category, r.awardYear
        """
    )
    print(res.get_as_pl())  # type: ignore

    # Create vector index on scholar table
    create_vector_index(conn, "Scholar", "scholar_index")
    create_vector_index(conn, "Reference", "reference_index")
    
    # Create similarity relationships between scholars and reference nodes
    create_scholar_reference_similarities(conn)
