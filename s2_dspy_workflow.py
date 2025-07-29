import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Literal

import dspy
import kuzu
from pydantic import BaseModel

import utils

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
if OPENROUTER_API_KEY is None:
    raise ValueError("Environment variable 'OPENROUTER_API_KEY' is not set. Please set it to proceed.")

EMBEDDING_MODEL = "nomic-embed-text"
DB_NAME = "entity_vectors.kuzu"
db = kuzu.Database(DB_NAME)
conn = kuzu.Connection(db)

# Using OpenRouter. Switch to another LLM provider as needed
lm = dspy.LM(
    model="openrouter/google/gemini-2.0-flash-001",
    api_base="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)
dspy.configure(lm=lm)


# --- Define DSPy signatures ---


class Scholar(BaseModel):
    name: str
    category: str


class Reference(BaseModel):
    id: int
    knownName: str
    fullName: str
    category: str


# --- Get sample recors from the database ---


def collect_laureate_records(conn: kuzu.Connection) -> list[dict[str, Any]]:
    """
    Collect laureate records from the database.
    """
    scholars = conn.execute(
        """
        MATCH (s:Scholar)
        RETURN s.name AS name, s.category AS category, s.year AS year, s.vector AS vector
        ORDER BY s.name;
        """
    ).get_as_pl()
    return scholars.to_dicts()


# --- Get similar reference records via vector search ---


def get_similar_records(
    conn: kuzu.Connection, vector: list[float], topk: int = 3
) -> list[Reference]:
    """
    Get top-k most similar reference records via vector search.
    """
    res = utils.query_vector_index(conn, vector, "Reference", "reference_index", topk=topk)
    res = res.select("id", "knownName", "fullName", "category", "year")
    # Below, we randomly shuffle to ensure that the LLM's reasoning ability is exercised
    # If we don't shuffle, the correct answer will likely always be the first in the list
    # because that's what vector search returns
    res = res.sample(n=len(res), shuffle=True)
    return [Reference(**row) for row in res.to_dicts()]


# --- DSPy signatures & modules ---


class EntityResolver(dspy.Signature):
    """
    Return the reference record `id` that's most likely the same person as the sample record.
    - The result must contain ONLY ONE reference record `id`
    - Also return the confidence level of the mapping based on your judgment.
    """

    sample: Scholar = dspy.InputField(desc="A sample scholar record")
    reference_records: list[Reference] = dspy.InputField(
        desc="A list of reference records from the official Nobel Prize API"
    )
    output: int = dspy.OutputField(desc="Most similar reference record to the sample record")
    confidence: Literal["high", "low"] = dspy.OutputField(
        desc="The confidence level of mapping the sample record to one of the reference records"
    )


async def execute_entity_resolution(
    sample: Scholar, reference_records: list[Reference]
) -> tuple[int, str]:
    """
    Execute the DSPy entity resolution module.

    The approach is similar to "LLM as a judge". The LLM is given a list of reference records
    and a sample laureate record, and it needs to determine which reference record is most likely
    the same person as the sample laureate record.
    """
    resolver = dspy.Predict(EntityResolver)
    result = await resolver.acall(sample=sample, reference_records=reference_records)
    return result.output, result.confidence


async def main(start: int, end: int):
    async def process_record(record) -> dict:
        name, category, award_year, vector = record.values()
        reference_records = get_similar_records(conn, vector)
        scholar = Scholar(name=name, category=category)
        result_id, confidence = await execute_entity_resolution(scholar, reference_records)
        matched_record = [record for record in reference_records if record.id == result_id][0]
        print(f"Sample '{name}' -> Reference ID {result_id}")
        print(f"Matched record: {matched_record}")
        return {
            "source": {"name": name, "category": category, "year": award_year},
            "matched_record": matched_record.model_dump(),
            "confidence": confidence,
        }

    laureate_records = collect_laureate_records(conn)
    tasks = [process_record(record) for record in laureate_records[start:end]]
    results = await asyncio.gather(*tasks)

    # Write to results
    output_dir = Path("./data/02_entity_resolution")
    output_dir.mkdir(exist_ok=True)
    results_file = output_dir / "result-test.json"
    Path(results_file).unlink(missing_ok=True)
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", "-s", type=int, default=0, help="Start index (inclusive)")
    parser.add_argument("--end", "-e", type=int, default=10_000, help="End index (exclusive)")
    args = parser.parse_args()
    if args.end <= args.start or any(x < 0 for x in (args.start, args.end)):
        raise ValueError(
            "Invalid start and end indices. Check that end > start and both are non-negative."
        )

    # asyncio.run(main(args.start, args.end))
