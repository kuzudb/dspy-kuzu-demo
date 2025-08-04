# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an entity resolution demonstration using DSPy (declarative language model programming) and Kuzu (graph database) to match Nobel Prize laureates between different data sources. The project implements a complete 4-stage pipeline that processes Nobel Prize data from raw API responses to a fully connected knowledge graph.

### Pipeline Stages

1. **s1_create_embeddings.py**: Creates vector embeddings and similarity relationships
   - Loads laureate data from `nobeltree.json` and reference data from Nobel Prize API
   - Generates embeddings using Ollama's `nomic-embed-text` model  
   - Creates Kuzu database with `Scholar` and `Reference` node tables
   - Builds vector indices and `SIMILAR_TO` relationships between entities
   - Outputs: `entity_vectors.kuzu` database

2. **s2_dspy_workflow.py**: Performs LLM-based entity resolution
   - Uses DSPy framework with structured `EntityHandler` signature
   - Queries vector indices to find candidate matches
   - Employs the Gemini 2.0 Flash model using OpenRouter for reasoning about entity matches
   - Supports async processing with confidence scoring and batch operations
   - Outputs: `data/02_entity_resolution/result.json`

3. **s3_merge_datasets.py**: Maps entities to consistent IDs 
   - Builds lookup tables for laureates (l-prefixed) and scholars (s-prefixed)
   - Merges the disambiguated entity mappings with source data
   - Ensures consistent ID assignment across the entire dataset
   - Outputs: `data/03_merge_datasets/result.json`

4. **s4_create_graph.py**: Constructs the final knowledge graph
   - Creates comprehensive graph schema with 6 node types and 7 relationship types
   - Ingests all entity data with proper relationships
   - Builds mentorship networks from genealogical data
   - Links laureates to prizes, institutions, and geographic locations
   - Outputs: `nobel.kuzu` final graph database

## Essential Commands

### Environment Setup
```bash
uv sync                    # Install dependencies
```

### Pipeline Execution
Run the stages sequentially:
```bash
uv run s1_create_embeddings.py     # Creates entity_vectors.kuzu database
uv run s2_dspy_workflow.py         # Requires OPENROUTER_API_KEY environment variable  
uv run s3_merge_datasets.py        # Maps entity IDs
uv run s4_create_graph.py         # Creates final nobel.kuzu database
```

### Testing and Development
```bash
uv run test.py            # Run test script
python -m utils           # Test utility functions
```

## Architecture

### Data Sources
- **Primary**: Nobel Prize genealogical data (`nobeltree.json`) with mentor-student relationships
- **Reference**: Official Nobel Prize API data (`reference.json`) with biographical details
- **Raw Data**: Original API responses in `data/00_raw_data/nobel_prize_api/`

### Data Flow
- **Raw Input** → `data/00_raw_data/` (API responses, preprocessing scripts)
- **Processed Input** → `data/01_source_and_reference/` (nobeltree.json, reference.json)
- **Vector Database** → `entity_vectors.kuzu` (embeddings, similarity relationships)
- **Entity Resolution** → `data/02_entity_resolution/` (LLM matching results)
- **ID Mapping** → `data/03_merge_datasets/` (consistent entity IDs)
- **Final Graph** → `nobel.kuzu` (complete knowledge graph)

### Key Components

**Vector Search Pipeline** (`s1_create_embeddings.py`):
- Uses Ollama's `nomic-embed-text` model (768-dimensional embeddings)
- Creates primary keys from name + category + year for deduplication
- Builds bidirectional Scholar ↔ Reference similarity relationships
- Custom `query_vector_index()` function for efficient similarity search

**Entity Resolution** (`s2_dspy_workflow.py`):
- DSPy `EntityHandler` signature with structured LLM prompting
- Async batch processing with confidence scoring ("high"/"low")
- Shuffles vector search results to exercise LLM reasoning
- OpenRouter integration with Gemini 2.0 Flash model

**Graph Schema** (`s4_create_graph.py`):
- **Nodes**: Scholar, Prize, City, Country, Continent, Institution
- **Relationships**: MENTORED, BORN_IN, DIED_IN, WON, AFFILIATED_WITH, IS_CITY_IN, IS_COUNTRY_IN
- Comprehensive geographic and institutional relationship modeling

**Data Validation** (`utils.py`):
- Pydantic models for `Laureate` and `Affiliation` with field validation
- Automatic date parsing with malformed date handling
- State extraction from city strings (e.g., "Boston, MA" → city="Boston", state="MA")
- ID prefixing (laureates get "l" prefix, scholars get "s" prefix)

### Dependencies
- **dspy** (≥3.0.0b3): LLM programming framework with structured prompting
- **kuzu** (0.11.1): Embeddable, scable and fast graph database with vector search capabilities
- **ollama** (≥0.5.1): Local embedding model inference
- **polars** (≥1.31.0): High-performance DataFrame operations
- **pydantic** (≥2.11.7): Data validation and parsing with type safety
- **python-dotenv**: Environment variable management

## Environment Variables
- `OPENROUTER_API_KEY`: Required for s2_dspy_workflow.py LLM calls

## Project Structure
```
├── data/
│   ├── 00_raw_data/nobel_prize_api/    # Original API data and scripts
│   ├── 01_source_and_reference/        # Processed input data
│   ├── 02_entity_resolution/           # LLM matching results  
│   └── 03_merge_datasets/              # ID-mapped datasets
├── assets/                             # Schema diagrams
├── entity_vectors.kuzu                 # Vector database
├── nobel.kuzu                          # Final knowledge graph
├── s1_create_embeddings.py            # Stage 1: Embeddings
├── s2_dspy_workflow.py                # Stage 2: Entity resolution
├── s3_merge_datasets.py               # Stage 3: ID mapping
├── s4_create_graph.py                 # Stage 4: Graph construction
├── utils.py                           # Shared utilities and data models
└── test.py                            # Test script
```

## Notable Implementation Details
- John Bardeen appears twice (won Physics in 1956 and 1972) but stored once per category
- Marie Curie appears twice (Physics and Chemistry prizes) - correctly handled
- Mentorship relationships extracted from genealogical tree structure
- Vector similarity used for fuzzy name matching between data sources
- Async processing enables efficient batch entity resolution
- Comprehensive geographic modeling with city→country→continent hierarchy