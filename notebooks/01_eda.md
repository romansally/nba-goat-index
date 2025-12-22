# NBA GOAT Index - Exploratory Data Analysis

This notebook explores the NBA player data and experiments with different ranking algorithms.

## Setup

```python
import sys
sys.path.append('..')

import polars as pl
import duckdb
from src.storage.storage_interface import get_storage

# Initialize storage
storage = get_storage("local")
```

## Load Data

```python
# TODO: Load player data
# df = pl.read_parquet(storage.read("silver/players.parquet"))
```

## Exploration

```python
# TODO: Add your analysis here
```
