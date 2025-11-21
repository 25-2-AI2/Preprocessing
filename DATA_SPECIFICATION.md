# Parquet Data Specification

## Overview

| Item | Value |
|------|-------|
| Source | `json_data/preprocessed_reviews.json` |
| Output Files | `parquet_data/reviews_part1.parquet`, `parquet_data/reviews_part2.parquet` |
| Total Records | 153,246 |
| Records per File | 76,623 |
| Total Columns | 15 |

## Schema

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `review_id` | string | No | Google Maps review unique identifier (Base64 encoded) |
| `original_text` | string | No | Original review text as written by user |
| `cleaned_text` | string | No | Preprocessed/cleaned review text |
| `date` | string | No | Review date in `YYYY.MM.DD` format |
| `date_valid` | bool | No | Whether the date is valid/parseable |
| `language` | string | No | Detected language code (ISO 639-1) |
| `rating` | int64 | No | User rating (0-5 stars) |
| `restaurant_name` | string | No | Restaurant name |
| `restaurant_place_id` | string | No | Google Maps place ID |
| `restaurant_grid` | string | No | Grid zone identifier (e.g., BK1, MH2) |
| `restaurant_address` | string | No | Full address |
| `restaurant_rating` | float64 | No | Restaurant average rating |
| `restaurant_phone` | string | Yes | Restaurant phone number (1,718 nulls per part) |
| `char_count` | int64 | No | Character count of cleaned_text |
| `word_count` | int64 | No | Word count of cleaned_text |

## Data Distribution

### Rating Distribution
| Rating | Count | Percentage |
|--------|-------|------------|
| 5 | 109,292 | 71.3% |
| 4 | 16,576 | 10.8% |
| 3 | 7,956 | 5.2% |
| 2 | 5,584 | 3.6% |
| 1 | 13,708 | 8.9% |
| 0 | 130 | 0.1% |

### Top Languages
| Language | Code | Count |
|----------|------|-------|
| English | en | 142,814 |
| Spanish | es | 5,138 |
| Korean | ko | 1,630 |
| French | fr | 632 |
| Russian | ru | 564 |
| Italian | it | 464 |
| German | de | 404 |
| Chinese | zh | 262 |
| Hebrew | iw | 252 |
| Polish | pl | 170 |

### Other Stats
- Unique Restaurants: ~2,374
- Date Range: 2025 (recent reviews)

## Usage Example

```python
import pandas as pd

# Load single part
df = pd.read_parquet('parquet_data/reviews_part1.parquet')

# Load both parts
df1 = pd.read_parquet('parquet_data/reviews_part1.parquet')
df2 = pd.read_parquet('parquet_data/reviews_part2.parquet')
df_full = pd.concat([df1, df2], ignore_index=True)
```
