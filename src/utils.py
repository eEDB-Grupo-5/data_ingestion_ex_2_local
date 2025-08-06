import unicodedata
from typing import List, Dict
import re
import polars as pl

def _sinitize_text(text: str) -> str:
    """
    Remove special characters from the text.
    """
    # Normalize unicode characters
    text = unicodedata.normalize('NFKD', text)
    # Remove special characters using regex
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '_', text)  # Replace spaces with underscores
    text = re.sub(r'_+', '_', text)  # Replace multiple underscores with a single one
    text = text.strip("_").strip()  # Remove leading/trailing underscores and spaces
    
    if re.match(r'^\d', text):
        text = f"col_{text}"

    return text

def normalize_column_names(colums: List) -> Dict[str, str]:
    """
    Normalize column names to lowercase and replace spaces with underscores and remove special character.
    """    
    return {
        col: _sinitize_text(col.upper())
        for col in colums
    }

def map_polars_to_postgres_types(dtype):
    if dtype == pl.Int64 or dtype == pl.Int32 or dtype == pl.Int16 or dtype == pl.Int8:
        return "BIGINT"
    elif dtype == pl.Float64 or dtype == pl.Float32:
        return "DOUBLE PRECISION"
    elif dtype == pl.Utf8:
        return "VARCHAR"
    elif dtype == pl.Boolean:
        return "BOOLEAN"
    elif dtype == pl.Datetime:
        return "TIMESTAMP"
    elif dtype == pl.Date:
        return "DATE"
    else:
        return "VARCHAR"
