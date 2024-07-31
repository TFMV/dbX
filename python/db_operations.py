import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.feather as feather
from typing import Any, Dict

def ingest_to_database(conn, table_name: str, reader):
    with conn.cursor() as cur:
        cur.adbc_ingest(table_name, reader, mode="create_append")
    conn.commit()

def generate_metadata(table: pa.Table) -> Dict[str, Any]:
    schema_dict = {field.name: str(field.type) for field in table.schema}
    return {
        "num_rows": table.num_rows,
        "num_columns": len(table.schema),
        "schema": schema_dict
    }
