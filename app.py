from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.feather as feather
import adbc_driver_postgresql.dbapi
import adbc_driver_sqlite.dbapi
import connectorx as cx
import yaml
import os
import time
from pgeon import copy_query

app = FastAPI()

# Load configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Construct the PostgreSQL URI from the configuration components
pg_uri = f"postgresql://{config['database']['postgres']['user']}:{config['database']['postgres']['password']}@" \
         f"{config['database']['postgres']['host']}:{config['database']['postgres']['port']}/{config['database']['postgres']['dbname']}"

# PostgreSQL connection setup
pg_conn = adbc_driver_postgresql.dbapi.connect(pg_uri)

# SQLite connection setup
sqlite_conn = adbc_driver_sqlite.dbapi.connect(config['database']['sqlite']['dbname'])

# Base directory for local files
base_dir = config['local']['base_dir']

class FileUploadResponse(BaseModel):
    filename: str
    time_taken: float

class LoadDataRequest(BaseModel):
    table_name: str
    local_path: Optional[str] = None
    directory: Optional[str] = None
    file_format: str
    partitioning: Optional[List[str]] = None
    database: str

class LoadDataResponse(BaseModel):
    status: str
    time_taken: float
    metadata: Dict[str, Any]

class ExportDataRequest(BaseModel):
    table_name: str
    export_path: str
    file_format: str
    partitioning: Optional[List[str]] = None
    database: str

class ExportDataResponse(BaseModel):
    status: str
    time_taken: float
    metadata: Dict[str, Any]

class GenerateDataResponse(BaseModel):
    status: str
    time_taken: float

class CopyTableRequest(BaseModel):
    source_table: str
    target_table: str
    database: str

class CopyTableResponse(BaseModel):
    status: str
    time_taken: float
    metadata: Dict[str, Any]

def get_connection(database: str):
    if database == "postgres":
        return pg_conn
    elif database == "sqlite":
        return sqlite_conn
    else:
        raise ValueError("Unsupported database")

def ingest_to_database(conn, table_name: str, reader):
    with conn.cursor() as cur:
        cur.adbc_ingest(table_name, reader, mode="create_append")
    conn.commit()

def generate_metadata(table):
    schema_dict = {field.name: str(field.type) for field in table.schema}
    return {
        "num_rows": table.num_rows,
        "num_columns": len(table.schema),
        "schema": schema_dict
    }

@app.post("/load-data/", response_model=LoadDataResponse)
async def load_data(data_params: LoadDataRequest):
    start_time = time.time()
    try:
        dataset = None
        if data_params.directory:
            dataset_path = os.path.join(base_dir, data_params.directory)
            dataset = ds.dataset(dataset_path, format=data_params.file_format, partitioning=data_params.partitioning)
        elif data_params.local_path:
            local_file_path = os.path.join(base_dir, data_params.local_path)
            dataset = ds.dataset(local_file_path, format=data_params.file_format)

        if dataset is None:
            raise HTTPException(status_code=400, detail="Either local_path or directory must be provided.")

        table = dataset.to_table()
        conn = get_connection(data_params.database)
        ingest_to_database(conn, data_params.table_name, table)
        end_time = time.time()
        metadata = generate_metadata(table)
        return {"status": f"{data_params.file_format.upper()} data loaded into {data_params.database.upper()}", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-data/", response_model=ExportDataResponse)
async def export_data(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_path = os.path.join(base_dir, data_params.export_path)
        os.makedirs(os.path.dirname(export_path), exist_ok=True)

        conn = get_connection(data_params.database)
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {data_params.table_name}")
            table = cur.fetch_arrow_table()

        if data_params.partitioning:
            ds.write_dataset(table, export_path, format=data_params.file_format, partitioning=data_params.partitioning)
        else:
            if data_params.file_format == "parquet":
                pq.write_table(table, export_path)
            elif data_params.file_format == "feather":
                feather.write_feather(table, export_path)

        end_time = time.time()
        metadata = generate_metadata(table)
        return {"status": f"Table exported to {data_params.file_format.upper()} from {data_params.database.upper()}", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-connectorx/", response_model=ExportDataResponse)
async def export_connectorx(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_path = os.path.join(base_dir, data_params.export_path)
        os.makedirs(os.path.dirname(export_path), exist_ok=True)

        # Construct the query
        query = f"SELECT * FROM {data_params.table_name}"

        # Determine the URI based on the database
        if data_params.database == "postgres":
            source_uri = pg_uri
        elif data_params.database == "sqlite":
            source_uri = f"sqlite:///{config['database']['sqlite']['dbname']}"
        else:
            raise HTTPException(status_code=400, detail="Unsupported database")

        # Fetch the data using ConnectorX
        return_type = "arrow2"
        if data_params.partitioning:
            partition_col = data_params.partitioning[0]
            table = cx.read_sql(
                source_uri, query, return_type=return_type,
                protocol="binary", partition_on=partition_col, partition_num=10
            )
        else:
            table = cx.read_sql(source_uri, query, return_type=return_type, protocol="binary")

        # Convert to standard pyarrow table
        table = pa.Table.from_batches(table.to_batches())

        # Export to the specified format
        if data_params.file_format == "feather":
            if data_params.partitioning:
                ds.write_dataset(table, export_path, format="feather", partitioning=data_params.partitioning)
            else:
                feather.write_feather(table, export_path)
        elif data_params.file_format == "parquet":
            if data_params.partitioning:
                ds.write_dataset(table, export_path, format="parquet", partitioning=data_params.partitioning)
            else:
                pq.write_table(table, export_path)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")

        end_time = time.time()
        metadata = generate_metadata(table)
        return {"status": f"Table exported to {data_params.file_format.upper()} from {data_params.database.upper()} using ConnectorX", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/direct-load/", response_model=CopyTableResponse)
async def copy_table(data: CopyTableRequest):
    start_time = time.time()
    try:
        conn = get_connection(data.database)
        with conn.cursor() as cur:
            # Create an arrow table for the source table
            query = f"SELECT * FROM {data.source_table}"
            cur.execute(query)
            table = cur.fetch_arrow_table()

            # Ingest data into the target table
            cur.adbc_ingest(data.target_table, table, mode="create_append")
            conn.commit()

        end_time = time.time()
        metadata = generate_metadata(table)
        return {
            "status": "Table copied successfully",
            "time_taken": end_time - start_time,
            "metadata": metadata
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/copy-tablex/", response_model=CopyTableResponse)
async def copy_tablex(data: CopyTableRequest):
    start_time = time.time()
    try:
        # Construct the query
        query = f"SELECT * FROM {data.source_table}"

        # Determine the URI based on the database
        if data.database == "postgres":
            source_uri = pg_uri
        elif data.database == "sqlite":
            source_uri = f"sqlite:///{config['database']['sqlite']['dbname']}"
        else:
            raise HTTPException(status_code=400, detail="Unsupported database")

        # Fetch data using ConnectorX
        table = cx.read_sql(source_uri, query, return_type="arrow2")

        # Ingest data into the target table using ADBC
        conn = get_connection(data.database)
        ingest_to_database(conn, data.target_table, table)

        end_time = time.time()
        metadata = generate_metadata(table)
        return {
            "status": "Table copied successfully using ConnectorX",
            "time_taken": end_time - start_time,
            "metadata": metadata
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-pgeon/", response_model=ExportDataResponse)
async def export_pgeon(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_path = os.path.join(base_dir, data_params.export_path)
        os.makedirs(os.path.dirname(export_path), exist_ok=True)

        query = f"SELECT * FROM {data_params.table_name}"

        table = copy_query(pg_uri, query)

        if data_params.file_format == "parquet":
            pq.write_table(table, export_path)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")

        end_time = time.time()
        metadata = generate_metadata(table)
        return {"status": f"Table exported to {data_params.file_format.upper()} using pgeon", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate-test-data/", response_model=GenerateDataResponse)
async def generate_test_data():
    start_time = time.time()
    try:
        # Define the table
        table = pa.table(
            [
                [1, 1, 2],
                ["foo", "bar", "baz"],
            ],
            names=["ints", "strs"],
        )

        # Define paths
        test_data_dir = os.path.join(base_dir, "test_data")
        os.makedirs(test_data_dir, exist_ok=True)

        csv_file = os.path.join(test_data_dir, "example.csv")
        ipc_file = os.path.join(test_data_dir, "example.arrow")
        parquet_file = os.path.join(test_data_dir, "example.parquet")
        csv_dataset_path = os.path.join(test_data_dir, "csv_dataset")
        ipc_dataset_path = os.path.join(test_data_dir, "ipc_dataset")
        parquet_dataset_path = os.path.join(test_data_dir, "parquet_dataset")

        # Write single files
        pv.write_csv(table, csv_file)
        feather.write_feather(table, ipc_file)
        pq.write_table(table, parquet_file)

        # Write partitioned datasets
        ds.write_dataset(table, csv_dataset_path, format="csv", partitioning=["ints"])
        ds.write_dataset(table, ipc_dataset_path, format="feather", partitioning=["ints"])
        ds.write_dataset(table, parquet_dataset_path, format="parquet", partitioning=["ints"])

        end_time = time.time()
        return {"status": "Test data generated successfully", "time_taken": end_time - start_time}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
