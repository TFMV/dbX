from fastapi import FastAPI, HTTPException
from typing import Any
import os
import time
import connectorx as cx
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.feather as feather
from pgeon import copy_query
from database import db_manager, get_pg_uri
from models import LoadDataRequest, LoadDataResponse, ExportDataRequest, ExportDataResponse, CopyTableRequest, CopyTableResponse, GenerateDataResponse
from db_operations import ingest_to_database
import yaml

app = FastAPI()

# Load configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

base_dir = config['local']['base_dir']

def generate_metadata(schema_or_table):
    if isinstance(schema_or_table, pa.Table):
        schema = schema_or_table.schema
    elif isinstance(schema_or_table, pa.Schema):
        schema = schema_or_table
    else:
        raise ValueError("Expected a pyarrow.Table or pyarrow.Schema object")

    metadata = {
        "fields": [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable
            }
            for field in schema
        ]
    }
    return metadata

@app.post("/load-data/", response_model=LoadDataResponse)
async def load_data(data_params: LoadDataRequest):
    start_time = time.time()
    try:
        conn = db_manager.get_connection(data_params.database_role)

        if data_params.local_path:
            parquet_file = os.path.join(base_dir, data_params.local_path)
            with conn.cursor() as cur:
                reader = pq.ParquetFile(parquet_file)
                cur.adbc_ingest(data_params.table_name, reader.iter_batches(), mode="create_append")
                metadata = generate_metadata(reader.schema_arrow)

        elif data_params.directory:
            parquet_dataset = os.path.join(base_dir, data_params.directory)
            reader = ds.dataset(
                parquet_dataset,
                format="parquet",
                partitioning=data_params.partitioning,
            )
            with conn.cursor() as cur:
                cur.adbc_ingest(data_params.table_name, reader, mode="create_append")
                metadata = generate_metadata(reader.schema)

        else:
            raise HTTPException(status_code=400, detail="Either local_path or directory must be provided.")

        conn.commit()

        end_time = time.time()
        return {
            "status": f"{data_params.file_format.upper()} data loaded into {data_params.database_role.upper()}",
            "time_taken": end_time - start_time,
            "metadata": metadata,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-data/", response_model=ExportDataResponse)
async def export_data(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_path = os.path.join(base_dir, data_params.export_path)
        os.makedirs(os.path.dirname(export_path), exist_ok=True)

        conn = db_manager.get_connection(data_params.database_role)
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
        return {"status": f"Table exported to {data_params.file_format.upper()} from {data_params.database_role.upper()}", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-connectorx/", response_model=ExportDataResponse)
async def export_connectorx(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_path = os.path.join(base_dir, data_params.export_path)
        os.makedirs(os.path.dirname(export_path), exist_ok=True)

        query = f"SELECT * FROM {data_params.table_name}"

        if data_params.database_role == "source":
            if config['database']['source']['type'] == "postgres":
                source_uri = get_pg_uri(config['database']['source'])
            elif config['database']['source']['type'] == "sqlite":
                source_uri = f"sqlite:///{config['database']['source']['dbname']}"
            else:
                raise HTTPException(status_code=400, detail="Unsupported source database")

        elif data_params.database_role == "target":
            if config['database']['target']['type'] == "postgres":
                source_uri = get_pg_uri(config['database']['target'])
            elif config['database']['target']['type'] == "sqlite":
                source_uri = f"sqlite:///{config['database']['target']['dbname']}"
            else:
                raise HTTPException(status_code=400, detail="Unsupported target database")
        else:
            raise HTTPException(status_code=400, detail="Invalid database role")

        return_type = "arrow2"
        if data_params.partitioning:
            partition_col = data_params.partitioning[0]
            table = cx.read_sql(
                source_uri, query, return_type=return_type,
                protocol="binary", partition_on=partition_col, partition_num=10
            )
        else:
            table = cx.read_sql(source_uri, query, return_type=return_type, protocol="binary")

        table = pa.Table.from_batches(table.to_batches())

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
        return {"status": f"Table exported to {data_params.file_format.upper()} from {data_params.database_role.upper()} using ConnectorX", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/direct-load/", response_model=CopyTableResponse)
async def copy_table(data: CopyTableRequest):
    start_time = time.time()
    try:
        source_conn = db_manager.get_connection("source")
        target_conn = db_manager.get_connection("target")
        
        with source_conn.cursor() as cur:
            query = f"SELECT * FROM {data.source_table}"
            cur.execute(query)
            table = cur.fetch_arrow_table()

        with target_conn.cursor() as cur:
            cur.adbc_ingest(data.target_table, table, mode="create_append")
            target_conn.commit()

        end_time = time.time()
        metadata = generate_metadata(table)
        return {
            "status": "Table copied successfully",
            "time_taken": end_time - start_time,
            "metadata": metadata
        }
    except Exception as e:
        target_conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/copy-tablex/", response_model=CopyTableResponse)
async def copy_tablex(data: CopyTableRequest):
    start_time = time.time()
    try:
        query = f"SELECT * FROM {data.source_table}"

        if config['database']['source']['type'] == "postgres":
            source_uri = get_pg_uri(config['database']['source'])
        elif config['database']['source']['type'] == "sqlite":
            source_uri = f"sqlite:///{config['database']['source']['dbname']}"
        else:
            raise HTTPException(status_code=400, detail="Unsupported source database")

        table = cx.read_sql(source_uri, query, return_type="arrow2")

        target_conn = db_manager.get_connection("target")
        ingest_to_database(target_conn, data.target_table, table)

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

        table = copy_query(get_pg_uri(config['database']['source']), query)

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
        table = pa.table(
            [
                [1, 1, 2],
                ["foo", "bar", "baz"],
            ],
            names=["ints", "strs"],
        )

        test_data_dir = os.path.join(base_dir, "test_data")
        os.makedirs(test_data_dir, exist_ok=True)

        csv_file = os.path.join(test_data_dir, "example.csv")
        ipc_file = os.path.join(test_data_dir, "example.arrow")
        parquet_file = os.path.join(test_data_dir, "example.parquet")
        csv_dataset_path = os.path.join(test_data_dir, "csv_dataset")
        ipc_dataset_path = os.path.join(test_data_dir, "ipc_dataset")
        parquet_dataset_path = os.path.join(test_data_dir, "parquet_dataset")

        pv.write_csv(table, csv_file)
        feather.write_feather(table, ipc_file)
        pq.write_table(table, parquet_file)

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
