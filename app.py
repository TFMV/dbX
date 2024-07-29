from fastapi import FastAPI, UploadFile, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.feather as feather
import adbc_driver_postgresql.dbapi
import connectorx as cx
import yaml
import os
import time

app = FastAPI()

# Load configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Construct the PostgreSQL URI from the configuration components
uri = f"postgresql://{config['database']['user']}:{config['database']['password']}@" \
      f"{config['database']['host']}:{config['database']['port']}/{config['database']['dbname']}"

# PostgreSQL connection setup
conn = adbc_driver_postgresql.dbapi.connect(uri)

# Base directory for local files
base_dir = config['local']['base_dir']

class FileUploadResponse(BaseModel):
    filename: str
    time_taken: float

class LoadDataRequest(BaseModel):
    table_name: str
    local_path: Optional[str] = None
    directory: Optional[str] = None
    partitioning: Optional[List[str]] = None

class LoadDataResponse(BaseModel):
    status: str
    time_taken: float
    metadata: Dict[str, Any]

class ExportDataRequest(BaseModel):
    table_name: str
    export_path: str
    partitioning: Optional[List[str]] = None

class ExportDataResponse(BaseModel):
    status: str
    time_taken: float

class GenerateDataResponse(BaseModel):
    status: str
    time_taken: float

async def ingest_to_postgres(table_name: str, reader):
    try:
        with conn.cursor() as cur:
            cur.adbc_ingest(table_name, reader, mode="create_append")
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

def generate_metadata(dataset):
    schema_dict = {field.name: str(field.type) for field in dataset.schema}
    num_rows = sum(fragment.count_rows() for fragment in dataset.get_fragments())
    return {
        "num_rows": num_rows,
        "num_columns": len(dataset.schema),
        "schema": schema_dict
    }

@app.post("/upload-file/", response_model=FileUploadResponse)
async def upload_file(file: UploadFile):
    start_time = time.time()
    try:
        local_path = os.path.join(base_dir, file.filename)
        with open(local_path, "wb") as out_file:
            out_file.write(file.file.read())
        end_time = time.time()
        return {"filename": file.filename, "time_taken": end_time - start_time}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/load-csv/", response_model=LoadDataResponse)
async def load_csv_from_local(data_params: LoadDataRequest):
    start_time = time.time()
    try:
        if not data_params.local_path:
            raise HTTPException(status_code=400, detail="local_path must be provided for loading CSV.")
        local_file_path = os.path.join(base_dir, data_params.local_path)
        table = pv.read_csv(local_file_path)
        await ingest_to_postgres(data_params.table_name, table)
        end_time = time.time()
        metadata = generate_metadata(table)
        return {"status": "CSV data loaded into PostgreSQL", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/load-parquet/", response_model=LoadDataResponse)
async def load_parquet_from_local(data_params: LoadDataRequest):
    start_time = time.time()
    try:
        if data_params.directory:
            dataset_path = os.path.join(base_dir, data_params.directory)
            dataset = ds.dataset(dataset_path, format="parquet", partitioning=data_params.partitioning)
            table = dataset.to_table()
        elif data_params.local_path:
            local_file_path = os.path.join(base_dir, data_params.local_path)
            table = pq.read_table(local_file_path)
        else:
            raise HTTPException(status_code=400, detail="Either local_path or directory must be provided.")

        await ingest_to_postgres(data_params.table_name, table)
        end_time = time.time()
        metadata = generate_metadata(dataset if data_params.directory else ds.dataset(local_file_path, format="parquet"))
        return {"status": "Parquet data loaded into PostgreSQL", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/load-arrow/", response_model=LoadDataResponse)
async def load_arrow_from_local(data_params: LoadDataRequest):
    start_time = time.time()
    try:
        if data_params.directory:
            dataset_path = os.path.join(base_dir, data_params.directory)
            dataset = ds.dataset(dataset_path, format="arrow", partitioning=data_params.partitioning)
            table = dataset.to_table()
            metadata = generate_metadata(dataset)
        elif data_params.local_path:
            local_file_path = os.path.join(base_dir, data_params.local_path)
            dataset = ds.dataset(local_file_path, format="arrow")
            table = dataset.to_table()
            metadata = generate_metadata(dataset)
        else:
            raise HTTPException(status_code=400, detail="Either local_path or directory must be provided.")

        await ingest_to_postgres(data_params.table_name, table)
        end_time = time.time()
        return {"status": "Arrow data loaded into PostgreSQL", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-parquet/", response_model=ExportDataResponse)
async def export_parquet(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_dir = os.path.join(base_dir, data_params.export_path)
        os.makedirs(export_dir, exist_ok=True)

        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {data_params.table_name}")
            table = cur.fetch_arrow_table()
        
        if data_params.partitioning:
            ds.write_dataset(table, export_dir, format="parquet", partitioning=data_params.partitioning)
        else:
            pq.write_table(table, os.path.join(export_dir, "data.parquet"))
        
        end_time = time.time()
        return {"status": "Table exported to Parquet", "time_taken": end_time - start_time}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-arrow/", response_model=ExportDataResponse)
async def export_arrow(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_file_path = os.path.join(base_dir, data_params.export_path)
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {data_params.table_name}")
            table = cur.fetch_arrow_table()
        if data_params.partitioning:
            ds.write_dataset(table, export_file_path, format="feather", partitioning=data_params.partitioning)
        else:
            feather.write_feather(table, export_file_path)
        end_time = time.time()
        return {"status": "Table exported to Arrow (Feather)", "time_taken": end_time - start_time}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-parquet-connectorx/", response_model=ExportDataResponse)
async def export_parquet_connectorx(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_dir = os.path.join(base_dir, data_params.export_path)
        os.makedirs(export_dir, exist_ok=True)

        # Construct the query
        query = f"SELECT * FROM {data_params.table_name}"

        # Fetch the data using ConnectorX
        if data_params.partitioning:
            partition_col = data_params.partitioning[0]
            table = cx.read_sql(uri, query, return_type="arrow2", partition_on=partition_col, partition_num=10)
        else:
            table = cx.read_sql(uri, query, return_type="arrow2")

        # Export to Parquet
        if data_params.partitioning:
            ds.write_dataset(table, export_dir, format="parquet", partitioning=data_params.partitioning)
        else:
            pq.write_table(table, os.path.join(export_dir, "data.parquet"))

        end_time = time.time()
        return {"status": "Table exported to Parquet using ConnectorX", "time_taken": end_time - start_time}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-arrow-connectorx/", response_model=ExportDataResponse)
async def export_arrow_connectorx(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_file_path = os.path.join(base_dir, data_params.export_path)
        os.makedirs(os.path.dirname(export_file_path), exist_ok=True)

        # Construct the query
        query = f"SELECT * FROM {data_params.table_name}"

        # Fetch the data using ConnectorX
        if data_params.partitioning:
            partition_col = data_params.partitioning[0]
            table = cx.read_sql(
                uri, query, return_type="arrow2", 
                protocol="binary",  # Adjust the protocol if needed
                partition_on=partition_col, partition_num=10
            )
        else:
            table = cx.read_sql(uri, query, return_type="arrow2", protocol="binary")

        # Export to Arrow (Feather)
        if data_params.partitioning:
            ds.write_dataset(table, export_file_path, format="feather", partitioning=data_params.partitioning)
        else:
            feather.write_feather(table, export_file_path)

        end_time = time.time()
        return {"status": "Table exported to Arrow using ConnectorX", "time_taken": end_time - start_time}
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
