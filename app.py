from fastapi import FastAPI, UploadFile, HTTPException
from pydantic import BaseModel
from typing import Any, Dict
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.feather as feather
import pyarrow as pa
import adbc_driver_postgresql.dbapi
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
    local_path: str

class LoadDataResponse(BaseModel):
    status: str
    time_taken: float
    metadata: Dict[str, Any]

class ExportDataRequest(BaseModel):
    table_name: str
    export_path: str

class ExportDataResponse(BaseModel):
    status: str
    time_taken: float

async def ingest_to_postgres(table_name: str, reader):
    with conn.cursor() as cur:
        cur.adbc_ingest(table_name, reader, mode="create_append")
    conn.commit()

def generate_metadata(table):
    schema_dict = {field.name: str(field.type) for field in table.schema}
    return {
        "num_rows": table.num_rows,
        "num_columns": table.num_columns,
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
        local_file_path = os.path.join(base_dir, data_params.local_path)
        table = pq.read_table(local_file_path)
        await ingest_to_postgres(data_params.table_name, table)
        end_time = time.time()
        metadata = generate_metadata(table)
        return {"status": "Parquet data loaded into PostgreSQL", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/load-arrow/", response_model=LoadDataResponse)
async def load_arrow_from_local(data_params: LoadDataRequest):
    start_time = time.time()
    try:
        local_file_path = os.path.join(base_dir, data_params.local_path)
        table = ds.dataset(local_file_path, format="arrow").to_table()
        await ingest_to_postgres(data_params.table_name, table)
        end_time = time.time()
        metadata = generate_metadata(table)
        return {"status": "Arrow data loaded into PostgreSQL", "time_taken": end_time - start_time, "metadata": metadata}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-parquet/", response_model=ExportDataResponse)
async def export_parquet(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_file_path = os.path.join(base_dir, data_params.export_path)
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {data_params.table_name}")
            table = cur.fetch_arrow_table()
        pq.write_table(table, export_file_path)
        end_time = time.time()
        return {"status": "Table exported to Parquet", "time_taken": end_time - start_time}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export-arrow/", response_model=ExportDataResponse)
async def export_arrow(data_params: ExportDataRequest):
    start_time = time.time()
    try:
        export_file_path = os.path.join(base_dir, data_params.export_path)
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {data_params.table_name}")
            table = cur.fetch_arrow_table()
        feather.write_feather(table, export_file_path)
        end_time = time.time()
        return {"status": "Table exported to Arrow (Feather)", "time_taken": end_time - start_time}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
