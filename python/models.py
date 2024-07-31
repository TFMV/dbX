from pydantic import BaseModel
from typing import Any, Dict, List, Optional

class FileUploadResponse(BaseModel):
    filename: str
    time_taken: float

class LoadDataRequest(BaseModel):
    table_name: str
    local_path: Optional[str] = None
    directory: Optional[str] = None
    file_format: str
    partitioning: Optional[List[str]] = None
    database_role: str  # Can be 'source' or 'target'

class LoadDataResponse(BaseModel):
    status: str
    time_taken: float
    metadata: Dict[str, Any]

class ExportDataRequest(BaseModel):
    table_name: str
    export_path: str
    file_format: str
    partitioning: Optional[List[str]] = None
    database_role: str  # Can be 'source' or 'target'

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

class CopyTableResponse(BaseModel):
    status: str
    time_taken: float
    metadata: Dict[str, Any]
