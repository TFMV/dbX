import pyarrow.parquet as pq

def print_metadata(file_path):
    parquet_file = pq.ParquetFile(file_path)
    print("File:", file_path)
    print("Metadata:")
    print(parquet_file.metadata)
    print("Schema:")
    print(parquet_file.schema)
    print("\n")

export_connectorx_file = "/Users/thomasmcgeehan/dbX/dbX/data/flights/1/part-0.parquet"
export_data_file = "/Users/thomasmcgeehan/dbX/dbX/data/flights2/1/part-0.parquet"

print_metadata(export_connectorx_file)
print_metadata(export_data_file)
