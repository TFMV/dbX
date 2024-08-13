package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

type response struct {
	RowsWritten    int64         `json:"rows_written"`
	Message        string        `json:"message"`
	Duration       time.Duration `json:"duration"`
	OutputFileSize int64         `json:"output_file_size"`
}

func main() {
	tableName := flag.String("table", "", "Name of the table to export")
	filePath := flag.String("file", "", "Path to the Parquet file to import")
	flag.Parse()

	if *tableName != "" {
		startTime := time.Now()
		resp, err := exportTable(*tableName)
		duration := time.Since(startTime)

		if err != nil {
			log.Fatalf("Failed to export table: %v", err)
		}

		fmt.Printf("Rows written: %d\nMessage: %s\nDuration: %v\nOutput file size: %d bytes\n", resp.RowsWritten, resp.Message, duration, resp.OutputFileSize)
	} else if *filePath != "" {
		if err := checkParquetFile(*filePath); err != nil {
			log.Fatalf("Failed to check Parquet file: %v", err)
		}
	} else {
		if err := insertArrowData(); err != nil {
			log.Fatalf("Failed to insert Arrow data: %v", err)
		}
	}
}

func exportTable(tableName string) (*response, error) {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":          "/usr/local/lib/libadbc_driver_postgresql.dylib",
		adbc.OptionKeyURI: "postgresql://postgres:notapassword@localhost:5432/tfmv",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ADBC database: %w", err)
	}

	cnxn, err := db.Open(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to open ADBC connection: %w", err)
	}
	defer cnxn.Close()

	ctx := context.Background()
	// pool := memory.NewGoAllocator()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}
	defer stmt.Close()

	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	if err := stmt.SetSqlQuery(query); err != nil {
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer reader.Release()

	parquetFile, err := os.Create("output.parquet")
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet file: %w", err)
	}
	defer parquetFile.Close()

	parquetWriter, err := pqarrow.NewFileWriter(reader.Schema(), parquetFile, nil, pqarrow.ArrowWriterProperties{})
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer parquetWriter.Close()

	rowsWritten := int64(0)
	for reader.Next() {
		record := reader.Record()
		if record == nil {
			continue
		}
		if err := parquetWriter.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write record to Parquet file: %w", err)
		}
		rowsWritten += record.NumRows()
		record.Release()
	}

	if err := parquetWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	fileInfo, err := os.Stat("output.parquet")
	if err != nil {
		return nil, fmt.Errorf("failed to get output file info: %w", err)
	}

	return &response{
		RowsWritten:    rowsWritten,
		Message:        "Data successfully written to Parquet file",
		OutputFileSize: fileInfo.Size(),
	}, nil
}

func insertArrowData() error {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":          "/usr/local/lib/libadbc_driver_postgresql.dylib",
		adbc.OptionKeyURI: "postgresql://postgres:notapassword@localhost:5432/tfmv",
	})
	if err != nil {
		return fmt.Errorf("failed to create ADBC database: %w", err)
	}

	cnxn, err := db.Open(context.Background())
	if err != nil {
		return fmt.Errorf("failed to open ADBC connection: %w", err)
	}
	defer cnxn.Close()

	ctx := context.Background()
	pool := memory.NewGoAllocator()

	// Create a simple Arrow schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	// Create a new record batch
	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

	record := bldr.NewRecord()
	defer record.Release()

	// Create a RecordReader that will read our single record
	stream, err := array.NewRecordReader(schema, []arrow.Record{record})
	if err != nil {
		return fmt.Errorf("failed to create record reader: %w", err)
	}
	defer stream.Release()

	// Prepare a statement for inserting data
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create statement: %w", err)
	}
	defer stmt.Close()

	// Set the SQL command to create the target table
	createTable := `
	CREATE TABLE IF NOT EXISTS hello (
		id INTEGER,
		name TEXT
	)`
	if err := stmt.SetSqlQuery(createTable); err != nil {
		return fmt.Errorf("failed to set SQL query: %w", err)
	}

	// Execute the table creation query
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Prepare the insertion statement
	insertQuery := `INSERT INTO hello (id, name) VALUES ($1, $2)`
	if err := stmt.SetSqlQuery(insertQuery); err != nil {
		return fmt.Errorf("failed to set SQL query: %w", err)
	}

	// Bind the Arrow data stream to the statement
	if err := stmt.BindStream(ctx, stream); err != nil {
		return fmt.Errorf("failed to bind stream: %w", err)
	}

	// Execute the insertion of the Arrow data
	if _, err := stmt.ExecuteUpdate(ctx); err != nil {
		// Rollback the transaction if something goes wrong
		_ = cnxn.Rollback(ctx)
		return fmt.Errorf("failed to execute update: %w", err)
	}

	fmt.Println("Successfully inserted Arrow data into PostgreSQL table")
	return nil
}

func checkParquetFile(filePath string) error {
	ctx := context.Background()
	pool := memory.NewGoAllocator()

	// Open the Parquet file
	parquetFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open Parquet file: %w", err)
	}
	defer parquetFile.Close()

	// Create a Parquet reader
	pqFileReader, err := file.NewParquetReader(parquetFile)
	if err != nil {
		return fmt.Errorf("failed to create Parquet reader: %w", err)
	}

	// Create an Arrow FileReader from the Parquet reader
	pqReader, err := pqarrow.NewFileReader(pqFileReader, pqarrow.ArrowReadProperties{}, pool)
	if err != nil {
		return fmt.Errorf("failed to create Parquet file reader: %w", err)
	}

	// Read the entire file into an Arrow Table
	table, err := pqReader.ReadTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to read Parquet file into Arrow table: %w", err)
	}
	defer table.Release()

	// Print basic information about the table
	fmt.Printf("Parquet file: %s\n", filePath)
	fmt.Printf("Schema: %s\n", table.Schema())
	fmt.Printf("Number of rows: %d\n", table.NumRows())
	fmt.Printf("Number of columns: %d\n", table.NumCols())

	return nil
}
