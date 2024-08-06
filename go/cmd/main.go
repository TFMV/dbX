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
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

type response struct {
	RowsWritten    int64         `json:"rows_written"`
	Message        string        `json:"message"`
	Duration       time.Duration `json:"duration"`
	OutputFileSize int64         `json:"output_file_size"`
}

func main() {
	// Define command-line arguments
	tableName := flag.String("table", "", "Name of the table to export")
	flag.Parse()

	// Validate the input
	if *tableName == "" {
		log.Fatalf("Table name is required")
	}

	// Call the exportTable function
	startTime := time.Now()
	resp, err := exportTable(*tableName)
	duration := time.Since(startTime)

	if err != nil {
		log.Fatalf("Failed to export table: %v", err)
	}

	// Print the response
	fmt.Printf("Rows written: %d\nMessage: %s\nDuration: %v\nOutput file size: %d bytes\n", resp.RowsWritten, resp.Message, duration, resp.OutputFileSize)
}

func exportTable(tableName string) (*response, error) {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":          "adbc_driver_postgresql",
		adbc.OptionKeyURI: "postgresql://tfmv:notapassword@localhost:5432/postgres",
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
	pool := memory.NewGoAllocator()

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

	props := parquet.NewWriterProperties(parquet.WithAllocator(pool))
	arrowProps := pqarrow.DefaultWriterProps()

	parquetWriter, err := pqarrow.NewFileWriter(reader.Schema(), parquetFile, props, arrowProps)
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
