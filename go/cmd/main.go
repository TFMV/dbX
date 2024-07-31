package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v17/arrow/arrio"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

type response struct {
	RowsWritten int64  `json:"rows_written"`
	Message     string `json:"message"`
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
	resp, err := exportTable(*tableName)
	if err != nil {
		log.Fatalf("Failed to export table: %v", err)
	}

	// Print the response
	fmt.Printf("Rows written: %d\nMessage: %s\n", resp.RowsWritten, resp.Message)
}

func exportTable(tableName string) (*response, error) {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":          "adbc_driver_postgresql",
		adbc.OptionKeyURI: "postgresql://postgres:notapassword@130.211.115.76:5432/postgres",
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
	parquetWriter, err := pqarrow.NewFileWriter(reader.Schema(), parquetFile, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer parquetWriter.Close()

	// Channel to pass records from readers to writers
	recordChan := make(chan arrio.Record, 100)
	var rowsWritten int64
	var wg sync.WaitGroup

	// Start a number of writer goroutines
	numWriters := 4
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for record := range recordChan {
				if err := parquetWriter.Write(record); err != nil {
					log.Fatalf("failed to write Arrow table to Parquet file: %v", err)
				}
				rowsWritten += record.NumRows()
				record.Release()
			}
		}()
	}

	// Read records and send them to the writers
	for reader.Next() {
		record := reader.Record()
		recordChan <- record
	}

	// Close the record channel and wait for all writers to finish
	close(recordChan)
	wg.Wait()

	return &response{
		RowsWritten: rowsWritten,
		Message:     "Data successfully written to Parquet file",
	}, nil
}
