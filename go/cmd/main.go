package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

func main() {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":          "adbc_driver_postgresql",
		adbc.OptionKeyURI: "postgresql://postgres:pass@localhost:5432/tfmv",
	})
	if err != nil {
		log.Fatalf("Failed to create ADBC database: %v", err)
	}

	cnxn, err := db.Open(context.Background())
	if err != nil {
		log.Fatalf("Failed to open ADBC connection: %v", err)
	}
	defer cnxn.Close()

	ctx := context.Background()

	// Specify the table name
	tableName := "flights_part"

	// Create an Arrow memory allocator
	pool := memory.NewGoAllocator()

	// Create a statement to execute a query
	stmt, err := cnxn.NewStatement()
	if err != nil {
		log.Fatalf("Failed to create statement: %v", err)
	}
	defer stmt.Close()

	// Set the query
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	if err := stmt.SetSqlQuery(query); err != nil {
		log.Fatalf("Failed to set SQL query: %v", err)
	}

	// Execute the query and fetch results as an Arrow table
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer reader.Release()

	// Create a Parquet file
	parquetFile, err := os.Create("output.parquet")
	if err != nil {
		log.Fatalf("Failed to create Parquet file: %v", err)
	}
	defer parquetFile.Close()

	// Create a Parquet writer from the Arrow schema
	props := parquet.NewWriterProperties(parquet.WithAllocator(pool))
	parquetWriter, err := pqarrow.NewFileWriter(reader.Schema(), parquetFile, props, pqarrow.DefaultWriterProps())
	if err != nil {
		log.Fatalf("Failed to create Parquet writer: %v", err)
	}
	defer parquetWriter.Close()

	// Write the Arrow table to the Parquet file
	for reader.Next() {
		record := reader.Record()
		if err := parquetWriter.Write(record); err != nil {
			log.Fatalf("Failed to write Arrow table to Parquet file: %v", err)
		}
		record.Release()
	}

	// Print a success message
	fmt.Println("Data successfully written to Parquet file")
}
