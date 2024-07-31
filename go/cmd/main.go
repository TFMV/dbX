package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/gin-gonic/gin"
)

type request struct {
	TableName string `json:"table_name"`
}

type response struct {
	RowsWritten int64  `json:"rows_written"`
	Message     string `json:"message"`
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	r := gin.Default()
	r.POST("/export", exportHandler)
	log.Printf("Starting server on port %s...\n", port)
	r.Run(fmt.Sprintf(":%s", port))
}

func exportHandler(c *gin.Context) {
	var req request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request payload"})
		return
	}

	// Validate the input
	if req.TableName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Table name is required"})
		return
	}

	resp, err := exportTable(req.TableName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to export table: %v", err)})
		return
	}

	c.JSON(http.StatusOK, resp)
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

	var rowsWritten int64
	for reader.Next() {
		record := reader.Record()
		if err := parquetWriter.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write Arrow table to Parquet file: %w", err)
		}
		rowsWritten += record.NumRows()
		record.Release()
	}

	return &response{
		RowsWritten: rowsWritten,
		Message:     "Data successfully written to Parquet file",
	}, nil
}
