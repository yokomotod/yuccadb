package server

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/yokomotod/yuccadb"
)

type server struct {
	db *yuccadb.YuccaDB
}

func NewServer(ctx context.Context) (*server, error) {
	db, err := yuccadb.NewYuccaDB(ctx, "./data")
	if err != nil {
		return nil, fmt.Errorf("failed to create db: %s", err)
	}

	return &server{
		db: db,
	}, nil
}

func (s *server) Run() error {
	r := gin.Default()

	r.PUT("/:table", s.PutTable)
	r.GET("/:table/:key", s.GetValue)

	return r.Run()
}

type putTableReq struct {
	File string `json:"file" binding:"required"`
}

func (s *server) PutTable(c *gin.Context) {
	ctx := c.Request.Context()
	tableName := c.Param("table")

	var req putTableReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{
			"message": fmt.Sprintf("failed to bind json: %s", err),
		})
		return
	}

	if err := s.db.CreateTable(ctx, tableName, req.File); err != nil {
		c.JSON(500, gin.H{
			"message": fmt.Sprintf("failed to create table: %s", err),
		})
		return
	}

	c.JSON(200, gin.H{
		"message": fmt.Sprintf("%s table is created", tableName),
	})
}

func (s *server) GetValue(c *gin.Context) {
	tableName, key := c.Param("table"), c.Param("key")
	value, tableExists, keyExists, err := s.db.GetValue(tableName, key)

	if !tableExists {
		c.JSON(404, gin.H{
			"message": fmt.Sprintf("%s table is not found", tableName),
		})
		return
	}

	if err != nil {
		c.JSON(500, gin.H{
			"message": fmt.Sprintf("failed to get value: %s", err),
		})
		return
	}
	if !keyExists {
		c.JSON(200, gin.H{
			"value": nil,
		})
		return
	}

	c.JSON(200, gin.H{
		"value": value,
	})
}
