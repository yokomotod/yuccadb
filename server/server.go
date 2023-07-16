package server

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/yokomotod/yuccadb"
)

type server struct {
	db    *yuccadb.YuccaDB
	nodes []string
}

func NewServer(ctx context.Context, dataDir string) (*server, error) {
	db, err := yuccadb.NewYuccaDB(ctx, dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create db: %s", err)
	}

	return &server{
		db:    db,
		nodes: []string{},
	}, nil
}

func (s *server) Run(addr string) error {
	r := gin.Default()

	r.PUT("/:table", s.PutTable)
	r.GET("/:table/:key", s.GetValue)

	return r.Run(addr)
}

type putTableReq struct {
	File    string `json:"file" binding:"required"`
	Replace bool   `json:"replace"`
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

	if !req.Replace && s.db.HasTable(tableName) {
		c.JSON(400, gin.H{
			"message": fmt.Sprintf("%s table already exists and replace is false", tableName),
		})
		return
	}

	if err := s.db.PutTable(ctx, tableName, req.File, req.Replace); err != nil {
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
