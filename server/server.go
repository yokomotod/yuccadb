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

func (s *server) setupRouter() *gin.Engine {
	r := gin.Default()

	r.GET("/tables", s.GetTables)
	r.PUT("/tables/:table", s.PutTable)
	r.GET("/tables/:table/:key", s.GetValue)

	return r
}

func (s *server) Run(addr string) error {
	r := s.setupRouter()

	return r.Run(addr)
}

type putTableReq struct {
	File    string `json:"file" binding:"required"`
	Replace bool   `json:"replace"`
}

func (s *server) GetTables(c *gin.Context) {
	c.JSON(200, gin.H{
		"tables": s.db.Tables(),
	})
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
	res, err := s.db.GetValue(tableName, key)

	if err != nil {
		c.JSON(500, gin.H{
			"message": fmt.Sprintf("failed to get value: %s", err),
		})
		return
	}
	if !res.TableExists {
		c.JSON(404, gin.H{
			"message": fmt.Sprintf("%s table is not found", tableName),
		})
		return
	}
	if !res.KeyExists {
		c.JSON(200, gin.H{
			"value": nil,
		})
		return
	}

	c.JSON(200, gin.H{
		"value": res.Value,
	})
}
