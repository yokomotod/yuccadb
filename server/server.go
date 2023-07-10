package server

import (
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/yokomotod/yuccadb"
)

type server struct {
	tables map[string]*yuccadb.SSTable
}

func NewServer() *server {
	return &server{
		tables: make(map[string]*yuccadb.SSTable),
	}
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

	table, err := yuccadb.NewSSTable(ctx, req.File)
	if err != nil {
		c.JSON(500, gin.H{
			"message": fmt.Sprintf("failed to create table: %s", err),
		})
		return
	}

	s.tables[tableName] = table

	c.JSON(200, gin.H{
		"message": fmt.Sprintf("%s table is created", tableName),
	})
}

func (s *server) GetValue(c *gin.Context) {
	tableName := c.Param("table")
	ssTable, ok := s.tables[tableName]
	if !ok {
		c.JSON(404, gin.H{
			"message": fmt.Sprintf("%s table is not found", tableName),
		})
		return
	}

	key := c.Param("key")
	value, err := ssTable.Get(key)
	if err != nil {
		c.JSON(500, gin.H{
			"message": fmt.Sprintf("failed to get value: %s", err),
		})
		return
	}

	c.JSON(200, gin.H{
		"value": value,
	})
}
