package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/yokomotod/yuccadb/server"
)

var (
	port    = flag.String("bind", ":8080", "")
	dataDir = flag.String("datadir", "./data", "")
)

func main() {
	flag.Parse()

	err := runServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func runServer() error {
	server, err := server.NewServer(context.Background(), *dataDir)
	if err != nil {
		return fmt.Errorf("failed to create server: %s", err)
	}

	return server.Run(*port)
}
