package main

import (
	"context"
	"fmt"
	"os"

	"github.com/yokomotod/yuccadb/server"
)

func main() {
	err := runServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}

func runServer() error {
	server, err := server.NewServer(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create server: %s", err)
	}

	return server.Run()
}
