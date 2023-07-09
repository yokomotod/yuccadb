package main

import (
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
	return server.NewServer().Run()
}
