package godb

import (
	"flag"
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	flag.Parse()
    fmt.Println("Testing with Iterator Batch Size:", BatchSize)
	fmt.Println("Benchmarking with Table Size:", TableSize)

    exitCode := m.Run()
    os.Exit(exitCode)
}