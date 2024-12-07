package godb

import "flag"

// global variable to set the batch size
var BatchSize int
var TableSize int

func init() {
    flag.IntVar(&BatchSize, "batchSize", 1, "Specify the batch size for the iterator")
    flag.IntVar(&TableSize, "tableSize", 10000, "Specify the size of the database tables")
}