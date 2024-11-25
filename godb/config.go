package godb

import "flag"

// global variable to set the batch size
var BatchSize int

func init() {
    flag.IntVar(&BatchSize, "batchSize", 1, "Specify the batch size for the iterator")
}