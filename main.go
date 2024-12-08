package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/chzyer/readline"
	"github.com/srmadden/godb"
)

var helpText = `Enter a SQL query terminated by a ; to process it.  Commands prefixed with \ are processed as shell commands.

Available shell commands:
	\h : This help
	\c path/to/catalog : Change the current database to a specified catalog file
	\d : List tables and fields in the current database
	\f : List available functions for use in queries
	\a : Toggle aligned vs csv output
    \o : Toggle query optimization
	\l table path/to/file [sep] [hasHeader]: Append csv file to end of table.  Default to sep = ',', hasHeader = 'true'
	\z : Compute statistics for the database`

func printCatalog(c *godb.Catalog) {
	s := c.CatalogString()
	fmt.Printf("\033[34m%s\n\033[0m", s)
}

func main() {
	flag.Parse()
	fmt.Printf("Iterator Batch Size: ", godb.BatchSize)
	alarm := make(chan int, 1)

	go func() {
		c := make(chan os.Signal)

		signal.Notify(c, os.Interrupt, syscall.SIGINT)
		go func() {
			for {
				<-c
				alarm <- 1
				fmt.Println("Interrupted query.")
			}
		}()

	}()

	bp, err := godb.NewBufferPool(10000)
	if err != nil {
		log.Fatal(err.Error())
	}

	catName := "catalog.txt"
	catPath := "godb"

	c, err := godb.NewCatalogFromFile(catName, bp, catPath)
	if err != nil {
		fmt.Printf("failed load catalog, %s", err.Error())
		return
	}
	rl, err := readline.New("> ")
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	fmt.Printf("\033[35;1m")
	fmt.Println(`Welcome to

	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;93;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;93;48;5;54m░[38;5;55;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;93;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;55;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;92;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;129;48;5;232m [38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;16;48;5;16m▓[38;5;91;48;5;54m░[38;5;128;48;5;54m░[38;5;91;48;5;54m░[38;5;91;48;5;54m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;53;48;5;53m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;170;48;5;5m░[38;5;53;48;5;53m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;5m░[38;5;163;48;5;5m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;200;48;5;89m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;163;48;5;90m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;16;48;5;16m▓[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;212;48;5;53m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;199;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;162;48;5;126m░[38;5;125;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;125;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;89m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;162;48;5;126m░[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[38;5;16;48;5;16m▓[0m
	[0m
[35;1mType \h for help`)
	fmt.Printf("\033[0m\n")
	query := ""
	var autocommit bool = true
	var tid godb.TransactionID
	aligned := true
	for {
		text, err := rl.Readline()
		if err != nil { // io.EOF
			break
		}
		text = strings.TrimSpace(text)
		if len(text) == 0 {
			continue
		}
		if text[0] == '\\' {
			switch text[1] {
			case 'd':
				printCatalog(c)
			case 'c':
				if len(text) <= 3 {
					fmt.Printf("Expected catalog file name after \\c")
					continue
				}
				rest := text[3:]
				pathAr := strings.Split(rest, "/")
				catName = pathAr[len(pathAr)-1]
				catPath = strings.Join(pathAr[0:len(pathAr)-1], "/")
				c, err = godb.NewCatalogFromFile(catName, bp, catPath)
				if err != nil {
					fmt.Printf("failed load catalog, %s\n", err.Error())
					continue
				}
				fmt.Printf("Loaded %s/%s\n", catPath, catName)
				printCatalog(c)
			case 'f':
				fmt.Println("Available functions:")
				fmt.Print(godb.ListOfFunctions())
			case 'a':
				aligned = !aligned
				if aligned {
					fmt.Println("Output aligned")
				} else {
					fmt.Println("Output unaligned")
				}
			case 'o':
				godb.EnableJoinOptimization = !godb.EnableJoinOptimization
				if godb.EnableJoinOptimization {
					fmt.Println("\033[32;1mOptimization enabled\033[0m\n\n")
				} else {
					fmt.Println("\033[32;1mOptimization disabled\033[0m\n\n")
				}
			case 'z':
				c.ComputeTableStats()
				fmt.Printf("\033[32;1mAnalysis Complete\033[0m\n\n")
			case '?':
				fallthrough
			case 'h':
				fmt.Println(helpText)
			case 'l':
				splits := strings.Split(text, " ")
				table := splits[1]
				path := splits[2]
				sep := ","
				hasHeader := true
				if len(splits) > 3 {
					sep = splits[3]
				}
				if len(splits) > 4 {
					hasHeader = splits[4] != "false"
				}

				//todo -- following code assumes data is in heap files
				hf, err := c.GetTable(table)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				heapFile := hf.(*godb.HeapFile)
				f, err := os.Open(path)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				err = heapFile.LoadFromCSV(f, hasHeader, sep, false)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
				bp.FlushAllPages() //gross, if in a transaction, but oh well!
				fmt.Printf("\033[32;1mLOAD\033[0m\n\n")
			}

			query = ""
			continue
		}
		if text[len(text)-1] != ';' {
			query = query + " " + text
			continue
		}
		query = strings.TrimSpace(query + " " + text[0:len(text)-1])

		explain := false
		if strings.HasPrefix(strings.ToLower(query), "explain") {
			queryParts := strings.Split(query, " ")
			query = strings.Join(queryParts[1:], " ")
			explain = true
		}

		queryType, plan, err := godb.Parse(c, query)
		query = ""
		nresults := 0

		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "position") {
				positionPos := strings.LastIndex(errStr, "position")
				positionPos += 9

				spacePos := strings.Index(errStr[positionPos:], " ")
				if spacePos == -1 {
					spacePos = len(errStr) - positionPos
				}

				posStr := errStr[positionPos : spacePos+positionPos]
				pos, err := strconv.Atoi(posStr)
				if err == nil {
					s := strings.Repeat(" ", pos)
					fmt.Printf("\033[31;1m%s^\033[0m\n", s)
				}
			}
			fmt.Printf("\033[31;1mInvalid query (%s)\033[0m\n", err.Error())
			continue
		}

		switch queryType {
		case godb.UnknownQueryType:
			fmt.Printf("\033[31;1mUnknown query type\033[0m\n")
			continue

		case godb.IteratorType:
			if explain {
				fmt.Printf("\033[32m")
				godb.PrintPhysicalPlan(plan, "")
				fmt.Printf("\033[0m\n")
				break
			}
			if autocommit {
				tid = godb.NewTID()
				err := bp.BeginTransaction(tid)
				if err != nil {
					fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
					continue
				}
			}
			start := time.Now()

			iter, err := plan.Iterator(tid)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
				continue
			}

			fmt.Printf("\033[32;4m%s\033[0m\n", plan.Descriptor().HeaderString(aligned))

			for {
				batch, err := iter()
				if err != nil {
					fmt.Printf("%s\n", err.Error())
					break
				}
				if len(batch) == 0 {
					break
				}
				for _, tup := range batch {
					fmt.Printf("\033[32m%s\033[0m\n", tup.PrettyPrintString(aligned))
					nresults++
				}
				select {
				case <-alarm:
					fmt.Println("Aborting")
					goto outer
				default:
				}
			}
			if autocommit {
				bp.CommitTransaction(tid)
			}
		outer:
			fmt.Printf("\033[32;1m(%d results)\033[0m\n", nresults)
			duration := time.Since(start)
			fmt.Printf("\033[32;1m%v\033[0m\n\n", duration)

		case godb.BeginXactionType:
			if !autocommit {
				fmt.Printf("\033[31;1m%s\033[0m\n", "Cannot start transaction while in transaction")
				continue
			}
			tid = godb.NewTID()
			err := bp.BeginTransaction(tid)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
				continue
			}
			autocommit = false
			fmt.Printf("\033[32;1mBEGIN\033[0m\n\n")

		case godb.AbortXactionType:
			if autocommit {
				fmt.Printf("\033[31;1m%s\033[0m\n", "Cannot abort transaction unless in transaction")
				continue
			}
			bp.AbortTransaction(tid)
			autocommit = true
			fmt.Printf("\033[32;1mABORT\033[0m\n\n")
		case godb.CommitXactionType:
			if autocommit {
				fmt.Printf("\033[31;1m%s\033[0m\n", "Cannot commit transaction unless in transaction")
				continue
			}
			bp.CommitTransaction(tid)
			autocommit = true
			fmt.Printf("\033[32;1mCOMMIT\033[0m\n\n")
		case godb.CreateTableQueryType:
			fmt.Printf("\033[32;1mCREATE\033[0m\n\n")
			err := c.SaveToFile(catName, catPath)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
			}
		case godb.DropTableQueryType:
			fmt.Printf("\033[32;1mDROP\033[0m\n\n")
			err := c.SaveToFile(catName, catPath)
			if err != nil {
				fmt.Printf("\033[31;1m%s\033[0m\n", err.Error())
			}
		}
	}
}
