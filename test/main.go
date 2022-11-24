package main

import (
	"fmt"
	"os"

	"github.com/sunblaze-ucb/simpleMPI/mpi"
)

func main() {
	fmt.Println("Hello, playground", os.Args)
	world := mpi.WorldInit("/etc/hosts", "config")
	fmt.Println(world)
	mpi.Close()
}
