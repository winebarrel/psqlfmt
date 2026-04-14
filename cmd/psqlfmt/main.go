package main

import (
	"fmt"
	"io"
	"os"

	"github.com/winebarrel/psqlfmt"
)

func main() {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading input: %v\n", err)
		os.Exit(1)
	}

	output, err := psqlfmt.Format(string(input))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error formatting SQL: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(output)
}
