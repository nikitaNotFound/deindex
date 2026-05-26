package main

import (
	"os"

	"github.com/nikitaNotFound/deindex/cmd/process"
	"github.com/nikitaNotFound/deindex/cmd/server"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "deindex",
	Short: "",
}

func init() {
	rootCmd.AddCommand(server.ServerCmd)
	rootCmd.AddCommand(process.ProcessCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
