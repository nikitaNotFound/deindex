package server

import "github.com/spf13/cobra"

var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "",
}

var RunServerCmd = &cobra.Command{
	Use:   "run",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func init() {
	ServerCmd.AddCommand(RunServerCmd)
}
