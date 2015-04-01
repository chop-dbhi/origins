package main

import (
	"github.com/chop-dbhi/origins/peer"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var httpCmd = &cobra.Command{
	Use: "http",

	Short: "Starts a http process.",

	Long: "Runs a process exposing an HTTP interface.",

	Run: func(cmd *cobra.Command, args []string) {
		peer.ServeHTTP()
	},
}

func init() {
	flags := httpCmd.Flags()

	flags.String("host", "", "The host the HTTP service will listen on.")
	flags.Int("port", 49110, "The port the HTTP will bind to.")

	viper.BindPFlag("http.host", flags.Lookup("host"))
	viper.BindPFlag("http.port", flags.Lookup("port"))
}
