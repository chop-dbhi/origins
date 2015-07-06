package main

import (
	"github.com/chop-dbhi/origins/http"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var httpCmd = &cobra.Command{
	Use: "http",

	Short: "Starts an HTTP service.",

	Long: "Runs a process exposing an HTTP interface.",

	Run: func(cmd *cobra.Command, args []string) {
		var (
			host = viper.GetString("http_host")
			port = viper.GetInt("http_port")
		)

		debug := logrus.GetLevel() == logrus.DebugLevel

		http.Serve(initStorage(), host, port, debug)
	},
}

func init() {
	flags := httpCmd.Flags()

	addStorageFlags(flags)

	flags.String("host", "", "The host the HTTP service will listen on.")
	flags.Int("port", 49110, "The port the HTTP will bind to.")

	viper.BindPFlag("http_host", flags.Lookup("host"))
	viper.BindPFlag("http_port", flags.Lookup("port"))
}
