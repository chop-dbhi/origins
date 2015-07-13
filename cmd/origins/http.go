package main

import (
	"strings"

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
		bindStorageFlags(cmd.Flags())

		allowedHosts := strings.Split(viper.GetString("http_allowed_hosts"), ",")

		server := http.Server{
			Engine:       initStorage(),
			Host:         viper.GetString("http_host"),
			Port:         viper.GetInt("http_port"),
			Debug:        logrus.GetLevel() == logrus.DebugLevel,
			AllowedHosts: allowedHosts,
		}

		server.Serve()
	},
}

func init() {
	flags := httpCmd.Flags()

	addStorageFlags(flags)

	flags.String("host", "", "The host the HTTP service will listen on.")
	flags.Int("port", 49110, "The port the HTTP will bind to.")
	flags.String("allowed-hosts", "*", "Set of allowed hosts for cross-origin resource sharing.")

	viper.BindPFlag("http_host", flags.Lookup("host"))
	viper.BindPFlag("http_port", flags.Lookup("port"))
	viper.BindPFlag("http_allowed_hosts", flags.Lookup("allowed-hosts"))
}
