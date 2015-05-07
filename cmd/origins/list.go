package main

import (
	"fmt"
	"os"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/identity"
	"github.com/chop-dbhi/origins/view"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var listCmd = &cobra.Command{
	Use: "list <domain> (entities | attributes | values)",

	Short: "Print a list of identities for the specified type.",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			cmd.Usage()
			os.Exit(1)
		}

		var (
			err      error
			min, max int64

			smin = viper.GetString("list_min")
			smax = viper.GetString("list_max")
		)

		min, err = origins.ParseTime(smin)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		max, err = origins.ParseTime(smax)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		filter := viper.GetString("list_filter")

		// Validate filter
		switch filter {
		case "all", "local", "external":
			break
		default:
			fmt.Printf("Unknown filter %s. Choices are: all, local, external\n", filter)
			os.Exit(1)
		}

		// Domain to read from.
		domain := args[0]

		store := initStore()

		v := view.Range(store, min, max)
		dv := v.Domain(domain)

		var ids identity.Idents

		switch args[1] {
		case "entity", "entities":
			switch filter {
			case "all":
				ids = dv.Entities()
			case "local":
				ids = dv.LocalEntities()
			case "external":
				ids = dv.ExternalEntities()
			}
			break
		case "attribute", "attributes":
			switch filter {
			case "all":
				ids = dv.Attributes()
			case "local":
				ids = dv.LocalAttributes()
			case "external":
				ids = dv.ExternalAttributes()
			}
			break
		case "value", "values":
			switch filter {
			case "all":
				ids = dv.Values()
			case "local":
				ids = dv.LocalValues()
			case "external":
				ids = dv.ExternalValues()
			}
			break
		default:
			fmt.Printf("Invalid identity type: %v\n", args[1])
			os.Exit(1)
		}

		for _, id := range ids {
			fmt.Println(id.String())
		}
	},
}

func init() {
	flags := listCmd.Flags()

	flags.String("min", "", "The min tranaction time of facts to read.")
	flags.String("max", "", "The max tranaction time of facts to read.")
	flags.String("filter", "local", "Filters the identities. Choices are: all, local, external.")

	viper.BindPFlag("list_min", flags.Lookup("min"))
	viper.BindPFlag("list_max", flags.Lookup("max"))
	viper.BindPFlag("list_filter", flags.Lookup("filter"))
}
