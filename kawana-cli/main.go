package main

import (
	"encoding/csv"
	"fmt"
	"github.com/chriskite/kawana/datastore"
	"github.com/codegangsta/cli"
	"os"
)

func kdbExport(c *cli.Context) {
	input := c.Args().First()
	output := c.String("output")

	if input == "" {
		fmt.Println("kdb-export: No input filename provided\n")
		cli.ShowAppHelp(c)
		os.Exit(1)
	}

	inputFile, err := os.Open(input)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	outputFile, err := os.Create(output)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	store := &datastore.IPDataMap{}
	dec := datastore.NewDecoder(inputFile)
	dec.Decode(store)

	writer := csv.NewWriter(outputFile)
	writer.Write(append([]string{"IP"}, datastore.IPDataHeaders()...))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for ip, data := range *store {
		record := append([]string{ip.String()}, data.Strings()...)
		err := writer.Write(record)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	writer.Flush()

	fmt.Println("Exported contents of " + input + " to " + output)
}

func main() {
	cli.AppHelpTemplate = `VERSION: {{.Version}}

USAGE:
   {{.Name}} {{if .Flags}}[global options] {{end}}command{{if .Flags}} [command options]{{end}} [arguments...]

COMMANDS:
   {{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
   {{end}}{{if .Flags}}
GLOBAL OPTIONS:
   {{range .Flags}}{{.}}
   {{end}}{{end}}
`

	app := cli.NewApp()
	app.EnableBashCompletion = true
	app.Commands = []cli.Command{
		{
			Name:   "kdb-export",
			Usage:  "read a .kdb file and export it as a .csv",
			Action: kdbExport,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "output",
					Value: "kawana.csv",
					Usage: "output csv filename",
				},
			},
		},
	}

	app.Run(os.Args)
}
