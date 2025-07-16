package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/relentlo/ab-data/common"
	"github.com/relentlo/ab-data/internal/core"
	"github.com/relentlo/ab-data/internal/prompt"
)

// Go package for benchmarking database data schemas for a specific set of queries
func main() {
	fmt.Println("Welcome to AB Data")
	fmt.Println("This tool is used to benchmark database data schemas for a specific set of queries")
	fmt.Println("Please follow the prompts to setup your database and schemas")
	fmt.Println("Note: You'll need to have docker installed and running to use this tool")
	fmt.Println("--------------------------------")
	fmt.Println("--------------------------------")
	platform := prompt.Ask("What platform are you using? [Options are: postgres]\n")

	if strings.ToLower(strings.TrimSpace(platform)) != "postgres" {
		fmt.Println("Invalid platform")
		return
	}

	dbHost := prompt.Ask("What's your db host?\n")
	dbPort := prompt.Ask("What's your db port?\n")
	dbUser := prompt.Ask("What's your db user?\n")
	dbPassword := prompt.AskPassword("What's your db password?\n")
	dbName := prompt.Ask("What's your db name?\n")
	schemas := prompt.Ask("List all schemas that have data that you want to benchmark?[please use a comma separated list]\n")
	schemaList := common.MapWithFilter(strings.Split(schemas, ","), func(schema string) (string, bool) {
		return strings.TrimSpace(schema), schema != ""
	})

	// Launch postgres pod
	sqlDB, err := core.SetupDatabase(context.Background(), dbHost, dbPort, dbUser, dbPassword, dbName, schemaList, []string{})
	if err != nil {
		fmt.Println("Error launching postgres pod", err)
		tables := prompt.Ask("Try listing all tables with schemas that you want to benchmark?[please use a comma separated list]\n")
		tablesList := common.MapWithFilter(strings.Split(tables, ","), func(table string) (string, bool) {
			return strings.TrimSpace(table), table != ""
		})
		sqlDB, err = core.SetupDatabase(context.Background(), dbHost, dbPort, dbUser, dbPassword, dbName, []string{}, tablesList)
		if err != nil {
			fmt.Println("Error launching postgres pod", err)
			return
		}
	}

	// // For testing the postgres setup
	// rows, err := sqlDB.QueryContext(context.Background(), "SELECT * FROM clone_crm.variable_schemas LIMIT 1;")
	// if err != nil {
	// 	fmt.Println("Error querying database", err)
	// 	return
	// }
	// defer rows.Close()

	// columns, err := rows.Columns()
	// if err != nil {
	// 	fmt.Println("Error getting columns", err)
	// 	return
	// }
	// fmt.Println("Columns", columns)

	// Ask for the users migration script location absolute path and run it
	migrationScriptPath := prompt.Ask("What's the absolute path to the migration script?\n")
	err = core.RunMigrationScript(sqlDB, migrationScriptPath)
	if err != nil {
		fmt.Println("Error running migration script", err)
		return
	}

	rows, err := sqlDB.QueryContext(context.Background(), "SELECT * FROM crm.variable_schemas LIMIT 1;")
	if err != nil {
		fmt.Println("Error querying database", err)
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		fmt.Println("Error getting columns", err)
		return
	}
	fmt.Println("Columns", columns)

	// TODO: Ask users for path to the queries file and it should unmarshal into a list of queries

	// TODO: Run the data migration script and benchmark the results while the data is migrating

}
