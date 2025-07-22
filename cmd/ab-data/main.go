package main

import (
	"context"
	"fmt"

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
	// platform := prompt.Ask("What platform are you using? [Options are: postgres]\n")

	// if strings.ToLower(strings.TrimSpace(platform)) != "postgres" {
	// 	fmt.Println("Invalid platform")
	// 	return
	// }

	// dbHost := prompt.Ask("What's your db host?\n")
	// dbPort := prompt.Ask("What's your db port?\n")
	// dbUser := prompt.Ask("What's your db user?\n")
	// dbPassword := prompt.AskPassword("What's your db password?\n")
	// dbName := prompt.Ask("What's your db name?\n")
	// schemas := prompt.Ask("List all schemas that have data that you want to benchmark?[please use a comma separated list]\n")
	// schemaList := common.MapWithFilter(strings.Split(schemas, ","), func(schema string) (string, bool) {
	// 	return strings.TrimSpace(schema), schema != ""
	// })

	// // Launch postgres pod
	// sqlDB, _, err := core.SetupDatabase(context.Background(), dbHost, dbPort, dbUser, dbPassword, dbName, schemaList, []string{})
	// if err != nil {
	// 	fmt.Println("Error launching postgres pod", err)
	// 	tables := prompt.Ask("Try listing all tables with schemas that you want to benchmark?[please use a comma separated list]\n")
	// 	tablesList := common.MapWithFilter(strings.Split(tables, ","), func(table string) (string, bool) {
	// 		return strings.TrimSpace(table), table != ""
	// 	})
	// 	sqlDB, _, err = core.SetupDatabase(context.Background(), dbHost, dbPort, dbUser, dbPassword, dbName, []string{}, tablesList)
	// 	if err != nil {
	// 		fmt.Println("Error launching postgres pod", err)
	// 		return
	// 	}
	// }

	// Speed testing
	dbHost := "proddb.conversion.ai"
	dbPort := "5432"
	dbUser := "tapp"
	dbPassword := "tapp"
	dbName := "business"
	schemaList := []string{"crm"}
	tablesList := []string{"public.profiles"}
	sqlDB, _, err := core.SetupDatabase(context.Background(), dbHost, dbPort, dbUser, dbPassword, dbName, schemaList, tablesList)
	if err != nil {
		fmt.Println("Error launching postgres pod", err)
		return
	}

	fmt.Println("Connected to database")

	// Ask for the users migration script location absolute path and run it
	// migrationScriptPath := prompt.Ask("What's the absolute path to the migration script?\n")
	migrationScriptPath := "file://../../migrations"
	err = core.RunMigrationScript(sqlDB, migrationScriptPath)
	if err != nil {
		fmt.Println("Error running migration script", err)
		return
	}

	// rows, err := sqlDB.Query("SELECT * FROM crm.contacts limit 1")
	// if err != nil {
	// 	fmt.Println("Error querying contacts", err)
	// 	return
	// }
	// defer rows.Close()

	// for rows.Next() {
	// 	var id string
	// 	var businessId string
	// 	var email string
	// 	var companyId string
	// 	var createdAt time.Time
	// 	var updatedAt time.Time
	// 	var customVariables json.RawMessage
	// 	err := rows.Scan(&id, &businessId, &email, &companyId, &createdAt, &updatedAt, &customVariables)
	// 	if err != nil {
	// 		fmt.Println("Error scanning row", err)
	// 		return
	// 	}
	// 	fmt.Println(id, businessId, email, companyId, createdAt, updatedAt, customVariables)
	// }

	// Ask users for path to the queries file and it should unmarshal into a list of queries
	for {
		queriesPath := prompt.Ask("What's the relative path to the queries file?\n")
		// queriesPath := "../../queries/test_queries.json"
		queries, err := core.UnmarshalQueries(queriesPath)
		if err != nil {
			fmt.Println("Error unmarshalling queries", err)
			return
		}

		err = core.RunBenchmarkQueries(sqlDB, queries)
		if err != nil {
			fmt.Println("Error running benchmark queries", err)
			return
		}
	}

	// Run the data migration script and benchmark the results while the data is migrating
	// Note: Currently assuming it's a SQL script
	// dataMigrationScriptPath := prompt.Ask("What's the absolute path to the data migration script?\n")
	// err = core.RunDataMigrationScript(sqlDB, dataMigrationScriptPath, queries)
	// if err != nil {
	// 	fmt.Println("Error running data migration script", err)
	// 	return
	// }
}

// TODO:
// Test sorting,
// Test greater than, less than, etc.
// Test DISTINCT query
