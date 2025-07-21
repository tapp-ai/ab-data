package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	mpostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file" // import file driver for migrations
	_ "github.com/lib/pq"                                // import the PostgreSQL driver
	db "github.com/relentlo/ab-data/internal/database"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func SetupDatabase(ctx context.Context, dbHost string, dbPort string, dbUser string, dbPassword string, dbName string, schemaList []string, tablesList []string) (*sql.DB, *postgres.PostgresContainer, error) {
	postgresContainer, err := postgres.Run(ctx,
		"docker.io/postgres:15.2-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second)),
	)
	if err != nil {
		return nil, nil, err
	}

	// create businessDB for the entire test suite
	baseConnStr, err := postgresContainer.ConnectionString(context.Background(), "sslmode=disable")
	if err != nil {
		return nil, nil, err
	}

	realDB, err := db.Connect(context.Background(), baseConnStr)
	if err != nil {
		return nil, nil, err
	}

	realDB.SetMaxOpenConns(3)
	realDB.SetMaxIdleConns(1)

	// Set timeouts after connecting
	_, err = realDB.ExecContext(ctx, "SET statement_timeout = 0;")
	if err != nil {
		return nil, nil, err
	}

	_, err = realDB.ExecContext(ctx, "SET idle_in_transaction_session_timeout = 0;")
	if err != nil {
		return nil, nil, err
	}

	_, err = realDB.ExecContext(ctx, "SET lock_timeout = 0;")
	if err != nil {
		return nil, nil, err
	}

	// Note: We also need to create the extensions that the user has in the source database
	_, err = realDB.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS postgres_fdw;")
	if err != nil {
		return nil, nil, err
	}

	// TODO: Automate this for all the extensions that the user has in the source database and not just hstore
	_, err = realDB.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS hstore;")
	if err != nil {
		return nil, nil, err
	}

	_, err = realDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE SERVER source_srv FOREIGN DATA WRAPPER postgres_fdw OPTIONS ("+
			"host '%s', dbname '%s', port '%s', "+
			"fetch_size '1000', "+
			"extensions 'postgres_fdw', "+
			"keep_connections 'on' "+
			");",
		dbHost, dbName, dbPort))
	if err != nil {
		return nil, nil, err
	}

	_, err = realDB.ExecContext(ctx, fmt.Sprintf("CREATE USER MAPPING FOR tapp SERVER source_srv OPTIONS (user '%s', password '%s');", dbUser, dbPassword))
	if err != nil {
		return nil, nil, err
	}

	fmt.Println("Creating schemas")

	// Note: We might need to have a table fallback if we had covered the edge cases like enum loading here
	for _, schema := range schemaList {
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS clone_%s;", schema))
		if err != nil {
			return nil, nil, err
		}
		// Grant permissions to 'tapp' (the test container user), not dbUser
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA clone_%s TO tapp;", schema))
		if err != nil {
			return nil, nil, err
		}
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA clone_%s TO tapp;", schema))
		if err != nil {
			return nil, nil, err
		}
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA clone_%s GRANT SELECT ON TABLES TO tapp;", schema))
		if err != nil {
			return nil, nil, err
		}
		fmt.Println("Importing foreign schema", schema)
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("IMPORT FOREIGN SCHEMA %s FROM SERVER source_srv INTO clone_%s;", schema, schema))
		if err != nil {
			return nil, nil, err
		}
		fmt.Println("Created schema", schema)
	}

	mapSchemaToTables := make(map[string][]string)
	for _, table := range tablesList {
		schema := strings.Split(table, ".")[0]
		mapSchemaToTables[schema] = append(mapSchemaToTables[schema], table)
	}

	for schema, tables := range mapSchemaToTables {
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS clone_%s;", schema))
		if err != nil {
			return nil, nil, err
		}
		// Grant permissions to 'tapp' (the test container user), not dbUser
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA clone_%s TO tapp;", schema))
		if err != nil {
			return nil, nil, err
		}
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA clone_%s TO tapp;", schema))
		if err != nil {
			return nil, nil, err
		}
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA clone_%s GRANT SELECT ON TABLES TO tapp;", schema))
		if err != nil {
			return nil, nil, err
		}

		_, err = realDB.ExecContext(ctx, fmt.Sprintf(
			"IMPORT FOREIGN SCHEMA %s LIMIT TO (%s) FROM SERVER source_srv INTO clone_%s;",
			schema,
			strings.Join(tables, ","),
			schema,
		))
		if err != nil {
			return nil, nil, err
		}
	}

	return realDB, postgresContainer, nil
}

func RunMigrationScript(sqlDB *sql.DB, migrationScriptPath string) error {
	// run migrations on the businessdb repository
	driver, err := mpostgres.WithInstance(sqlDB, &mpostgres.Config{})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithDatabaseInstance(
		migrationScriptPath,
		"postgres", driver)
	if err != nil {
		return err
	}

	err = m.Up()
	if err != nil {
		return err
	}

	return nil
}

func UnmarshalQueries(queriesPath string) ([]string, error) {
	queries := []string{}

	content, err := os.ReadFile(queriesPath)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(content, &queries)
	if err != nil {
		return nil, err
	}

	return queries, nil
}

func RunBenchmarkQueries(sqlDB *sql.DB, queries []string) error {
	if err := runBenchmarkQueries(sqlDB, queries); err != nil {
		return fmt.Errorf("failed to run benchmark queries: %w", err)
	}
	return nil
}

func RunDataMigrationScript(sqlDB *sql.DB, dataMigrationScriptPath string, queries []string) error {
	if err := runBenchmarkQueries(sqlDB, queries); err != nil {
		return fmt.Errorf("failed to run benchmark queries: %w", err)
	}
	// content, err := os.ReadFile(dataMigrationScriptPath)
	// if err != nil {
	// 	return err
	// }

	// // Split the script by \prompt 'benchmark' statements
	// scriptContent := string(content)
	// sections := splitByPromptBenchmark(scriptContent)

	// // Execute each section
	// for i, section := range sections {
	// 	// Skip empty sections
	// 	section = strings.TrimSpace(section)
	// 	if section == "" {
	// 		continue
	// 	}

	// 	// Execute the SQL statements in this section
	// 	if err := executeSQLStatements(sqlDB, section); err != nil {
	// 		return fmt.Errorf("failed to execute section %d: %w", i, err)
	// 	}

	// 	// If this is not the last section, it means we hit a \prompt 'benchmark'
	// 	// so we should run the benchmark queries
	// 	if i < len(sections)-1 {
	// 		fmt.Printf("Running benchmark queries at checkpoint %d...\n", i+1)
	// 		if err := runBenchmarkQueries(sqlDB, queries); err != nil {
	// 			return fmt.Errorf("failed to run benchmark queries at checkpoint %d: %w", i+1, err)
	// 		}
	// 	}
	// }

	return nil
}

// splitByPromptBenchmark splits the script by \prompt 'benchmark' statements
func splitByPromptBenchmark(script string) []string {
	// Split by the prompt benchmark pattern
	// This handles various formats like \prompt 'benchmark', \\prompt 'benchmark', etc.
	delimiter := `\prompt 'benchmark'`
	sections := strings.Split(script, delimiter)

	// Also check for escaped version
	if len(sections) == 1 {
		delimiter = `\\prompt 'benchmark'`
		sections = strings.Split(script, delimiter)
	}

	return sections
}

// executeSQLStatements executes a block of SQL statements
func executeSQLStatements(db *sql.DB, statements string) error {
	// Split by semicolons but be careful about statements that might contain semicolons in strings
	// For now, we'll use a simple approach
	stmts := strings.Split(statements, ";")

	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Add the semicolon back
		stmt = stmt + ";"

		_, err := db.Exec(stmt)
		if err != nil {
			return fmt.Errorf("failed to execute statement: %s, error: %w", stmt, err)
		}
	}

	return nil
}

// runBenchmarkQueries runs the provided benchmark queries
func runBenchmarkQueries(db *sql.DB, queries []string) error {
	// Open a file
	file, err := os.Create("benchmark_results.txt")
	if err != nil {
		return fmt.Errorf("failed to create benchmark results file: %w", err)
	}
	defer file.Close()

	for i, query := range queries {
		start := time.Now()

		rows, err := db.Query(query)
		if err != nil {
			return fmt.Errorf("failed to execute benchmark query %d: %w", i, err)
		}

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			return fmt.Errorf("failed to get columns for query %d: %w", i, err)
		}

		// Count rows and collect data
		rowCount := 0
		for rows.Next() {
			rowCount++

			// Create a slice to hold the values for this row
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))

			for i := range values {
				valuePtrs[i] = &values[i]
			}

			// Scan the row into the values slice
			err := rows.Scan(valuePtrs...)
			if err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan row %d: %w", rowCount, err)
			}

			// Convert values to strings
			rowData := make([]string, len(columns))
			for j, val := range values {
				if val == nil {
					rowData[j] = "NULL"
				} else {
					rowData[j] = fmt.Sprintf("%v", val)
				}
			}

			file.WriteString(fmt.Sprintf("Columns: %v\n", columns))
			file.WriteString(fmt.Sprintf("RowData: %v\n", rowData))
		}
		rows.Close()

		duration := time.Since(start)
		fmt.Printf("  Query %s: %d rows in %v\n", query, rowCount, duration)
	}

	return nil
}
