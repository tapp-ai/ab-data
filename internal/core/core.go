package core

import (
	"context"
	"database/sql"
	"fmt"
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

func SetupDatabase(ctx context.Context, dbHost string, dbPort string, dbUser string, dbPassword string, dbName string, schemaList []string, tablesList []string) (*sql.DB, error) {
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
		return nil, err
	}

	// create businessDB for the entire test suite
	connStr, err := postgresContainer.ConnectionString(context.Background(), "sslmode=disable")
	if err != nil {
		return nil, err
	}

	realDB, err := db.Connect(context.Background(), connStr)
	if err != nil {
		return nil, err
	}

	// Note: We also need to create the extensions that the user has in the source database
	_, err = realDB.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS postgres_fdw;")
	if err != nil {
		return nil, err
	}

	// TODO: Automate this for all the extensions that the user has in the source database and not just hstore
	_, err = realDB.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS hstore;")
	if err != nil {
		return nil, err
	}

	_, err = realDB.ExecContext(ctx, fmt.Sprintf("CREATE SERVER source_srv FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', dbname '%s', port '%s');", dbHost, dbName, dbPort))
	if err != nil {
		return nil, err
	}
	_, err = realDB.ExecContext(ctx, fmt.Sprintf("CREATE USER MAPPING FOR tapp SERVER source_srv OPTIONS (user '%s', password '%s');", dbUser, dbPassword))
	if err != nil {
		return nil, err
	}

	// Note: We might need to have a table fallback if we had covered the edge cases like enum loading here
	for _, schema := range schemaList {
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS clone_%s;", schema))
		if err != nil {
			return nil, err
		}
		// Grant permissions to 'tapp' (the test container user), not dbUser
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA clone_%s TO tapp;", schema))
		if err != nil {
			return nil, err
		}
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA clone_%s TO tapp;", schema))
		if err != nil {
			return nil, err
		}
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA clone_%s GRANT SELECT ON TABLES TO tapp;", schema))
		if err != nil {
			return nil, err
		}
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("IMPORT FOREIGN SCHEMA %s FROM SERVER source_srv INTO clone_%s;", schema, schema))
		if err != nil {
			return nil, err
		}
	}

	mapSchemaToTables := make(map[string][]string)
	for _, table := range tablesList {
		schema := strings.Split(table, ".")[0]
		mapSchemaToTables[schema] = append(mapSchemaToTables[schema], table)
	}

	for schema, tables := range mapSchemaToTables {
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS clone_%s;", schema))
		if err != nil {
			return nil, err
		}
		// Grant permissions to 'tapp' (the test container user), not dbUser
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA clone_%s TO tapp;", schema))
		if err != nil {
			return nil, err
		}
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA clone_%s TO tapp;", schema))
		if err != nil {
			return nil, err
		}
		_, err = realDB.ExecContext(ctx, fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA clone_%s GRANT SELECT ON TABLES TO tapp;", schema))
		if err != nil {
			return nil, err
		}

		_, err = realDB.ExecContext(ctx, fmt.Sprintf(
			"IMPORT FOREIGN SCHEMA %s LIMIT TO (%s) FROM SERVER source_srv INTO clone_%s;",
			schema,
			strings.Join(tables, ","),
			schema,
		))
		if err != nil {
			return nil, err
		}
	}

	return realDB, nil
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
