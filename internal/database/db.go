package db

import (
	"context"
	"database/sql"
)

func Connect(ctx context.Context, connString string) (*sql.DB, error) {
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	return db, nil
}
