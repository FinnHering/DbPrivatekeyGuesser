package pg

import (
	"database/sql"
	""
)

type PostgresKeyGuesser struct {
	con *sql.DB
}

func NewPostgresKeyGuesser(host string, port string, database string, user string, password string) (*PostgresKeyGuesser, error) {

	conCfg :=

	con, err := sql.Open("postgres", "user= dbname=postgres sslmode=disable")

	if err != nil {
		return nil, err
	}

	return &PostgresKeyGuesser{
		con: con,
	}, nil
}

func (db *PostgresKeyGuesser) GetTablesForSchema(schema string) []string {
	res, err := db.con.Exec("SELECT * FROM information_schema.tables WHERE table_schema = $1 AND databse", schema)
}

func (db *PostgresKeyGuesser) GetSchemasForDatabase() []string {
	//TODO implement me
	panic("implement me")
}

func (db *PostgresKeyGuesser) GetDatabasesForConnection() []string {
	//TODO implement me
	panic("implement me")
}

func (db *PostgresKeyGuesser) GetTables() []string {
	return []string{}
}

func (db *PostgresKeyGuesser) GetPrimaryKeyPossibilities() []db.PKeyRes {
    // Ensure type PKeyRes exists, or define it if missing.
	return []db.PKeyRes{}
}
