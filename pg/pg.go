package pg

import (
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"strings"
)

type PostgresKeyGuesser struct {
	con *sql.DB
}

func NewPostgresKeyGuesser(host string, port string, database string, user string, password string) (*PostgresKeyGuesser, error) {

	conStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", user, password, host, port, database)
	con, err := sql.Open("pgx", conStr)

	if err != nil {
		return nil, err
	}

	return &PostgresKeyGuesser{
		con: con,
	}, nil
}

func (pgkg *PostgresKeyGuesser) GetDBPool() *sql.DB {
	return pgkg.con
}

func (pgkg *PostgresKeyGuesser) CheckDuplicates(tx *sql.Tx, tableName string, schemaName string, colGrouping []string) (*int, error) {

	sanitizedColumns := make([]string, len(colGrouping))
	copy(sanitizedColumns, colGrouping)

	for idx, col := range colGrouping {
		col = pgx.Identifier.Sanitize([]string{col})
		sanitizedColumns[idx] = col
	}
	cols := strings.Join(colGrouping, ", ")

	objectIdentifier := pgx.Identifier.Sanitize([]string{schemaName, tableName})

	query := fmt.Sprintf(`
with t1 as (
    select count(1) cnt from %s group by %s
)
select sum(cnt - 1) res from t1
`, objectIdentifier, cols)

	res := tx.QueryRow(query)

	var resInt int
	err := res.Scan(&resInt)
	if err != nil {
		return nil, err
	}

	return &resInt, nil
}

func (pgkg *PostgresKeyGuesser) GetColsForTable(tableName string, schemaName string) (*[]string, error) {
	rows, err := pgkg.con.Query("SELECT column_name FROM information_schema.columns WHERE table_name = $1 and table_schema = $2", tableName, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make([]string, 0)
	for rows.Next() {
		var colName string
		err := rows.Scan(&colName)
		if err != nil {
			return nil, err
		}

		cols = append(cols, colName)
	}

	return &cols, nil
}

func (pgkg *PostgresKeyGuesser) GetTablesForSchema(schema string) (*[]string, error) {
	rows, err := pgkg.con.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = $1", schema)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	tableNames := make([]string, 0)

	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)

		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}

	return &tableNames, nil
}

func (pgkg *PostgresKeyGuesser) GetSchemasForDatabase() (*[]string, error) {
	rows, err := pgkg.con.Query("SELECT schema_name FROM information_schema.schemata")

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemata := make([]string, 0)

	for rows.Next() {
		var schemaName string
		err := rows.Scan(&schemaName)

		if err != nil {
			return nil, err
		}

		schemata = append(schemata, schemaName)
	}

	return &schemata, nil
}
