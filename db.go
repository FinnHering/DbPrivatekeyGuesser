package db

type DB interface {
	GetTables() []string
	GetTablesForSchema(schemaName string) []string
	GetSchemasForDatabase() []string
	GetPrimaryKeyPossibilities(tableName string) []PKeyRes
}

func GuessPrimaryKeyPossibilitiesForSchema(schema string, db DB) map[string][]PKeyRes {

	tableNames := db.GetTablesForSchema(schema)
	m := make(map[string][]PKeyRes)

	for _, tableName := range tableNames {
		pKeys := db.GetPrimaryKeyPossibilities(tableName)
		m[tableName] = pKeys
	}

	return m
}

func GuessPrimaryKeyPossibilitiesForDatabase(db DB) map[string]map[string][]PKeyRes {

	res := make(map[string]map[string][]PKeyRes)
	schemas := db.GetSchemasForDatabase()
	for _, schema := range schemas {
		res[schema] = GuessPrimaryKeyPossibilitiesForSchema(schema, db)
	}

	return res
}

type PKeyRes struct {
	Columns    []string
	Duplicates int
}
