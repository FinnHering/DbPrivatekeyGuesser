package db

import (
	"container/heap"
	"database/sql"
	"github.com/FinnHering/DbPrivatekeyGuesser/internal"
	mapset "github.com/deckarep/golang-set/v2"
	"math"
)

type DB interface {
	GetTablesForSchema(schemaName string) (*[]string, error)
	GetSchemasForDatabase() (*[]string, error)
	CheckDuplicates(tx *sql.Tx, tableName string, schemaName string, colGrouping []string) (*int, error)
	GetColsForTable(tableName string, schemaName string) (*[]string, error)
	GetDBPool() *sql.DB
}

// PKeyCompressor takes privateKey results and removes redundant PkeyRes inputs.
// E.g. input is: {Columns: {col1, col2, rol3}, Duplicates: 0, TableName "cool-table}, {Columns: {col1, col2}, Duplicates: 0, TableName "cool-table"}, {Columns: {col2}, Duplicates: 12, TableName "cool-table"} and {Columns: {col1}, Duplicates: 24, TableName "cool-table"}
// Then it would only output {Columns: {col1, col2}, Duplicates: 0, TableName "cool-table"}
func PKeyCompressor(input <-chan PKeyRes, output chan<- PKeyRes, stopChan <-chan bool) {
	previousValues := mapset.NewSet[PKeyRes]()

	for {
		select {
		case <-stopChan:
			return
		case res := <-input:
			if !checkPrevious(&previousValues, &res) {
				output <- res
			}
		}
	}

}

// checkPrevious checks if there has been a submitted better result in the past. Better means that there is a previous
// result that is a subset of the current result and has the same amount of duplicates. In this case it will return true
// if this isn't the case a new entry will be added to store and false will be returned.
// This function can only act on the result submitted in the past. This means that the size of the sets should be
// submitted from smallest to larges for this to work decently
func checkPrevious(store *mapset.Set[PKeyRes], res *PKeyRes) bool {
	if (*store).ContainsOne(*res) {
		return true
	}

	isImprovement := true
	subsetFound := false
	for e := range (*store).Iter() {
		if e.TableName != res.TableName {
			continue
		}

		if res.Columns.IsSuperset(e.Columns) {
			subsetFound = true
			if res.Duplicates < e.Duplicates {
				(*store).Add(*res)
			} else {
				isImprovement = false
			}
		}
	}

	if !subsetFound {
		(*store).Add(*res)
	}

	return !isImprovement
}

func GetPrimaryKeyPossibilities(pgkg DB, tableName string, schemaName string, resChan chan<- PKeyRes, errChan chan<- error, doneChan chan<- bool) {
	con := pgkg.GetDBPool()
	tx, err := con.Begin()
	if err != nil {
		errChan <- err
		doneChan <- true
		return
	}
	defer tx.Rollback()

	// Get columns
	cols, err := pgkg.GetColsForTable(tableName, schemaName)
	if err != nil {
		errChan <- err
		doneChan <- true
		return
	}

	// check if table has duplicate rows
	dupes, err := pgkg.CheckDuplicates(tx, tableName, schemaName, *cols)

	if err != nil {
		errChan <- err
		doneChan <- true
		return
	}

	// If table has duplicates we return immediately with result
	if *dupes > 0 {
		res := PKeyRes{
			Columns:    mapset.NewSet(*cols...),
			Duplicates: *dupes,
			TableName:  tableName,
		}

		resChan <- res
		doneChan <- true
		return
	}

	// Columns currently selected
	selectedCols := make([]string, 0)

	// Priority Queue working from least amount of columns selected to most. This is important for pKeyCompressor to properly work!
	selectionPQ := make(internal.PriorityQueue[KeyGuessState], 0)
	heap.Init(&selectionPQ)

	heap.Push(&selectionPQ, &internal.Item[KeyGuessState]{
		Value: KeyGuessState{
			selectedCols: &selectedCols,
			leftCols:     cols,
			globalBest:   math.MaxInt32,
		},
		Priority: len(selectedCols),
	})

	for selectionPQ.Len() > 0 {
		current := *heap.Pop(&selectionPQ).(*internal.Item[KeyGuessState])

		selectedCols := current.Value.selectedCols
		leftCols := current.Value.leftCols

		// Result mapping leftCols idx to absolute improvement compared to selectedCols. Does not store "winners"
		colResults := make(map[int]uint)

		for idx, columnName := range *leftCols {
			cols := make([]string, len(*selectedCols), len(*selectedCols)+1)

			copy(cols, *selectedCols)
			cols = append(cols, columnName)

			resInt, err := pgkg.CheckDuplicates(tx, tableName, schemaName, cols)

			if err != nil {
				errChan <- err
				close(resChan)
				close(errChan)
				return
			}

			if *resInt == 0 {
				// Exclude "winning" column from colResults as there are no more improvements to be found here nor further down the line
				//fmt.Printf("%s: Found primary key!\n", tableName)
				resChan <- PKeyRes{
					Columns:    mapset.NewSet(cols...),
					Duplicates: 0,
					TableName:  tableName,
				}

			} else if *resInt < current.Value.globalBest {
				colResults[idx] = uint(*resInt)
			}
		}

		// Evaluate results, push every non "winning" improvement back onto stack for further evaluation
		for key, value := range colResults {
			cols := make([]string, len(*selectedCols), len(*selectedCols)+1)
			copy(cols, *selectedCols)
			cols = append(cols, (*leftCols)[key])

			// Exclude current column from its own left cols
			newLeftCols := make([]string, len(*leftCols)-1)
			copy(newLeftCols, (*leftCols)[:key])
			copy(newLeftCols[key:], (*leftCols)[key+1:])

			state := KeyGuessState{
				selectedCols: &cols,
				leftCols:     leftCols,
				globalBest:   int(value),
			}
			heap.Push(&selectionPQ, &internal.Item[KeyGuessState]{
				Value:    state,
				Priority: state.globalBest,
			})
		}

	}
	doneChan <- true
	return
}

type KeyGuessState struct {
	selectedCols *[]string
	leftCols     *[]string
	globalBest   int
}

type PKeyRes struct {
	Columns    mapset.Set[string]
	Duplicates int
	TableName  string
}
