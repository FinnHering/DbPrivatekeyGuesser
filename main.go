package main

import (
	"fmt"
	"github.com/FinnHering/DbPrivatekeyGuesser/lib/db"
	"github.com/FinnHering/DbPrivatekeyGuesser/lib/db/pg"
	"runtime"
)

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	guesser, err := pg.NewPostgresKeyGuesser("localhost", "5432", "example_db", "example_user", "example_password")

	if err != nil {
		panic(err)
	}

	resChan := make(chan db.PKeyRes)
	errChan := make(chan error)
	doneChan := make(chan bool)

	cResChan := make(chan db.PKeyRes)

	go db.PKeyCompressor(resChan, cResChan, doneChan)
	go db.GetPrimaryKeyPossibilities(guesser, "airport", "postgres_air", resChan, errChan, doneChan)

	for {
		select {
		case err := <-errChan:
			panic(err)
		case <-doneChan:
			return
		case res := <-cResChan:
			fmt.Println(res.Columns)
		}
	}

}
