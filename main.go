package main

import (
	"fmt"
	"log"

	badger1 "github.com/dgraph-io/badger"
	badger3 "github.com/dgraph-io/badger/v3"
)

func main() {
	//path1 := `C:\badger\papagaio-api`
	path1 := `C:\Users\Alessandro Pinna\Desktop\papagaio-dev\badger\papagaio-api`

	path2 := `C:\badger\papagaio-api-v3`

	db1, err := Init1(path1)
	if err != nil {
		panic(err)
	}

	defer db1.Close()

	db2, err := Init3(path2)
	if err != nil {
		panic(err)
	}

	defer db2.Close()

	err = migrate(db1, db2)
	if err != nil {
		log.Println("something was wrong:", err)
	} else {
		log.Println("migration success!")
	}
}

func Init1(dir string) (*badger1.DB, error) {
	DB, err := badger1.Open(badger1.DefaultOptions(dir).WithSyncWrites(true).WithTruncate(true).WithLogger(nil))
	if err != nil {
		log.Fatal(err)
	}

	return DB, err
}

func Init3(dir string) (*badger3.DB, error) {
	DB, err := badger3.Open(badger3.DefaultOptions(dir).WithSyncWrites(true).WithLogger(nil))
	if err != nil {
		log.Fatal(err)
	}

	return DB, err
}

func migrate(db1 *badger1.DB, db2 *badger3.DB) error {

	err := db1.View(func(txn *badger1.Txn) error {
		opts := badger1.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte("")
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			fmt.Println("key:", string(key))
			fmt.Println("size:", item.ValueSize())

			dst := make([]byte, 0)
			value, err := item.ValueCopy(dst)
			if err != nil {
				fmt.Println("ValueCopy error")
				return err
			}

			fmt.Println("value:", string(value))

			err = db2.Update(func(txn *badger3.Txn) error {
				e := badger3.NewEntry([]byte(key), value)
				err := txn.SetEntry(e)

				if err != nil {
					fmt.Println("SetEntry error")
				}

				return err
			})

			if err != nil {
				fmt.Println("Update error")

				return err
			}
		}

		return nil
	})

	return err
}
