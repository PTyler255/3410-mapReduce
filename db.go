package main

import (
	_ "github.com/mattn/go-sqlite3"
	"database/sql"
	"os"
	"path/filepath"
	"net/http"
	"io"
	"errors"
	"log"
	"fmt"
)

func openDatabase(path string) (*sql.DB, error) {
	options :=
		"?" + "_busy_timeout=10000" +
		"&" + "_case_sensitive_like=OFF" +
		"&" + "_foreign_keys=ON" +
		"&" + "_journal_mode=OFF" +
		"&" + "_locking_mode=NORMAL" +
		"&" + "mode=rw" +
		"&" + "_synchronous=OFF"
	db, err := sql.Open("sqlite3", path+options)
	if err != nil {
		return new(sql.DB), err
	}
	return db, nil
}

func createDatabase(path string) (*sql.DB, error) {
	_, err := os.OpenFile(path, os.O_RDONLY, 0777)
	if !errors.Is(err, os.ErrNotExist) {
		os.Remove(path)
	}
	db, err := openDatabase(path)
	if err != nil {
		return db, err
	}
	_, err = db.Exec("CREATE TABLE pairs (key TEXT, value TEXT);")
	if err != nil {
		return new(sql.DB), err
	}
	return db, nil
}

func closeDB(dbs []*sql.DB) {
	for _, d := range dbs {
		err := d.Close()
		if err != nil {
			log.Printf("Closing error: %v", err)
		}
	}
}
func closeStmt(dbs []*sql.Stmt) {
	for _, d := range dbs {
		err := d.Close()
		if err != nil {
			log.Printf("Closing error: %v", err)
		}
	}
}

func splitDatabase(source, outputDir, outputPattern string, m int) ([]string, error) {
	outputPath := filepath.Join(outputDir, outputPattern)
	dbi, err := openDatabase(source)
	if err != nil {
		return []string{}, err
	}
	var outputDB []*sql.DB
	var outputNames []string
	for i := 0; i < m; i++ {
		name := fmt.Sprintf(outputPath, i)
		dbo, err := createDatabase(name)
		if err != nil {
			dbi.Close()
			closeDB(outputDB)
			return []string{}, err
		}
		outputDB = append(outputDB, dbo)
		outputNames = append(outputNames, name)
	}
	rows, err := dbi.Query("SELECT key, value FROM pairs")
	if err != nil {
		dbi.Close()
		closeDB(outputDB)
		return []string{}, err
	}
	var i, c int
	for rows.Next() {
		var key, value string
		err = rows.Scan(&key, &value)
		if err != nil {
			dbi.Close()
			closeDB(outputDB)
			return []string{}, err
		}
		db := outputDB[i]
		_, err := db.Exec("INSERT INTO pairs (key, value) values (?, ?)", key, value)
		if err != nil {
			dbi.Close()
			closeDB(outputDB)
			return []string{}, err
		}
		i++
		c++
		if i >= m {
			i = 0
		}
	}
	dbi.Close()
	closeDB(outputDB)
	if c < m {
		return []string{}, fmt.Errorf("Too few tasks in database")
	}
	return outputNames, nil
}

func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error) {
	dbo, err := createDatabase(path)
	if err != nil {
		return new(sql.DB), err
	}
	for _, url := range urls {
		err := download(url, temp)
		if err != nil {
			dbo.Close()
			return new(sql.DB), err
		}
		err = gatherInto(dbo, temp)
		if err != nil {
			dbo.Close()
			return new(sql.DB), err
		}
	}
	return dbo, nil
}


func download(url, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	resp, err := http.Get(url)
	//fmt.Printf(">%s\n>%s\n>--%v - %v\n", url, path, resp, err)
	if err != nil {
		return err
	}
	_, err = io.Copy(file, resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	return nil
}

func gatherInto(db *sql.DB, path string) error {
	_, err := db.Exec("attach ? as merge; insert into pairs select * from merge.pairs; detach merge;", path)
	if err != nil {
		return err
	}
	os.Remove(path)
	return nil
}

