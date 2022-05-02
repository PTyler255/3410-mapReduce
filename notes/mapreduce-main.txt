package main

import (
	"log"
	"fmt"
	"os"
	"errors"
	"path/filepath"
	"net/http"
	"net"
	"strings"
	"strconv"
	"unicode"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(1)
	m := 9
	r := 3
	tempdir := filepath.Join(os.TempDir(), fmt.Sprintf("mapreduce.%d", os.Getpid()))
	_, err := os.OpenFile(tempdir, os.O_RDONLY, 0777)
	if !errors.Is(err, os.ErrNotExist) {
		os.RemoveAll(tempdir)
	}
	err = os.Mkdir(tempdir, 0777)
	if err != nil {
		log.Fatalf("Error making directory: %v", err)
	}
	defer os.RemoveAll(tempdir)
	address := fmt.Sprintf("%s:%s", getLocalAddress(), "3410")
	go func() {
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Fatalf("Error in HTTP serever %s: %v", address, err)
		}
	} ()
	_, err = splitDatabase("austen.db", tempdir, "map_%d_source.db", m)
	if err != nil {
		log.Fatalf("Error splitting DB: %v", err)
	}
	var c Client
	for i := 0; i < m; i++ {
		var t MapTask
		t.M = m
		t.R = r
		t.N = i
		t.SourceHost = address
		err = t.Process(tempdir, c)
		if err != nil {
			log.Fatalf("Error processing map tasks: %v", err)
		}
	}
	for i := 0; i < r; i++ {
		var tr ReduceTask
		tr.M = m
		tr.R = r
		tr.N = i
		tr.SourceHost = address
		err = tr.Process(tempdir, c)
		if err != nil {
			log.Fatalf("Error processing reduce tasks: %v", err)
		}
	}
	urls := make([]string, r)
	for i := 0; i < r; i++ {
		urls[i] = makeURL(address, reduceOutputFile(i))
	}
	db, err := mergeDatabases(urls, filepath.Join("output", "final.db"), filepath.Join(tempdir, "final_temp.db"))
	if err != nil{
		log.Fatalf("Error merging databases: %v", err)
	}
	db.Close()
	fmt.Printf("SUCCESS!\n")
}

//----------------------------------------------------------------------------------------------------------------

func getLocalAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

type Client struct{
	Processed int
}

func (c Client) Map(key, value string, output chan<- Pair) error {
    defer close(output)
    lst := strings.Fields(value)
    for _, elt := range lst {
        word := strings.Map(func(r rune) rune {
            if unicode.IsLetter(r) || unicode.IsDigit(r) {
                    return unicode.ToLower(r)
            }
            return -1
        }, elt)
        if len(word) > 0 {
            output <- Pair{Key: word, Value: "1"}
        }
    }
    return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
    defer close(output)
    count := 0
    for v := range values {
        i, err := strconv.Atoi(v)
        if err != nil {
            return err
        }
        count += i
    }
    p := Pair{Key: key, Value: strconv.Itoa(count)}
    output <- p
    return nil
}
