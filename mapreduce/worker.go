package mapreduce

import (
	_ "github.com/mattn/go-sqlite3"
	"database/sql"
	"path/filepath"
	"hash/fnv"
	"fmt"
	"os"
	"log"
	"time"
)

type MapTask struct {
	M, R, N int //map & reduce tasks; maptask number, 0-based
	SourceHost string //address of host with map input file
}

type ReduceTask struct {
	M, R, N int //map & reduce tasks; maptask number, 0-based
	SourceHost []string //address of host with map input file
}

type Pair struct {
	Key, Value string
}

type Task interface {
	Process(tempdir string, client Interface) error
	GetN() int
}


//--------------------------NODE MANAGEMENT-------------------------------

func (n *Node) runWorker(c Interface) error {
	if err := n.initTemp(); err != nil {
		return fmt.Errorf("Error initializing temporary directory: %v", err)
	}
	defer os.RemoveAll(n.mTmp)
	address := fmt.Sprintf("%s:%s", getLocalAddress(), n.mPort)
	n.startHTTP(address)
	if err := n.startRPC(); err != nil {
		return fmt.Errorf("Error starting RPC server: %v", err)
	}
	n.processTasks(c)
	return nil
}

func (n *Node) processTasks(c Interface){
	address := fmt.Sprintf("%s:%s", getLocalAddress(), n.mPort)
	prevTask := TaskSource{Source: address, Task: -1}
	var t Task
	for !n.mDone.b {
		if err := call(n.mAddress, "Node.GetNextTask", prevTask, &t); err != nil {
			log.Fatalf("Error getting next task: %v", err)
		}
		if t != nil {
			if err := t.Process(n.mTmp, c); err != nil {
				log.Fatalf("Error processing task: %v")
			}
			prevTask = TaskSource{Source: address, Task: t.GetN()}
		} else {
			prevTask = TaskSource{Source: address, Task: -1}
			time.Sleep(2*time.Second)
		}
	}
}

//--------------------RPC--------------------
func (n *Node) Close(none string, nothing *string) error {
	n.mDone.mu.Lock()
	n.mDone.b = true
	n.mDone.mu.Unlock()
	return nil
}
//----------------------------PROCESSES-----------------------------------
//--------------------MAP--------------------

func (t *MapTask) GetInt() int {
	return t.N
}

func (t *MapTask) Process(tempdir string, client Interface) error {
	inputFile := filepath.Join(tempdir, mapInputFile(t.N))
	url := makeURL(t.SourceHost, mapSourceFile(t.N))
	err := download(url, inputFile)
	if err != nil {
		return err
	}
	tasks := make([]*sql.Stmt, t.R)
	for i := range tasks {
		db, err := createDatabase(filepath.Join(tempdir, mapOutputFile(t.N,i))) //figure out a name
		if err != nil {
			return err
		}
		something, err := db.Prepare("INSERT INTO pairs (key, value) VALUES (?,?)")
		if err != nil {
			return err
		}
		tasks[i] = something
	}
	defer closeStmt(tasks)
	db, err := openDatabase(inputFile)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.Query("SELECT key, value FROM pairs")
	if err != nil {
		return err
	}
	var pt int
	for rows.Next() {
		var key, value string
		err = rows.Scan(&key, &value)
		if err != nil {
			return err
		}
		map_out := make(chan Pair)
		joined := make(chan error)
		go t.write(map_out, joined, tasks, &pt)
		client.Map(key, value, map_out)
		<-joined
	}
	fmt.Printf("Map Tasks Processed: %d\n", pt)
	return nil
}

func (task *MapTask) write( output chan Pair, joined chan error, tasks []*sql.Stmt, pt *int) {
	for pair := range output {
		*pt++
		hash := fnv.New32() // from the stdlib package hash/fnv
		hash.Write([]byte(pair.Key))
		r := int(hash.Sum32() % uint32(task.R))
		//fmt.Printf("%d - %s, %s\n", r, pair.Key, pair.Value)
		_, err := tasks[r].Exec(pair.Key, pair.Value)
		if err != nil {
			joined <- err
			return
		}
	}
	joined <- nil
}

//-----------------REDUCE-----------------

func (t *ReduceTask) GetInt() int {
	return t.N
}

func (t *ReduceTask) Process(tempdir string, client Interface) error {
	urls := make([]string, t.M)
	for i, source := range t.SourceHost {
		urls[i] = makeURL(source, mapOutputFile(i, t.N))
	}
	db, err := mergeDatabases(urls, filepath.Join(tempdir, reduceInputFile(t.N)), filepath.Join(tempdir, reduceTempFile(t.N)))
	defer db.Close()
	if err != nil {
		return err
	}
	outputDB, err := createDatabase(filepath.Join(tempdir, reduceOutputFile(t.N)))
	defer outputDB.Close()
	if err != nil {
		return err
	}
	rows, err := db.Query("SELECT key, value FROM pairs ORDER BY key")//SEG FAULTING
	if err != nil {
		return err
	}
	input_chan := make(chan string)
	output_chan := make(chan Pair)
	prev_key := ""
	for rows.Next() {
		var key, value string
		rows.Scan(&key, &value)
		if prev_key != key {
			if prev_key != "" {
				close(input_chan)
				for i := range output_chan {
					_, err = outputDB.Exec("INSERT INTO pairs (key, value) values (?, ?)", i.Key, i.Value)
					if err != nil {
						return err
					}
				}
				input_chan = make(chan string)
				output_chan = make(chan Pair)
			}
			prev_key = key
			go func() {
				client.Reduce(key, input_chan, output_chan)
			} ()
			input_chan <- value
		} else {
			input_chan <- value
		}
	}
	close(input_chan)
	for i := range output_chan {
		outputDB.Exec("INSERT INTO pairs (key, value) VALUES (?, ?)", i.Key, i.Value)
	}
	return nil
}


//-------------------------------NAMES----------------------------------
func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }
