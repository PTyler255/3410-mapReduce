package mapreduce

import (
	"flag"
	"log"
	"os"
	"fmt"
	"errors"
	"path/filepath"
	"net/http"
	"net"
	"net/rpc"
	"sync"
)

//specify instance is master or worker
//make workers easier
//workers must know where master is and any config needed for local machine (tmp dir and port)

type Node struct {
	//----------FLAGS---------
	mAddress, mPort, mSource, mSplData, mTarg, mTmp string
	mMap, mRed int
	mMaster, mSplit, mZip bool
	//------TASK MANAGEMENT------
	mCompleted cl
	mWorkers map[string]string
	mMapTasks []*MapTask
	mRedTasks []*ReduceTask
	mMapI, mRedI il
	mMapDone, mDone bl //master Map, Worker Done
}

type TaskSource struct {
	Source string
	Task int
}

type cl struct {
	m map[int]string
	mu sync.Mutex
}

type il struct {
	i int
	mu sync.Mutex
}

type bl struct {
	b bool
	mu sync.Mutex
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

func Start(c Interface) error {
	var n Node
	//------------FLAGS---------
	getFlags(&n.mAddress, &n.mPort, &n.mSource, &n.mSplData, &n.mTarg, &n.mTmp, &n.mMap, &n.mRed, &n.mMaster, &n.mSplit, &n.mZip)
	//-------START PROCESS--------
	if n.mMaster {
		if err := n.runMaster(c); err != nil {
			return err
		}
		return nil
	} else if n.mAddress != "localhost"{
		if err := n.runWorker(c); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("No master address is given")
}

func (n *Node) runMaster(c Interface) error {
	if err := n.initTemp(); err != nil {
		return fmt.Errorf("Error initializing temporary directory: %v", err)
	}
	defer os.RemoveAll(n.mTmp)
	if true {
		if _, err := splitDatabase(n.mSource, n.mTmp, "map_%d_source.db", n.mMap); err != nil {
			return fmt.Errorf("Error splitting DB: %v", err)
		}
	}
	address := fmt.Sprintf("%s:%s", getLocalAddress(), n.mPort)
	n.startHTTP(address)
	n.mMapTasks = n.genMap(address)
	n.startRPC()
	for len(n.mCompleted.m) < n.mMap {}
	urls := n.genURLs()
	n.mRedTasks = n.genReduce(urls)
	 n.mMapDone.mu.Lock()
	  n.mMapDone.b = true
	 n.mMapDone.mu.Unlock()
	for len(n.mCompleted.m) < n.mRed {}
	defer n.closeWorkers()
	urls = n.genURLs()
	for i, add := range urls {
		urls[i] = makeURL(add, reduceOutputFile(i))
	}
	db, err := mergeDatabases(urls, n.mTarg, filepath.Join(n.mTmp, "final_temp.db"))
	defer db.Close()
	if err != nil {
		return err
	}
	return nil
}
//------------------------------RPC CALLS------------------------------
func (n *Node) startRPC( ) error {
	rpc.Register(n)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":"+n.mPort)
	if err != nil {
		return err
	}
	go http.Serve(l, nil)
	return nil
}

func call(address string, method string, request interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()
	if err = client.Call(method, request, reply); err != nil {
		return err
	}
	return nil
}

func (n *Node) GetNextTask( prevTask TaskSource, reply *interface{}) error {
	//add new worker and get previously completed task
	n.mWorkers[prevTask.Source] = prevTask.Source
	if prevTask.Task != -1 {
		n.mCompleted.mu.Lock()
		n.mCompleted.m[prevTask.Task] = prevTask.Source
		n.mCompleted.mu.Unlock()
	}
	n.mMapI.mu.Lock()
	n.mRedI.mu.Lock()
	defer n.mRedI.mu.Unlock()
	defer n.mMapI.mu.Unlock()
	if n.mMapI.i < n.mMap {
		*reply = n.mMapTasks[n.mMapI.i]
		n.mMapI.i++
	} else if n.mMapDone.b && n.mRedI.i < n.mRed {
		*reply = n.mRedTasks[n.mRedI.i]
		n.mRedI.i++
	} else {
		*reply = nil
	}
	return nil
}

func (n *Node) closeWorkers() {
	var junk1, junk2 string
	for worker := range n.mWorkers {
		if err := call(worker, "Node.Close", junk1, &junk2); err != nil {
			log.Printf("Worker unresponsive: %v", err)
		}
	}
}

//--------------------HELPER FUNCTIONS-----------------------------

func getLocalAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func (n *Node) initTemp() error {
	_, err := os.OpenFile(n.mTmp, os.O_RDONLY, 0777)
	if errors.Is(err, os.ErrNotExist) {
		os.RemoveAll(n.mTmp)
	}
	if err = os.Mkdir(n.mTmp, 0777); err != nil {
		return err
	}
	return nil
}

func (n *Node) startHTTP(address string) {
	go func() {
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(n.mTmp))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Fatalf("Error in HTTP serever %s: %v", address, err)
		}
	} ()
}

func (n *Node) genMap(address string) []*MapTask {
	var mapTasks []*MapTask
	for i := 0; i < n.mMap; i++ {
		var t MapTask
		t.M = n.mMap
		t.R = n.mRed
		t.N = i
		t.SourceHost = address
		mapTasks = append(mapTasks, &t)
	}
	return mapTasks
}

func (n *Node) genReduce(address []string) []*ReduceTask {
	var redTasks []*ReduceTask
	for i := 0; i < n.mRed; i++ {
		var t ReduceTask
		t.M = n.mMap
		t.R = n.mRed
		t.N = i
		t.SourceHost = address
		redTasks = append(redTasks, &t)
	}
	return redTasks
}

func (n *Node) genURLs() []string {
	n.mCompleted.mu.Lock()
	cp := copyMap(n.mCompleted.m)
	urls := make([]string, len(n.mCompleted.m))
	for i, address := range cp {
		urls[i] = address
		delete(n.mCompleted.m, i)
	}
	n.mCompleted.mu.Unlock()
	return urls
}

func copyMap(mp map[int]string) map[int]string {
	cp := make(map[int]string)
	for k, v := range mp {
		cp[k] = v
	}
	return cp
}

//------------------------BANISHED CODE---------------------------------

func getFlags(add, port, source, splitdata, target, tmp *string, mp, red *int, master, split, zip *bool){
	//-----------STRINGS---------
	tempdir := filepath.Join(os.TempDir(), fmt.Sprintf("mapreduce.%d", os.Getpid()))
	flag.StringVar(add, "address", "localhost", "Set the address for the Master Node (For Worker)")
	flag.StringVar(port, "port", "3410", "Set used port (Default 3410)")
	flag.StringVar(source, "source", "source.sqlite3", "Set the source file being read (Default source.sqlite3)")
	flag.StringVar(splitdata, "splitdata", tempdir, "Directory for split files--does not create nor delete")
	flag.StringVar(target, "target", "target.sqlite3", "Filename for output file (Default target.sqlite3")
	flag.StringVar(tmp, "tmp", tempdir, "Set temporary directory")
	//----------INTEGERS----------
	flag.IntVar(mp, "m", 10, "Set the numbr of map tasks (Default 10)")
	flag.IntVar(red, "r", 5, "Set the number of Reduce tasks (Default 5)")
	//------------BOOLEAN------------
	flag.BoolVar(master, "master", false, "Run as Master Node")
	flag.BoolVar(split, "split", false, "Set whether or not to begin split or not")
	flag.BoolVar(zip, "zip", false, "Auto zip large files")
	flag.Parse()
}
/*
struct Tasks {
	mu sync.Mutex
	hold []*interface{}
	count int
}

func (t *Tasks) inc(){
	t.mu.Lock()
	t.count++
	t.mu.Unlock()
}

func (t *Tasks) get() *interface{}{
	t.mu.Lock()
	defer t.mu.Unlock()
	return */
