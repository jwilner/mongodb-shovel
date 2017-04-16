package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var uriFlag = flag.String("uri", "mongodb://localhost:27017", "a mongodb connection uri")
var pathFlag = flag.String("ledgerPath", "/tmp/ledger", "where to store the progress record")
var freqFlag = flag.Int("freq", 100, "how often to store progress")

const (
	localDbName   = "local"
	oplogCollName = "oplog.rs"

	timeout time.Duration = 5 * time.Second

	// Update operation performed was an update
	Update OpType = "u"
	// Insert operation performed was an insert
	Insert OpType = "i"
	// Delete operation performed was a delete
	Delete OpType = "d"
)

// OpType an operation type
//go:generate stringer -type=OpType
type OpType string

// Entry an op log entry
//go:generate stringer -type=Entry
type Entry struct {
	Hash      int64               `bson:"h"`
	Namespace string              `bson:"ns"`
	Timestamp bson.MongoTimestamp `bson:"ts"`
	OpType    OpType              `bson:"op"`
	Info      struct {
		ID bson.ObjectId `bson:"_id"`
	} `bson:"o2,omitempty"`
	Op bson.M `bson:"o"`
}

func parseSession(uri string) (*mgo.Session, error) {
	session, err := mgo.Dial(uri)
	if err != nil {
		return nil, err
	}
	log.Printf("Connected to %v", *uriFlag)
	return session, nil
}

func queryFunc(session *mgo.Session) (func(bson.MongoTimestamp) *mgo.Iter, error) {
	db := session.DB(localDbName)
	names, err := db.CollectionNames()
	if err != nil {
		return nil, fmt.Errorf("Failed reading collection names from %v: %v", localDbName,
			err)
	}
	if !contains(names, oplogCollName) {
		return nil, errors.New("No oplog found; is this a replica set member?")
	}
	coll := db.C(oplogCollName)

	return func(ts bson.MongoTimestamp) *mgo.Iter {
		var query bson.M
		if ts != 0 {
			query = bson.M{"ts": bson.M{"$gt": ts}}
		}
		log.Printf("querying with %v", query)
		return coll.Find(query).Sort("$natural").Tail(5 * time.Second)
	}, nil
}

func contains(strings []string, needle string) bool {
	for _, val := range strings {
		if val == needle {
			return true
		}
	}
	return false
}

func load(path string) (bson.MongoTimestamp, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("erred loading path %v: %v", path, err)
	}
	var ts bson.MongoTimestamp
	if err = bson.UnmarshalJSON(data, &ts); err != nil {
		return 0, fmt.Errorf("Failed unmarshaling data from %v: %v", path, err)
	}
	return ts, nil
}

func write(path string, ts bson.MongoTimestamp) error {
	data, err := bson.MarshalJSON(ts)
	if err != nil {
		return fmt.Errorf("failed marshalling to json: %v", err)
	}
	err = ioutil.WriteFile(path, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to persist ts %v: %v", ts, err)
	}
	return nil
}

func read(queryFunc func(bson.MongoTimestamp) *mgo.Iter, ts bson.MongoTimestamp) (<-chan Entry, <-chan error) {
	entries := make(chan Entry)
	errs := make(chan error)
	go func() {
		iter := queryFunc(ts)
		var entry Entry
		for {
			for iter.Next(&entry) {
				entries <- entry
				ts = entry.Timestamp
			}

			if err := iter.Err(); err != nil {
				errs <- fmt.Errorf("From db: %v", err)
				return
			}

			if iter.Timeout() {
				log.Print("Iterator timed out")
				continue
			}

			iter = queryFunc(ts)
		}
	}()
	return entries, errs
}

func run(uri string, path string, freq int) error {
	session, err := parseSession(uri)
	if err != nil {
		return err
	}
	defer session.Close()

	ts, err := load(path)
	if err != nil {
		return err
	}

	defer func() {
		if ts != 0 {
			write(path, ts)
		}
	}()

	queryFor, err := queryFunc(session)
	if err != nil {
		return err
	}

	entries, errors := read(queryFor, ts)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	var counter int
	for {
		select {
		case sig := <-sigs:
			log.Printf("Receive %v; shutting down", sig)
			return nil
		case entry := <-entries:
			log.Printf("Received entry: %v", entry)
			ts = entry.Timestamp
			counter = (counter + 1) % freq
			if counter == 0 {
				write(path, ts)
			}
		case err := <-errors:
			return err
		}
	}
}

func main() {
	flag.Parse()

	if err := run(*uriFlag, *pathFlag, *freqFlag); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
