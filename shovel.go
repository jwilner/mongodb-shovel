package main

import (
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var uriFlag = flag.String("uri", "mongodb://localhost:27017", "a mongodb connection uri")
var ledgerFlag = flag.String("ledgerPath", "/tmp/ledger", "where to store the progress record")
var ledgerFreqFlag = flag.Int("freq", 100, "how often to store progress")

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
		return nil, err
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
	var i int64
	data, err := ioutil.ReadFile(path)

	if err == nil {
		i, err = strconv.ParseInt(string(data), 16, 64)
	} else if os.IsNotExist(err) {
		err = nil // ignore ENOENT
	}

	return bson.MongoTimestamp(i), err
}

func write(path string, ts bson.MongoTimestamp) error {
	err := ioutil.WriteFile(path, []byte(strconv.FormatInt(int64(ts), 16)), 0644)
	if err != nil {
		log.Printf("failed to persist ts %v: %v", ts, err)
	}
	return err
}

func dispatch(entry Entry) {
	log.Printf("Received: %v", entry)
}

func handle(session *mgo.Session, path string, freq int, stop func() bool) error {
	defer session.Close()

	ts, err := load(path)
	if err != nil {
		return nil
	}
	defer func() {
		if ts != 0 {
			write(path, ts)
		}
	}()

	queryFor, err := queryFunc(session)
	if err != nil {
		return nil
	}

	iter := queryFor(ts)

	var entry Entry
	var counter int
	for {
		for !stop() && iter.Next(&entry) {
			dispatch(entry)
			ts = entry.Timestamp

			counter = (counter + 1) % freq
			if counter == 0 {
				log.Print("Persisting timestamp")
				write(path, ts)
			}
		}

		if err := iter.Err(); err != nil {
			return err
		}

		if !stop() && iter.Timeout() {
			log.Print("Iterator timed out")
			continue
		}

		if stop() {
			log.Print("Shutting down cleanly")
			return nil
		}

		iter = queryFor(ts)
	}
}

func makeStop() func() bool {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	var stopped bool
	return func() bool {
		if !stopped {
			select {
			case sig := <-sigs:
				stopped = true
				log.Printf("Received %v, marking stopped", sig)
			default:
				stopped = false
			}
		}
		return stopped
	}
}

func main() {
	flag.Parse()

	session, err := parseSession(*uriFlag)
	if err != nil {
		log.Fatalf("Failed parsing session: %v", err)
	}

	handle(session, *ledgerFlag, *ledgerFreqFlag, makeStop())
}
