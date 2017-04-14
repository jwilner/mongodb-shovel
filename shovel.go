package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strconv"
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

type timestampLedger struct {
	LastTimestamp bson.MongoTimestamp
	count         int
	freq          int
	path          string
}

// records every `freq` timestamp to the ledger
func (t *timestampLedger) Record(Ts bson.MongoTimestamp) error {
	t.LastTimestamp = Ts
	t.count++
	if t.freq == t.count {
		err := ioutil.WriteFile(t.path, []byte(strconv.FormatInt(int64(t.LastTimestamp), 16)), 0644)
		if err != nil {
			return err
		}
		t.count = 0
	}
	return nil
}

func (t *timestampLedger) Query() interface{} {
	if t.LastTimestamp == 0 {
		return nil
	}
	return bson.M{"ts": bson.M{"$gt": t.LastTimestamp}}
}

func parseLedger() *timestampLedger {
	path := *ledgerFlag
	freq := *ledgerFreqFlag

	var i int64
	data, err := ioutil.ReadFile(path)
	if err == nil {
		i, err = strconv.ParseInt(string(data), 16, 64)
	}
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}

	return &timestampLedger{
		path:          path,
		LastTimestamp: bson.MongoTimestamp(i),
		freq:          freq,
	}
}

func parseSession() *mgo.Session {
	session, err := mgo.Dial(*uriFlag)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Connected to %v", *uriFlag)
	return session
}

func contains(strings []string, needle string) bool {
	for _, val := range strings {
		if val == needle {
			return true
		}
	}
	return false
}

func read(session *mgo.Session, ledger *timestampLedger, work func(Entry)) {
	db := session.DB(localDbName)
	names, err := db.CollectionNames()
	if err != nil {
		log.Fatalf("Error getting collection names: %v", err)
	}

	if !contains(names, oplogCollName) {
		log.Fatal("No oplog found; is this a replica set member?")
	}
	coll := db.C(oplogCollName)

	query := ledger.Query()
	log.Printf("querying with %v", query)
	iter := coll.Find(query).Sort("$natural").Tail(5 * time.Second)
	defer iter.Close()

	for {
		result := Entry{}

		for iter.Next(&result) {
			work(result)

			err := ledger.Record(result.Timestamp)
			if err != nil {
				log.Fatal(err)
			}
		}

		if err := iter.Err(); err != nil {
			log.Fatalf("Got an error from the iterator: %v", err)
		}

		if iter.Timeout() {
			log.Print("Iterator timed out")
			continue
		}

		query = ledger.Query()
		log.Printf("Requerying with query: %v", query)
		iter = coll.Find(query).Sort("$natural").Tail(timeout)
	}
}

func dispatch(entry Entry) {
	log.Printf("Received: %v", entry)
}

func main() {
	flag.Parse()
	ledger := parseLedger()

	session := parseSession()
	defer session.Close()

	read(session, ledger, dispatch)
}
