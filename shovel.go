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

	"github.com/streadway/amqp"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var mongoURIFlag = flag.String("mongo", "mongodb://localhost:27017", "a mongodb connection uri")
var amqpURIFlag = flag.String("rabbit", "amqp://localhost:5672", "an amqp connection uri")
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

func timedConn(makeConn func() (interface{}, error), logger func(time.Duration)) (interface{}, error) {
	conns := make(chan interface{})
	errs := make(chan error)
	ticker := make(chan time.Duration)

	go func() {
		conn, err := makeConn()
		if err != nil {
			errs <- err
		} else {
			conns <- conn
		}
	}()

	go func() {
		var sec time.Duration
		for {
			time.Sleep(time.Second)
			sec += time.Second
			ticker <- sec
		}
	}()

	for {
		select {
		case conn := <-conns:
			return conn, nil
		case err := <-errs:
			return nil, err
		case dur := <-ticker:
			logger(dur)
		}
	}
}

func mongoConn(uri string) (*mgo.Session, error) {
	iface, err := timedConn(func() (interface{}, error) {
		return mgo.Dial(uri)
	}, func(dur time.Duration) {
		log.Printf("Waited %v for a connection from %v", dur, uri)
	})
	if err != nil {
		return nil, err
	}
	log.Printf("Successfully connected to %v", uri)
	return iface.(*mgo.Session), err
}

func amqpConn(uri string) (*amqp.Connection, error) {
	iface, err := timedConn(func() (interface{}, error) {
		return amqp.Dial(uri)
	}, func(dur time.Duration) {
		log.Printf("Waited %v for a connection from %v", dur, uri)
	})
	if err != nil {
		return nil, err
	}
	log.Printf("Successfully connected to %v", uri)
	return iface.(*amqp.Connection), nil
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

func read(queryFunc func(bson.MongoTimestamp) *mgo.Iter, ts bson.MongoTimestamp, errs chan<- error) <-chan Entry {
	entries := make(chan Entry)
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
	return entries
}

func pubFunc(conn *amqp.Connection) (func(Entry) error, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Error gettting channel: %v", err)
	}
	if err = channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("Error setting up confirmed channel %v", err)
	}
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation))

	return func(entry Entry) error {
		data, err := bson.Marshal(entry)
		if err != nil {
			return err
		}
		msg := amqp.Publishing{
			Headers:     amqp.Table{},
			ContentType: "application/bson",
			Body:        data,
			Timestamp:   time.Now(),
		}

		if err := channel.Publish("hi", "", false, false, msg); err != nil {
			return fmt.Errorf("failed publishing %v: %v", entry, err)
		}

		if c := <-confirms; !c.Ack {
			return fmt.Errorf("received nack: %v", c.DeliveryTag)
		}

		return nil
	}, nil
}

func publish(send func(Entry) error, entries <-chan Entry, errs chan<- error) <-chan bson.MongoTimestamp {
	tses := make(chan bson.MongoTimestamp)
	go func() {
		for entry := range entries {
			log.Printf("Got an entry with ts: %v", time.Unix(int64(entry.Timestamp)>>32, 0))
			err := send(entry)
			if err != nil {
				errs <- err
				return
			}
			tses <- entry.Timestamp
		}
	}()
	return tses
}

func run(mongoURI string, amqpURI string, path string, freq int) error {
	session, err := mongoConn(mongoURI)
	if err != nil {
		return err
	}
	defer session.Close()

	conn, err := amqpConn(amqpURI)
	if err != nil {
		return err
	}
	defer conn.Close()

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
	send, err := pubFunc(conn)
	if err != nil {
		return err
	}

	errs := make(chan error)

	entries := read(queryFor, ts, errs)
	tses := publish(send, entries, errs)

	sigs := make(chan os.Signal)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	var counter int
	for {
		select {
		case sig := <-sigs:
			log.Printf("Receive %v; shutting down", sig)
			return nil
		case ts = <-tses:
			counter = (counter + 1) % freq
			if counter == 0 {
				write(path, ts)
			}
		case err := <-errs:
			return err
		}
	}
}

func main() {
	flag.Parse()

	if err := run(*mongoURIFlag, *amqpURIFlag, *pathFlag, *freqFlag); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
