package streams

import (
	"log"
	"testing"
	"time"
	"flag"
)

var (
	FlagTable = flag.String("table", "", "Dynamodb table name")
	FlagSequenceNumber = flag.String("sequencenumber", "00000000000000000000000001", "Dynamodb table sequence number to stream after")
)

func TestStream(t *testing.T) {
	done := make(chan struct{}, 0)
	flag.Parse()
	client, err := NewDynamoDBStreamClient(*FlagTable, *FlagSequenceNumber, done)
	if err != nil {
		t.Fatalf("failed")
	}

	loop:
	for {
		select {
		case a := <-client.Ch:
			log.Printf("event(%v Type %T)", a, a)
		case <-time.After(time.Second * 6):
			log.Printf("closing done")
			close(done)
			break loop
		}
	}
	<-time.After(time.Second*10)
	log.Printf("finished testing")
}
