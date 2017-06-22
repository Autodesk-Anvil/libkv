package streams

import (
	"flag"
	"log"
	"testing"
	"time"
)

var (
	FlagTable          = flag.String("table", "traefik", "Dynamodb table name")
	FlagSequenceNumber = flag.String("sequencenumber", "00000000000000000000000001", "Dynamodb table sequence number to stream after")
)

//todo: add assertions etc.
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
	//close the done channel after some time
	<-time.After(time.Second * 10)
	log.Printf("finished testing")
}
