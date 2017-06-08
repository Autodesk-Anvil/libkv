package dynamo

import (
	"log"
	"testing"
	"time"
)

func TestPutGetDeleteExistsStream(t *testing.T) {
	kv := makeDynamoClient(t)
	stopch := make(chan struct{})
	ch, _ := kv.Watch("testPutGetDeleteExists", stopch)
	loop:
	for {
		select {
		case a := <-ch:
			log.Printf("TestStream(next) %v", a)
		case <-time.After(time.Second * 10):
			close(stopch)
			break loop
		}
	}
}

func TestWatchTreeRoot(t *testing.T) {
	kv := makeDynamoClient(t)
	done := make(chan struct{}, 1)
	key := ""
	ch, err := kv.WatchTree(key, done)
	if err != nil {
		t.Fatalf("streams failed %v", err)
	}
	go func() {
		select {
		case vv := <-ch:
			for v := range vv {
				log.Printf("v:%v", v)
			}
		case <-done:
			return
		}
	}()
	<-time.After(time.Second * 15)
	close(done)
	<-time.After(time.Second)
}

//todo: invoke testutils Watch function
func TestWatchPut(t *testing.T) {
	kv := makeDynamoClient(t)
	done := make(chan struct{}, 1)
	key := "testPut"
	ch, err := kv.Watch(key, done)
	if err != nil {
		t.Fatalf("streams failed %v", err)
	}
	go func() {
		select {
		case v := <-ch:
			log.Printf("v:%v", v)
		case <-done:
			return
		}
	}()
	<-time.After(time.Second * 15)
	close(done)
	<-time.After(time.Second)
}
