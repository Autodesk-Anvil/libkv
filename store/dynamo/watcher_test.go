package dynamo

import (
	"log"
	"testing"
	"time"
)

func TestStream(t *testing.T) {
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

func TestWatchTree(t *testing.T) {
	kv := makeDynamoClient(t)
	//_TestStream(kv, t)
	//testutils.RunTestAtomic(t, kv)
	//testutils.RunCleanup(t, kv)
	done := make(chan struct{}, 1)
	//key := "testAtomicPut"
	key := ""
	ch, err := kv.WatchTree(key, done)
	if err != nil {
		t.Fatalf("streams failed %v", err)
	}
	go func() {
		select {
		case <-ch:
		case <-done:
			return
		}
	}()
	<-time.After(time.Second * 15)
	close(done)
	<-time.After(time.Second)
}

//todo: invoke testutils Watch function
func TestWatch(t *testing.T) {
	kv := makeDynamoClient(t)
	done := make(chan struct{}, 1)
	key := ""
	ch, err := kv.Watch(key, done)
	if err != nil {
		t.Fatalf("streams failed %v", err)
	}
	go func() {
		select {
		case <-ch:
		case <-done:
			return
		}
	}()
	<-time.After(time.Second * 15)
	close(done)
	<-time.After(time.Second)
}
