package dynamo

import (
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
	//"time"
	//"fmt"
	"log"
	"time"
)

var client = "traefik"

func makeDynamoClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			Bucket: "us-east-1",
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()
	kv, err := libkv.NewStore(store.DYNAMODB, []string{client}, &store.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*DynamoDB); !ok {
		t.Fatal("Error registering and initializing dynamodb")
	}
}

func _TestStream(kv store.Store, t *testing.T)  {
	stopch := make(chan struct {})
	ch, _ := kv.Watch("testPutGetDeleteExists", stopch)
	for {
		select {
			case a:= <-ch :
				log.Printf("TestStream(next) %v",a)
			case <-time.After(time.Second*10):
				stopch <- struct{}{}
				goto done
		}
	}
	done:
		log.Printf("done testing")
}

func TestDynamoDBStore(t *testing.T) {
	kv := makeDynamoClient(t)
	//_TestStream(kv, t)
	testutils.RunTestAtomic(t, kv)
	testutils.RunCleanup(t, kv)
}
