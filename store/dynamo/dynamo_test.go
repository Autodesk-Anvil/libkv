package dynamo

import (
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/stretchr/testify/assert"
	"testing"
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
	kv, err := libkv.NewStore(DYNAMODBSTORE, []string{client}, &store.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*DynamoDB); !ok {
		t.Fatal("Error registering and initializing dynamodb")
	}
}

