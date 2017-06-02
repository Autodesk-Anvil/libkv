package dynamo

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/dynamo/streams"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for Dynamodb
	ErrMultipleEndpointsUnsupported = errors.New("dynamodb does not support multiple endpoints")
)

// NOTE:
//  The primary key value of the table should a string named "Key"
//  The attributes will be called "Index" and "Value"
type DynamoDB struct {
	tableName string
	client    *dynamodb.DynamoDB
	watcher   *Watcher
}

//watched tracks a particular key or directory
type WatchClient struct {
	prefix string
	feedCh chan *store.KVPair
}

func (wc *WatchClient) Notify(pair *store.KVPair) {
	wc.feedCh <- pair
}

func (wc *WatchClient) IsMatch(s string) bool {
	return strings.Contains(s, wc.prefix)
}

//watcher helps consolidate all the
type Watcher struct {
	sync.RWMutex
	clients map[<-chan struct{}]*WatchClient
}

func (w *Watcher) Monitor(stopCh <-chan struct{}) {
	select {
	case <-stopCh:
		w.RemoveClient(stopCh)
	}
}

//Adds a watcher taking in a stopCh and returns a channel
func (w *Watcher) AddClient(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error ){
	wch := &WatchClient{key, make(chan *store.KVPair, 100)}
	w.Lock()
	defer w.Unlock()
	w.clients[stopCh] = wch
	go w.Monitor(stopCh)
	return wch.feedCh, nil
}

func (w *Watcher) RemoveClient(stopCh <-chan struct{}) {
	w.Lock()
	defer w.Unlock()
	//close the channel
	if client, ok := w.clients[stopCh]; ok {
		delete(w.clients, stopCh)
		//close the feed channel
		close(client.feedCh)
	}
}

func NewWatcher(table string) *Watcher {
	w := &Watcher{clients: make(map[<-chan struct{}]*WatchClient)}
	go w.WatcherLoop(table)
	return w
}

//who triggers watching?
func (w *Watcher) WatcherLoop(table string) {
	
	for {

		// if len(w.clients) == 0 {
		// 	continue
		// }

		//setup the notifyloop
		sink := make(chan *store.KVPair, 10000)
		stopCh := make(chan struct {})
		go w.notifyLoop(sink, stopCh)


		//set up the stream client
		client, err := streams.NewDynamoDBStreamClient(table)
		//todo handle error with backoff
		if err != nil {
			return
		}
		var  input <-chan *dynamodbstreams.Record = client.Watch() 
		for {
			select {
			case rec := <- input:
				//todo: synthesize an event
				fmt.Printf("record(%+v)", rec)
				sink <- &store.KVPair{Key: "a", Value: []byte("b")}
			}
		}

	}
}

func (w *Watcher) notifyLoop(input <-chan *store.KVPair, stopCh <-chan struct {}) {
	var pair *store.KVPair
	for {
		select {
		case pair = <-input:
			w.Lock()
			for _, v := range w.clients {
				if v.IsMatch(pair.Key) {
					v.Notify(pair)
				}
			}
			w.Unlock()
		case <- stopCh :
			break;
		}
	}
}

// Register registers dynamodb to libkv
func Register() {
	libkv.AddStore(store.DYNAMODB, New)
}

func getSession() (*session.Session, error) {
	sess, err := session.NewSession()

	if dynamodbURL := os.Getenv("DYNAMODB_LOCAL"); dynamodbURL != "" {
		fmt.Println("DYNAMODB_LOCAL is set to %v", dynamodbURL)
		c := &aws.Config{
			Endpoint: &dynamodbURL,
		}
		//Create a Session with a custom region
		sess, err = session.NewSession(c)
	}

	// Fail early, if no credentials can be found
	_, err = sess.Config.Credentials.Get()
	return sess, err
}

//this works
func New(endpoints []string, options *store.Config) (store.Store, error) {
	fmt.Printf("\nendpoints : %+v, options : %+v\n", endpoints, options)

	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	fmt.Printf("Dynaodb table name %v\n", endpoints[0])

	sess, err := getSession()
	if err != nil {
		return nil, err
	}

	dyna := &DynamoDB{
		tableName: endpoints[0],
		client:    dynamodb.New(sess),
		watcher:   NewWatcher(endpoints[0]),
	}
	return dyna, nil
}

// Get gets the KVPair of the item stored at 'key' in the db
func (d *DynamoDB) Get(key string) (*store.KVPair, error) {

	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
		},
		TableName: aws.String(d.tableName),
	}

	resp, err := d.client.GetItem(params)
	if err != nil {
		return nil, err
	}
	if len(resp.Item) == 0 {
		return nil, store.ErrKeyNotFound
	}

	pair := &store.KVPair{
		Key: key,
	}
	value, exists := resp.Item["Value"]
	if exists {
		pair.Value = []byte(*value.S)
	}
	pair.LastIndex, _ = strconv.ParseUint(*resp.Item["Index"].N, 10, 64)
	return pair, nil
}

// getPutParams returns an UpdateItemInput struct populated  depending
//  on if value is empty or not
func (d *DynamoDB) getPutParams(key string, value []byte) *dynamodb.UpdateItemInput {
	params := &dynamodb.UpdateItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
		},
		ReturnValues:     aws.String("ALL_NEW"),
		UpdateExpression: aws.String("add #i :i"),
		ExpressionAttributeNames: map[string]*string{
			"#i": aws.String("Index"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":i": {
				N: aws.String("1"),
			},
		},
	}

	if len(value) != 0 {
		// DynamoDB doesn't allow empty values so remove the Value entirely
		params.ExpressionAttributeNames["#v"] = aws.String("Value")
		params.ExpressionAttributeValues[":v"] = &dynamodb.AttributeValue{S: aws.String(string(value[:]))}
		params.UpdateExpression = aws.String("set #v = :v add #i :i")
	}
	return params
}

// Puts the 'key':'value' in the db
func (d *DynamoDB) Put(key string, value []byte, opts *store.WriteOptions) error {

	params := d.getPutParams(key, value)
	fmt.Printf("Dynamodb.Put : %+v", params)
	_, err := d.client.UpdateItem(params)
	return err
}

// Delete deletes the 'key' from the db
func (d *DynamoDB) Delete(key string) error {
	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
		},
		TableName: aws.String(d.tableName),
	}

	_, err := d.client.DeleteItem(params)
	return err
}

// Exists checks to see if the 'key' exists in the db
func (d *DynamoDB) Exists(key string) (bool, error) {
	pair, err := d.Get(key)
	if pair != nil && pair.Key == "" || err == store.ErrKeyNotFound {
		return false, nil
	} else if err == nil {
		return true, nil
	}
	return false, err
}

// List gets all KVPairs whose keys start with 'directory'
func (d *DynamoDB) List(directory string) ([]*store.KVPair, error) {
	pairs := make([]*store.KVPair, 0)
	params := &dynamodb.ScanInput{
		FilterExpression: aws.String("begins_with( #k, :v)"),
		TableName:        aws.String(d.tableName),
		ExpressionAttributeNames: map[string]*string{
			"#k": aws.String("Key"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": {
				S: aws.String(directory),
			},
		},
	}
	// TODO is scan the best way to do this?
	// Maybe a refactor of the key value format will allow
	// a more efficient query to be used or something?
	resp, err := d.client.Scan(params)
	if err != nil {
		return nil, err
	}
	// Scan won't throw an error if no items match the filter
	// so we check it
	if len(resp.Items) == 0 {
		return nil, store.ErrKeyNotFound
	}

	for _, item := range resp.Items {
		tPair := &store.KVPair{
			Key: *item["Key"].S,
		}
		// 'Value' may not exist for every key
		val, exists := item["Value"]
		if exists {
			tPair.Value = []byte(*val.S)
		}
		pairs = append(pairs, tPair)
	}
	return pairs, nil
}

// DeleteTree deletes all keys that start with 'directory'
func (d *DynamoDB) DeleteTree(directory string) error {
	retryList := make([]*store.KVPair, 0)
	pairs, err := d.List(directory)
	if err != nil {
		return err
	}
	for _, pair := range pairs {
		err = d.Delete(pair.Key)
		if err != nil {
			retryList = append(retryList, pair)
		}
	}
	// TODO maybe retry deletes
	if len(retryList) > 0 {
		return fmt.Errorf("Unable to delete all of the tree: %v", retryList)
	}
	return nil
}

// Watch has to be implemented at the library level or be hooked up to a dynamodb stream
//   which might not be likely since AWS only suggests at most two processes reading
//   from a dynamodb stream
func (d *DynamoDB) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	// since scans are expensive maybe we should keep all keys being watched in a map
	// and consolidate our scans of the db into one scan
	return d.watcher.AddClient(key, stopCh)
}

// WatchTree has to be implemented at the library since it is not natively supportedby dynamoDB
func (d *DynamoDB) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, errors.New("WatchTree not supported")
}

// Not supported
func (d *DynamoDB) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, errors.New("NewLock not supported")
}

// AtomicPut put a value at "key" if the key has not been modified in the meantime
func (d *DynamoDB) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	params := d.getPutParams(key, value)

	// if previous provided compare previous values to current values in DB
	if previous != nil {
		params.ConditionExpression = aws.String("#v = :pv AND #i = :pi")
		params.ExpressionAttributeValues[":pv"] = &dynamodb.AttributeValue{
			S: aws.String(string(previous.Value[:])),
		}
		params.ExpressionAttributeValues[":pi"] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatUint(previous.LastIndex, 10)),
		}
	} else {
		// if previous not provided don't put if the item already exists
		params.ConditionExpression = aws.String("attribute_not_exists(#i)")
	}

	resp, err := d.client.UpdateItem(params)
	if err != nil {
		// check to see if the error was failure of the condition
		if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
			return false, nil, store.ErrKeyModified
		}
		return false, nil, err
	}

	tmpIndex, _ := strconv.ParseUint(*resp.Attributes["Index"].N, 10, 64)
	// return new KVPair
	kvPairSuccess := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: tmpIndex,
	}
	return true, kvPairSuccess, nil
}

// AtomicDelete deletes the key if it hasn't been modified or if previous is not provided
func (d *DynamoDB) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if previous == nil {
		if err := d.Delete(key); err != nil {
			return false, err
		}
		return true, nil
	}
	// Delete if the indices match
	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
		},
		TableName:           aws.String(d.tableName),
		ConditionExpression: aws.String("#i = :i"),
		ExpressionAttributeNames: map[string]*string{
			"#i": aws.String("Index"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":i": {
				N: aws.String(strconv.FormatUint(previous.LastIndex, 10)),
			},
		},
	}

	// If there was a value in previous add
	if len(previous.Value) > 0 {
		params.ConditionExpression = aws.String("#v = :v AND #i = :i")
		params.ExpressionAttributeNames["#v"] = aws.String("Value")
		params.ExpressionAttributeValues[":v"] = &dynamodb.AttributeValue{S: aws.String(string(previous.Value[:]))}
	}

	_, err := d.client.DeleteItem(params)
	if err != nil {
		// check to see if it was the unmet condition
		if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
			return false, store.ErrKeyModified
		}
		return false, err
	}
	return true, nil
}

func (d *DynamoDB) Close() {
	return
}
