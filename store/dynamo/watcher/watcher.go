package watcher

import (
	"github.com/autodesk-anvil/libkv/store/dynamo/streams"
	"github.com/docker/libkv/store"
	"strconv"
	"strings"
	"sync"
)

//WatchClient tracks a particular key or directory
type watchClient struct {
	prefix      string
	receiver    chan<- *store.KVPair
	lastIndex   uint64
	isDirectory bool
}

func (wc *watchClient) notify(pair *store.KVPair) {
	if wc.isMatch(pair.Key) {
		wc.receiver <- pair
	}
}

func (wc *watchClient) isMatch(s string) bool {
	//fmt.Printf("isMatch(%v, %v)", wc.prefix, s)
	if !wc.isDirectory {
		return s == wc.prefix
	} else {
		return strings.HasPrefix(s, wc.prefix)
	}
}

//watcher helps consolidate all the watches
type Watcher struct {
	sync.RWMutex
	clients map[<-chan struct{}]*watchClient
	stream  *streams.StreamClient
}

func NewWatcher(table string, sequenceNumber string, done <-chan struct{}) (*Watcher, error) {
	s, err := streams.NewDynamoDBStreamClient(table, sequenceNumber, done)
	if err != nil {
		return nil, err
	}
	w := &Watcher{stream: s, clients: make(map[<-chan struct{}]*watchClient)}

	//monitor stream
	go func(w *Watcher) {
		for {
			select {
			case e := <-w.stream.Ch:
				if e == nil || e.Dynamodb == nil || e.Dynamodb.Keys == nil {
					continue
				}
				key := e.Dynamodb.Keys["Key"].S
				pair := &store.KVPair{
					Key: *key,
				}

				if e.Dynamodb.NewImage == nil || len(e.Dynamodb.NewImage) == 0 {
					continue
				}

				item := e.Dynamodb.NewImage

				value, exists := item["Value"]
				if exists {
					pair.Value = []byte(*value.S)
				}
				pair.LastIndex, _ = strconv.ParseUint(*item["Index"].N, 10, 64)

				for _, v := range w.clients {
					v.notify(pair)
				}

			case <-done:
				w.Lock()
				defer w.Unlock()
				for k, v := range w.clients {
					close(v.receiver)
					delete(w.clients, k)
				}
				return
			}
		}
	}(w)

	return w, nil
}

//Adds a watcher taking in a stopCh and returns a channel
//todo: change lastIndex to be a string
func (w *Watcher) AddClient(key string, stopCh <-chan struct{}, lastIndex uint64, isDir bool) (<-chan *store.KVPair, error) {
	ch := make(chan *store.KVPair, 10000)

	wc := &watchClient{key, ch, lastIndex, isDir}

	w.Lock()
	w.clients[stopCh] = wc
	w.Unlock()

	//monitor the close channel
	go w.monitor(stopCh)
	return ch, nil
}

//Removes a watcher for added previously a given stopCh
func (w *Watcher) removeClient(stopCh <-chan struct{}) {
	w.Lock()
	defer w.Unlock()
	//close the channel
	if client, ok := w.clients[stopCh]; ok {
		delete(w.clients, stopCh)
		close(client.receiver)
	}
}

//todo: Find out if there is a better way than spawning a goroutine for each one?
func (w *Watcher) monitor(stopCh <-chan struct{}) {
	select {
	case <-stopCh:
		w.removeClient(stopCh)
	}
}
