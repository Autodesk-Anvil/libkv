package watcher

import (
	"log"
	"testing"
	"time"
)

var table = "traefik"
var sequenceNumber = "0"

func TestWatcherLatest(t *testing.T) {
	done := make(chan struct{}, 0)
	done2 := make(chan struct{}, 0)
	wc, err := NewWatcher(table, "", done)
	if err != nil {
		t.Fatalf("error %v", err)
	}
	go func() {
		log.Printf("TestWatcherLatest adding a client")
		nn, err := wc.AddClient("", done2, 0, true)
		if err != nil {
			t.Fatalf("error %v", err)
		}
		for v := range nn {
			log.Printf("TestWatcherLatest event %v", v)
		}
		log.Printf("client events over")
	}()
	<-time.After(time.Second * 15)
	log.Printf("closing client")
	close(done2)
	<-time.After(time.Second * 1)
	log.Printf("closing watcher")
	close(done)
	<-time.After(time.Second * 1)
	log.Printf("closing test")
}

func TestWatcherFromStart(t *testing.T) {
	done := make(chan struct{}, 0)
	done2 := make(chan struct{}, 0)
	wc, err := NewWatcher(table, sequenceNumber, done)
	if err != nil {
		t.Fatalf("error %v", err)
	}
	go func() {
		nn, err := wc.AddClient("/", done2, 0, true)
		if err != nil {
			t.Fatalf("error %v", err)
		}
		for v := range nn {
			log.Printf("event %v", v)
		}
		log.Printf("client events over")
	}()
	<-time.After(time.Second * 5)
	log.Printf("closing client")
	close(done2)
	<-time.After(time.Second * 1)
	log.Printf("closing watcher")
	close(done)
	<-time.After(time.Second * 1)
	log.Printf("closing test")
}
