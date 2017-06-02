package streams

import (
	"testing"
		"log"
	"time"
)

func TestStream(t *testing.T)  {
	RestDBClient.GetTables()
	ch := StreamDBClient.Watch()
	for {
		select {
			case a:= <-ch :
				log.Printf("TestStream(next) %v",a)
			case <-time.After(time.Second*10):
				StreamDBClient.StopWatch()
				goto done
		}
	}
	done:
		log.Printf("done testing")
}