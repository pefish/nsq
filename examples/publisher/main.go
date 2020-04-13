package main

import (
	"io/ioutil"
	"log"
	"strconv"

	"github.com/nsqio/nsq/client"
)

func main() {
	topicName := "test"
	msgCount := 1000

	config := client.NewConfig()
	w, _ := client.NewProducer("127.0.0.1:4150", config)
	w.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags), client.LogLevelInfo)
	defer w.Stop()

	for i := 0; i < msgCount; i++ {
		err := w.Publish(topicName, []byte(strconv.Itoa(i)))
		if err != nil {
			log.Fatalf("error %s", err)
		}
	}

	// err := w.Publish(topicName, []byte("bad_test_case"))
	// if err != nil {
	// 	log.Fatalf("error %s", err)
	// }
}
