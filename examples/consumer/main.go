package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/client"
)

type ConsumerHandler struct {
	q              *client.Consumer
	messagesGood   int
	messagesFailed int
}

func (h *ConsumerHandler) LogFailedMessage(message *client.Message) {
	fmt.Println("失败")
	h.messagesFailed++
	h.q.Stop()
}

func (h *ConsumerHandler) HandleMessage(message *client.Message) error {
	message.DisableAutoResponse() // 不要自动ack
	msg := string(message.Body)
	fmt.Println(msg)
	h.messagesGood++
	message.Finish()
	return nil
}

func main() {
	topicName := "test"
	msgCount := 10

	config := client.NewConfig()
	config.DefaultRequeueDelay = 0
	config.MaxBackoffDuration = 50 * time.Millisecond
	q, err := client.NewConsumer(topicName, "ch", config)
	if err != nil {
		log.Fatalf(err.Error())
	}
	q.SetLogger(log.New(os.Stdout, "", log.LstdFlags), client.LogLevelInfo)

	h := &ConsumerHandler{
		q: q,
	}
	q.AddHandler(h)

	err = q.ConnectToNSQDs([]string{
		"127.0.0.1:4150",
		"127.0.0.1:4152",
	})
	// err = q.ConnectToNSQLookupd("127.0.0.1:4161")  // 可以通过连接nsqlookupd，来连接每个nsqd，因为nsqlookupd保存了nsqd的连接信息
	if err != nil {
		log.Fatalf(err.Error())
	}
	<-q.StopChan

	if h.messagesGood != msgCount {
		log.Fatalf("end of test. should have handled a diff number of messages %d != %d", h.messagesGood, msgCount)
	}

	if h.messagesFailed != 1 {
		log.Fatal("failed message not done")
	}
}
