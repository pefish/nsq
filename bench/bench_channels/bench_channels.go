package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/nsqio/nsq/client"
	"net"
	"sync"
	"time"

)

var (
	num        = flag.Int("num", 10000, "num channels")
	tcpAddress = flag.String("nsqd-tcp-address", "127.0.0.1:4150", "<addr>:<port> to connect to nsqd")
)

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < *num; j++ {
		wg.Add(1)
		go func(id int) {
			subWorker(*num, *tcpAddress, fmt.Sprintf("t%d", j), "ch", rdyChan, goChan, id)
			wg.Done()
		}(j)
		<-rdyChan
		time.Sleep(5 * time.Millisecond)
	}

	close(goChan)
	wg.Wait()
}

func subWorker(n int, tcpAddr string,
	topic string, channel string,
	rdyChan chan int, goChan chan int, id int) {
	conn, err := net.DialTimeout("tcp", tcpAddr, time.Second)
	if err != nil {
		panic(err.Error())
	}
	conn.Write(client.MagicV2)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	ci := make(map[string]interface{})
	ci["client_id"] = "test"
	cmd, _ := client.Identify(ci)
	cmd.WriteTo(rw)
	client.Subscribe(topic, channel).WriteTo(rw)
	rdyCount := 1
	rdy := rdyCount
	rdyChan <- 1
	<-goChan
	client.Ready(rdyCount).WriteTo(rw)
	rw.Flush()
	client.ReadResponse(rw)
	client.ReadResponse(rw)
	for {
		resp, err := client.ReadResponse(rw)
		if err != nil {
			panic(err.Error())
		}
		frameType, data, err := client.UnpackResponse(resp)
		if err != nil {
			panic(err.Error())
		}
		if frameType == client.FrameTypeError {
			panic(string(data))
		} else if frameType == client.FrameTypeResponse {
			client.Nop().WriteTo(rw)
			rw.Flush()
			continue
		}
		msg, err := client.DecodeMessage(data)
		if err != nil {
			panic(err.Error())
		}
		client.Finish(msg.ID).WriteTo(rw)
		rdy--
		if rdy == 0 {
			client.Ready(rdyCount).WriteTo(rw)
			rdy = rdyCount
			rw.Flush()
		}
	}
}
