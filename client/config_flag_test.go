package client_test

import (
	"flag"
	"github.com/nsqio/nsq/client"
)

func ExampleConfigFlag() {
	cfg := client.NewConfig()
	flagSet := flag.NewFlagSet("", flag.ExitOnError)

	flagSet.Var(&client.ConfigFlag{cfg}, "consumer-opt", "option to pass through to nsq.Consumer (may be given multiple times)")
	flagSet.PrintDefaults()

	err := flagSet.Parse([]string{
		"--consumer-opt=heartbeat_interval,1s",
		"--consumer-opt=max_attempts,10",
	})
	if err != nil {
		panic(err.Error())
	}
	println("HeartbeatInterval", cfg.HeartbeatInterval)
	println("MaxAttempts", cfg.MaxAttempts)
}
