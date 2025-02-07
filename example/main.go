package main

import (
	"github.com/expgo/factory"
	pipemqtt "github.com/expgo/pipe-mqtt"
	"time"
)

// @Log
//go:generate ag

func main() {
	_ = factory.Find[pipemqtt.MQTT]()

	for i := 0; i < 10; i++ {
		logger.Infof("hello %d", i)
		time.Sleep(time.Second * 1)
	}
}
