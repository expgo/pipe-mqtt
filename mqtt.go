package pipemqtt

import (
	"context"
	"errors"
	"time"

	"github.com/expgo/factory"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/expgo/config"
	"github.com/expgo/log"
	logpipe "github.com/expgo/log-pipe"
	"github.com/expgo/serve"
	"github.com/thejerf/suture/v4"
)

//go:generate ag

// @Singleton
type MQTT struct {
	log.InnerLog
	pipe       *logpipe.Pipe `wire:"auto"`
	token      suture.ServiceToken
	cfg        *Config
	subscriber *logpipe.Subscriber
	client     mqtt.Client
}

func (m *MQTT) Init() {
	m.cfg = factory.New[Config]()
	config.GetConfig(m.cfg, log.DefaultConfigPath, "pipe", "mqtt")

	m.subscriber = m.pipe.Subscribe(m.cfg.CacheSize)

	m.token = serve.AddServe("pipe-mqtt", m)
}

func (m *MQTT) Serve(ctx context.Context) error {
	if m.client == nil {
		if err := m.cfg.Validate(); err != nil {
			time.Sleep(time.Millisecond * 50)
			return err
		}

		opts := mqtt.NewClientOptions()
		opts.AddBroker(m.cfg.Url)
		opts.SetClientID(m.cfg.ClientID)
		opts.SetUsername(m.cfg.Username)
		opts.SetPassword(m.cfg.Password)
		opts.SetAutoReconnect(true)
		opts.SetMaxReconnectInterval(time.Second * 30)
		opts.SetConnectRetry(true)
		opts.SetConnectRetryInterval(time.Second * 5)
		opts.SetKeepAlive(time.Second * 60)
		opts.SetPingTimeout(time.Second * 10)
		opts.SetWriteTimeout(time.Second * 5)
		opts.SetOrderMatters(false)

		client := mqtt.NewClient(opts)

		// Initial connection attempt
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}
		defer client.Disconnect(250) // Graceful disconnect with 250ms timeout

		m.client = client
	}

	for {
		// Check topic length before select
		topic := m.cfg.Topic
		if len(topic) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond * 50): // Wait for 1 second before checking again
				continue
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-m.subscriber.Channel():
			if !ok {
				// Channel has been closed
				return errors.New("subscriber channel closed")
			}

			client := m.client
			if client == nil {
				return errors.New("client is nil")
			}

			if !client.IsConnected() {
				return errors.New("client is not connected")
			}

			token := client.Publish(topic, 0, false, msg)
			if token.Wait() && token.Error() != nil {
				return token.Error()
			}
		}
	}
}

func (m *MQTT) UpdateConfig(cfg *Config) error {
	if m.cfg == nil {
		return errors.New("config is nil")
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	m.cfg = cfg

	if m.client != nil {
		m.client.Disconnect(250)
		m.client = nil
	}
	return nil
}

func (m *MQTT) UpdateCacheSize(cacheSize int) {
	m.subscriber.Close()
	m.subscriber = m.pipe.Subscribe(cacheSize)
}

func (m *MQTT) SetClient(client mqtt.Client) {
	if m.client != nil {
		m.client.Disconnect(250)
	}

	m.client = client
}

func (m *MQTT) SetTopic(topic string) {
	m.cfg.Topic = topic
}
