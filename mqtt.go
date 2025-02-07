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

	m.L.Debugf("Initializing MQTT client with config: %+v", m.cfg)
	m.subscriber = m.pipe.Subscribe(m.cfg.CacheSize)

	m.token = serve.AddServe("pipe-mqtt", m)
	m.L.Info("MQTT client initialized")
}

func (m *MQTT) Serve(ctx context.Context) error {
	if m.client == nil {
		if err := m.cfg.Validate(); err != nil {
			m.L.Errorf("Config validation failed: %v", err)
			time.Sleep(time.Millisecond * 50)
			return err
		}

		m.L.Debugf("Creating new MQTT client with broker URL: %s", m.cfg.Url)
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
		m.L.Info("Attempting to connect to MQTT broker")
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			m.L.Errorf("Failed to connect to MQTT broker: %v", token.Error())
			return token.Error()
		}
		m.L.Info("Successfully connected to MQTT broker")
		defer client.Disconnect(250)

		m.client = client
	}

	for {
		// Check topic length before select
		topic := m.cfg.Topic
		if len(topic) == 0 {
			m.L.Debug("Topic is empty, waiting for topic to be set")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond * 50):
				continue
			}
		}

		select {
		case <-ctx.Done():
			m.L.Info("Context cancelled, shutting down MQTT client")
			return ctx.Err()
		case msg, ok := <-m.subscriber.Channel():
			if !ok {
				m.L.Error("Subscriber channel closed")
				return errors.New("subscriber channel closed")
			}

			client := m.client
			if client == nil {
				m.L.Error("MQTT client is nil")
				return errors.New("client is nil")
			}

			if !client.IsConnected() {
				m.L.Error("MQTT client is not connected")
				return errors.New("client is not connected")
			}

			m.L.Debugf("Publishing message to topic: %s", topic)
			token := client.Publish(topic, 0, false, msg)
			if token.Wait() && token.Error() != nil {
				m.L.Errorf("Failed to publish message: %v", token.Error())
				return token.Error()
			}
			m.L.Debug("Message published successfully")
		}
	}
}

func (m *MQTT) UpdateConfig(cfg *Config) error {
	if m.cfg == nil {
		m.L.Error("Config is nil")
		return errors.New("config is nil")
	}

	if err := cfg.Validate(); err != nil {
		m.L.Errorf("Invalid config: %v", err)
		return err
	}

	m.L.Infof("Updating MQTT config: %+v", cfg)
	m.cfg = cfg

	if m.client != nil {
		m.L.Debug("Disconnecting existing MQTT client")
		m.client.Disconnect(250)
		m.client = nil
	}
	return nil
}

func (m *MQTT) UpdateCacheSize(cacheSize int) {
	m.L.Infof("Updating cache size to: %d", cacheSize)
	m.subscriber.Close()
	m.subscriber = m.pipe.Subscribe(cacheSize)
}

func (m *MQTT) SetClient(client mqtt.Client) {
	m.L.Debug("Setting MQTT client")
	if m.client != nil {
		m.L.Debug("Disconnecting existing client")
		m.client.Disconnect(250)
	}

	m.client = client
	m.L.Info("MQTT client set")
}

func (m *MQTT) SetTopic(topic string) {
	m.L.Infof("Setting topic: %s", topic)
	m.cfg.Topic = topic
}
