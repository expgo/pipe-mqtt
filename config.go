package pipemqtt

import "errors"

type Config struct {
	Url       string `yaml:"url" json:"url"`
	Topic     string `yaml:"topic" json:"topic"`
	ClientID  string `yaml:"clientid" json:"clientid"`
	Username  string `yaml:"username" json:"username" value:"admin"`
	Password  string `yaml:"password" json:"password" value:"public"`
	CacheSize int    `yaml:"cachesize" json:"cachesize" value:"300"`
}

func (c *Config) Validate() error {
	if c.Url == "" {
		return errors.New("url is required")
	}

	if c.Topic == "" {
		return errors.New("topic is required")
	}

	if c.ClientID == "" {
		return errors.New("clientid is required")
	}

	return nil
}
