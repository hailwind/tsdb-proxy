package main

import (
	"fmt"
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

const (
	VERSION = "1.1"
)

type Backend struct {
	Name  string
	URL   string
	Alive bool
}
type Server struct {
	Scheme  string
	Host   string
	Alive bool
}
type Mapping struct {
	Name        string
	DB          string
	Measurement string
	Backends    []string
	MetricMap   map[string]string
	TagMap      map[string]string
}
type Conf struct {
	ListenAddr  string
	IdleTimeout int
	Mode        string
	Mappings    []Mapping
	Backends    []Backend
	RServers    []Server
}

func (c *Conf) GetInfluxMeta(om string) (im map[string]string, tagmap map[string]string) {
	im = make(map[string]string)
	for _, m := range c.Mappings {
		if v, ok := m.MetricMap[om]; ok {
			im["db"] = m.DB
			im["measurement"] = m.Measurement
			im["metric"] = v
			backUrls := c.getAliveBackendUrl(m.Backends)
			if len(backUrls) >= 1 {
				im["primary_url"] = backUrls[0]
			}
			if len(backUrls) >= 2 {
				im["secondary_url"] = backUrls[1]
			}
			return im, m.TagMap
		}
	}
	return
}

func (c *Conf) getAliveBackendUrl(backends []string) (backUrls []string) {
	for _, ba := range c.Backends {
		for _, backend := range backends {
			if ba.Name == backend && ba.Alive {
				backUrls = append(backUrls, ba.URL)
			}
		}
	}
	return
}

func (c *Conf) GetConf(configPath string) *Conf {
	if configPath == "" {
		configPath = "config.yaml"
	}
	yamlFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		fmt.Println(err.Error())
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		fmt.Println(err.Error())
	}
	return c
}
