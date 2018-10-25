package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"time"
)

var (
	ErrConfig   = errors.New("config parse error")
	ConfigFile  string
	LogFilePath string
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	flag.StringVar(&LogFilePath, "log", "", "logging file")
	flag.StringVar(&ConfigFile, "config", "", "config file")
	flag.Parse()
}

func main() {
	var c Conf
	conf := c.GetConf(ConfigFile)
	log.Printf("http service start at %s.", conf.ListenAddr)

	go Ping(&c.Backends)
	var err error
	mux := http.NewServeMux()
	NewHttpService(conf).Register(mux)

	server := &http.Server{
		Addr:        conf.ListenAddr,
		Handler:     mux,
		IdleTimeout: time.Duration(conf.IdleTimeout) * time.Second,
	}

	err = server.ListenAndServe()
	if err != nil {
		log.Print(err)
		return
	}
}
