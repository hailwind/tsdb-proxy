package main

import (
	"errors"
	"flag"
	"log"
	"time"
	"math/rand"
    "net/url"
    "net/http"
	"net/http/httputil"
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

func converter(conf *Conf) {
	go Ping(&conf.Backends)
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

func ReverseProxy(targets []url.URL) *httputil.ReverseProxy {
	director := func(req *http.Request) {
			target := targets[rand.Int()%len(targets)]
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.URL.Path = target.Path
	}
	return &httputil.ReverseProxy{Director: director}
}

func reverse(conf *Conf) {
	var urls []url.URL
	for _, rserver := range conf.RServers {
		urls = append(urls, url.URL{
			Scheme: rserver.Scheme,
			Host: rserver.Host,
		})
	}
	reverseProxy := ReverseProxy(urls)
	var err = http.ListenAndServe(conf.ListenAddr, reverseProxy)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	var c Conf
	conf := c.GetConf(ConfigFile)
	log.Printf("http service start at %s.", conf.ListenAddr)
	if conf.Mode == "reverse" {
		reverse(conf)
	}else{
		converter(conf)
	}
}
