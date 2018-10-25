
all: build

build:
	mkdir -p bin
	go build -o bin/tsdb-proxy github.com/hailwind/tsdb-proxy/cmd

clean:
	rm -rf bin