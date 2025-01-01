export CGO_ENABLED=1

build:
	go build -o bin/rrd2promd cmd/rrd2promd/main.go

run: build
	./bin/rrd2promd

test:
	go test -v ./... 
