package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	// "github.com/davecgh/go-spew/spew"
	"github.com/jessegalley/rrd2prom"
)

func main() {
	var (
		rrdURL = flag.String("url", "", "URL or path of the RRD file to monitor")
		name   = flag.String("name", "default", "Name identifier for the RRD metrics")
	)

	flag.Parse()

	if *rrdURL == "" {
		flag.Usage()
		os.Exit(1)
	}

	// create the RRD file
	rrdFile, err := rrd2prom.NewRRDFile(*rrdURL, *name)
	if err != nil {
		log.Fatalf("couldn't open rrd file at: %s (%v)", *rrdURL, err)
	}

	// create manager with our RRD file
	manager, err := rrd2prom.NewRRDManager([]*rrd2prom.RRDFile{rrdFile})
	if err != nil {
		log.Fatalf("couldn't create manager: %v", err)
	}

  // spew.Dump(manager)
	// set up signal handling for graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// start a goroutine to handle messages and metrics
	go func() {
		for {
			select {
			case msg := <-manager.Msgs:
				fmt.Printf("MSG: %s\n", msg)
			case err := <-manager.Errors:
				fmt.Printf("ERROR: %v\n", err)
			case metric := <-manager.Metrics:
				fmt.Printf("METRIC: %s{source=\"%s\"} %d [%v]\n", 
					metric.Name, 
					metric.Source, 
					metric.Value,
					metric.Timestamp)
			}
		}
	}()

	// start the manager
	go manager.Run()

	// wait for shutdown signal
	<-signals
	fmt.Println("\nShutting down...")
	
	// stop the manager and wait for cleanup
	manager.Stop()
}
