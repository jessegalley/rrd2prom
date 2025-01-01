package main

import (
	"fmt"
	"time"
  "log"
  "flag"
  "os"

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

    rrdFile, err := rrd2prom.NewRRDFile(*rrdURL, *name)
    if err != nil {
        log.Fatalf("couldn't open rrd file at: %s (%v)", *rrdURL, err)
    }

    for {
        if err := rrdFile.Update(); err != nil {
            log.Printf("couldn't update RRD file: %v\n", err)
            continue
        }

        for dsname, v := range rrdFile.DataSources {
            fmt.Printf("%s\t%s: %d\n", rrdFile.Name, dsname, v.LastValue)
        }
        
        time.Sleep(rrdFile.Interval)
    }
}
