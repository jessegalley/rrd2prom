package main

import (
	"fmt"
	"time"

	// "github.com/davecgh/go-spew/spew"
	"github.com/jessegalley/rrd2prom"
)


func main() {

  url := ""
  // rrdFile, err := rrd2prom.NewRRDFile(url, "port1")
  // if err != nil {
  //   fmt.Printf("couldn't open rrd file at: %s (%v)", url, err)
  // }

  // spew.Dump(rrdFile)

  
  for {
    rrdFile, err := rrd2prom.NewRRDFile(url, "port1")
    if err != nil {
      fmt.Printf("couldn't open rrd file at: %s (%v)", url, err)
    }
    for dsname, v := range rrdFile.DataSources {
      fmt.Printf("%s\t%s: %d\n", rrdFile.Name, dsname, v.LastValue)
    }
    time.Sleep(rrdFile.Interval)
     
  }
 
}

