package rrd2prom

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
  "crypto/tls"

	"github.com/davecgh/go-spew/spew"
	"github.com/ziutek/rrd"
)

type RRDFile struct {
  Location     string
  Name         string 
  Interval     time.Duration 
  LastUpdate   time.Time 
  DataSources  map[string]RRDDataSource
}

type RRDDataSource struct {
  Name       string 
  Type       string
  Index      uint
  LastValue  uint64
}


func init(){
  spew.Dump(nil) //lol 
}

// NewRRDFile constructs and returns an RRDFile struct from an 
// actual RRD file found at fileLocation. fileLocation can be 
// either a system path, or an HTTP URL.
// Will return an error if the file is inacessible for any reason 
// at either method.
func NewRRDFile (fileLocation, name string) (*RRDFile, error) {
  rrdFile := RRDFile{
    Location: fileLocation,
    Name: name,
    DataSources: make(map[string]RRDDataSource),
  }

  err := rrdFile.readRRD()
  if err != nil {
    return &rrdFile, err
  }

  return &rrdFile, nil
}

// getRRDInfo abstracts the common logic for getting RRD info from either
// a local file or URL source
func (r *RRDFile) getRRDInfo() (map[string]interface{}, error) {
    if isURL(r.Location) {
        // Create temp file for HTTP source
        tmpFile, err := os.CreateTemp("", "rrd-*")
        if err != nil {
            return nil, fmt.Errorf("failed to create temp file: %v", err)
        }
        defer os.Remove(tmpFile.Name())
        defer tmpFile.Close()

        tr := &http.Transport{
            TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        }
        client := &http.Client{Transport: tr}

        resp, err := client.Get(r.Location)
        if err != nil {
            return nil, fmt.Errorf("failed to download RRD: %v", err)
        }
        defer resp.Body.Close()

        if resp.StatusCode != http.StatusOK {
            return nil, fmt.Errorf("bad status: %s", resp.Status)
        }

        if _, err := io.Copy(tmpFile, resp.Body); err != nil {
            return nil, fmt.Errorf("failed to save RRD: %v", err)
        }

        return rrd.Info(tmpFile.Name())
    } 
    
    return rrd.Info(r.Location)
}

// Update refreshes only the last update time and data source values
// without reparsing unchanging metadata.
// I don't _believe_ that .rrd metadata changes, but if it does this 
// approach will need to be changed.
func (r *RRDFile) Update() error {
    info, err := r.getRRDInfo()
    if err != nil {
        return fmt.Errorf("couldn't read RRD file: %v", err)
    }

    // update last_update timestamp
    if err := r.parseLastUpdate(info); err != nil {
        return err
    }

    // update only the last values, not the full DS metadata
    dsLast, ok := info["ds.last_ds"].(map[string]interface{})
    if !ok {
        return fmt.Errorf("couldn't parse ds last values")
    }

    for dsName, ds := range r.DataSources {
        if lastVal, exists := dsLast[dsName]; exists {
            lastValStr, ok := lastVal.(string) 
            if !ok {
                return fmt.Errorf("invalid last_ds value type for %s", dsName)
            }
            lastValUint, err := strconv.ParseUint(lastValStr, 10, 64)
            if err != nil {
                return fmt.Errorf("couldn't parse last_ds value for %s: %v", dsName, err)
            }
            ds.LastValue = lastValUint
            r.DataSources[dsName] = ds
        }
    }

    return nil
}

// readRRD attempts to read and parse an RRD file from either a local path or URL
func (r *RRDFile) readRRD() error {
    info, err := r.getRRDInfo()
    if err != nil {
        return fmt.Errorf("couldn't open rrd file at: %s (%v)", r.Location, err)
    }

    // parse all the RRD metadata
    if err := r.parseLastUpdate(info); err != nil {
        return err
    }

    if err := r.parseStep(info); err != nil {
        return err
    }

    if err := r.parseDS(info); err != nil {
        return err
    }

    return nil
}



func (r *RRDFile) parseDS(info map[string]interface{}) error {
  // dump structure of fields with which we are concerned:
  // (string) (len=10) "ds.last_ds": (map[string]interface {}) (len=2) {
  //  (string) (len=10) "traffic_in": (string) (len=15) "321105865553987",
  //  (string) (len=11) "traffic_out": (string) (len=14) "53340229448019"
  // },
  // (string) (len=8) "ds.index": (map[string]interface {}) (len=2) {                              
  //  (string) (len=10) "traffic_in": (uint) 0,                                                    
  //  (string) (len=11) "traffic_out": (uint) 1
  // },  
  //  (string) (len=10) "traffic_in": (uint) 0,
  //  (string) (len=11) "traffic_out": (uint) 1
  // },                               
  // (string) (len=7) "ds.type": (map[string]interface {}) (len=2) {
  //  (string) (len=10) "traffic_in": (string) (len=7) "COUNTER",
  //  (string) (len=11) "traffic_out": (string) (len=7) "COUNTER"
  // },
  //

  // the name, type, and last are important for exporting a raw value  
  // via prom.  the index is importand if we were to utilize the 
  // native rrd FETCH calls, since the index is the only way we can tell 
  // which row is which field, but we won't use those here.  we'll 
  // parse it anyway though incase there is a need for future use


  // this rrd library has these types as nested map[string]interface{} 
  // so we'll need to do some double assertion shenanigans in order 
  // to extract all the fields. 

  // set up some maps to temporarily hold the data after assertion 
  typesMap := make(map[string]string)
  indexMap := make(map[string]uint)
  lastMap  := make(map[string]uint64)  


  // double assert the types map
  dsTypes, ok := info["ds.type"].(map[string]interface{})
  if !ok {
    return fmt.Errorf("couldn't parse ds types from %s", r.Location)
  }

  for k, v := range dsTypes {
    dsType, ok := v.(string)
    if !ok {
      return fmt.Errorf("couldn't parse ds types from %s", r.Location)
    }
    typesMap[k] = dsType
  }

  // double assert the indexes map 
  dsIndexes, ok := info["ds.index"].(map[string]interface{})
  if !ok {
    return fmt.Errorf("couldn't parse ds indexes from %s", r.Location)
  }
  for k, v := range dsIndexes  {
    dsIndex, ok := v.(uint)
    if !ok {
      return fmt.Errorf("couldn't parse ds indexes from %s", r.Location)
    }
    indexMap[k] = dsIndex
  }

  // double assert the last_ds map 
  dsLast, ok := info["ds.last_ds"].(map[string]interface{})
  if !ok {
    return fmt.Errorf("couldn't parse ds last from %s", r.Location)
  }
  for k, v := range dsLast {
    lastDs, ok := v.(string)
    if !ok {
      return fmt.Errorf("couldn't parse ds last from %s", r.Location)
    }
    lastDsUint, err := strconv.ParseUint(lastDs, 10, 64)
    if err != nil {
      return fmt.Errorf("couldn't parse ds last from %s", r.Location)
    }
    lastMap[k] = lastDsUint 
  }


  // all the assertion shenanigans are done with, so now we'll create 
  // the ds instances and add them to the rrdfile struct field
  for k, v := range indexMap {
    ds := RRDDataSource{
      Name: k,
      Index: v,
      Type: typesMap[k],
      LastValue: lastMap[k],
    }
    r.DataSources[k] = ds
  }

  // spew.Dump(typesMap)
  // spew.Dump(indexMap)
  // spew.Dump(lastMap)

  return nil
}

func (r *RRDFile) parseStep(info map[string]interface{}) error {
  //  (string) (len=4) "step": (uint) 60,          
  stepVal, ok := info["step"].(uint)
  if !ok {
    return fmt.Errorf("couldn't parse step from %s", r.Location)
  }

  r.Interval = time.Second * time.Duration(stepVal)
  
  return nil
}


// parseLastUpdate takes the map of RRD info returned by rrd.Info() 
// and pulls out the last_update field, converting it to native time.Time 
// and updating the instance of RRDFile with this value.
// Fails only if the unix timestamp couldn't be parsed/asserted from 
// the info map.
func (r *RRDFile) parseLastUpdate(info map[string]interface{}) error {
  // rrd keeps the lastupdate in a unix timestamp (uint)
  // but all of the rrd info fields are interfaces in the 
  // c wrapper, so we'll need to type assert back to uint 
  // to parse the time
  //  (string) (len=11) "last_update": (uint) 1735589344,
  lastUpdateVal, ok := info["last_update"].(uint)
  if !ok {
    return fmt.Errorf("couldn't parse last_update from %s", r.Location)
  }

  // spew.Dump(info["last_update"])
  // now since golang time package expects an int64 when 
  // parsing unix timestamps, we'll have to explicitly 
  // cast it as such in order to get a time.Time
  lastUpdateInt := int64(lastUpdateVal)
  lastUpdate := time.Unix(lastUpdateInt, 0)

  r.LastUpdate = lastUpdate

  return nil
}

// isURL simply checks if str is a URL.
func isURL(str string) bool {
    u, err := url.Parse(str)
    return err == nil && (u.Scheme == "http" || u.Scheme == "https")
}
