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
  // rrdFile := RRDFile{
  //   Location: fileLocation,
  //   Name: name,
  //   DataSources: make(map[string]RRDDataSource),
  // }
  //
  // // read from either http or file 
  // if isURL(fileLocation){
  //   // get from http
  //   err := rrdFile.ReadRRDFromUrl(fileLocation)
  //   if err != nil {
  //     return &rrdFile, err
  //   }
  // } else {
  //   // assume local file path 
  //   err := rrdFile.ReadRRDFromFile(fileLocation)
  //   if err != nil {
  //     return &rrdFile, err
  //   }
  // }
  // 
  // return &rrdFile, nil
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

// // ReadRRDFromFile attempts to read a local .rrd file.
// func (r *RRDFile) ReadRRDFromFile(path string) error {
//   // get the RRD file info
//   info, err := rrd.Info(path)
//   if err != nil {
//     return fmt.Errorf("couldn't open rrd file at: %s (%v)", path, err)
//   }
//
//   // parse the last_update field
//   err = r.parseLastUpdate(info)
//   if err != nil {
//     return err
//   }
//
//   // parse the step field 
//   err = r.parseStep(info)
//   if err != nil {
//     return err 
//   }
//   
//   // parse the datsources 
//   err = r.parseDS(info)
//   if err != nil {
//     return err 
//   }
//
//   // spew.Dump(info)
//   return nil 
// }

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

// // ReadRRDFeomUrl attempts to get a remote .rrd file from url, saving
// // it to a temp file which can then be read from via the file parsing
// // methods.
// func (r *RRDFile) ReadRRDFromUrl(urlStr string) error {
//     // create temp file to store downloaded RRD
//     tmpFile, err := os.CreateTemp("", "rrd-*")
//     if err != nil {
//         return fmt.Errorf("failed to create temp file: %v", err)
//     }
//     defer os.Remove(tmpFile.Name())
//     defer tmpFile.Close()
//
//     // download file
//     // resp, err := http.Get(urlStr)
//     // if err != nil {
//     //     return fmt.Errorf("failed to download RRD: %v", err)
//     // }
//     // defer resp.Body.Close()
//
//     //TODO: toggle insecure transport functionality with -K 
//     // create insecure transport
//     tr := &http.Transport{
//         TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
//     }
//     client := &http.Client{Transport: tr}
//
//     // Download file using custom client
//     resp, err := client.Get(urlStr)
//     if resp.StatusCode != http.StatusOK {
//         return fmt.Errorf("bad status: %s", resp.Status)
//     }
//
//     // copy to temp file
//     if _, err := io.Copy(tmpFile, resp.Body); err != nil {
//         return fmt.Errorf("failed to save RRD: %v", err)
//     }
//
//     // parse the downloaded file via the existing code 
//     return r.ReadRRDFromFile(tmpFile.Name())
// }

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
  //
  // for k, v := range typesMap {
  //   r.DataSources[k].Type = v
  // }
  //
  // for k, v := range lastMap {
  //   r.DataSources[k].LastValue = uint64(v)
  // }

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

// func getDSTypes(filename string) {
//   info, err := rrd.Info(filename)
//   if err != nil {
//     fmt.Println("couldn't get rrd info")
//     os.Exit(1)
//   }
//
//   dsTypes, ok := info["ds.type"].(map[string]interface{})
//   if !ok {
//     fmt.Println("couldn't parse ds types with assertion")
//     os.Exit(2)
//   }
//
//   for dsName, dsType := range  dsTypes {
//     fmt.Println(dsName, dsType)
//   }
// }
//
// func getLastUpdate(filename string) time.Time {
//   info, err := rrd.Info(filename)
//   if err != nil {
//     fmt.Println("couldn't get rrd info")
//     os.Exit(1)
//   }
//
//   //  (string) (len=11) "last_update": (uint) 1735589344,
//   lastUpdateVal, ok := info["last_update"].(uint)
//   if !ok {
//     fmt.Println("couldn't parse last update with type assertion")
//     os.Exit(3)
//   }
//
//   // lastUpdateUint := info["last_update"].(uint)
//   // Explicit conversion to int64, checking for overflow if needed
//   lastUpdateInt := int64(lastUpdateVal)
//   lastUpdate := time.Unix(lastUpdateInt, 0)
//
//   // spew.Dump(info)
//   return lastUpdate
// }
//
// func main() {
//   // spew.Dump(nil)
//   // Open the RRD file
//   const filename = "./testdata/port1.rrd"
//
//   getDSTypes(filename)
//   lastUpdate := getLastUpdate(filename)
//
//   fmt.Println("last_update: ", lastUpdate)
//
//   // Specify the time range you want to fetch
//   // end := time.Now()
//   // lastMinute := lastUpdate.Truncate(time.Minute)
//   // if lastUpdate.Sub(lastMinute) > 0 {
//   //   // the last update minus the last update minute has some seconds leftover
//   //   // this means that the last full elapsed minute might not contain data 
//   //   // inside the rrd file, so we'll move the pointer back one minute 
//   //   // to make sure we get valid data 
//   //   lastMinute = lastMinute.Add(1 * -time.Minute)
//   // }
//   // end := lastMinute
//   end := lastUpdate
//   start := end.Add(-5 * time.Minute) // last 5  minutes in the file 
//
//   // Fetch data from RRD
//   data, err := rrd.Fetch(filename, "AVERAGE", start, end, time.Minute)
//   if err != nil {
//     panic(err)
//   }
//   defer data.FreeValues()
//
//   // Print some metadata about the fetch
//   fmt.Printf("Data sources: %v\n", data.DsNames)
//   fmt.Printf("Step: %v\n", data.Step)
//   fmt.Printf("Start: %v\n", data.Start)
//   fmt.Printf("End: %v\n", data.End)
//   // spew.Dump(data.Values())
//
//   fmt.Printf("Last Update: %v\n", lastUpdate)
//   // fmt.Printf("Last Update Minute: %v\n", lastMinute)
//   // fmt.Printf("End: %v\n", end)
//   // fmt.Printf("Start: %v\n", start)
//   values := data.Values()
//   for i := 0; i < len(values); i += 2 {
//     if i+1 >= len(values) {
//       break
//     }
//     timestamp := data.Start.Add(time.Duration(i/2) * data.Step)
//     if timestamp.After(end) {
//       break
//     }
//     if !math.IsNaN(values[i]) && !math.IsNaN(values[i+1]) {
//       fmt.Printf("Time: %v\n  traffic_in: %f\n  traffic_out: %f\n", 
//       timestamp, values[i], values[i+1])
//     }
//   }
//   // for i, v := range data.Values() {
//   //   spew.Dump(v)
//   //   fmt.Printf("%d:\t%f\n", i, v)
//   // }
//
//   _, err = rrd.Info(filename)
//   if err != nil {
//     fmt.Println("couldn't get rrd info")
//     os.Exit(1)
//   }
//   // spew.Dump(info["ds.last_ds"])
// }
//
