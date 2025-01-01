package rrd2prom_test

import (
    "net/http"
    "net/http/httptest"
    "os"
    "testing"
    "time"

    "github.com/jessegalley/rrd2prom"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

type testFixture struct {
    rrdPath string
    httpServer *httptest.Server
}

func setupTestFixture(t *testing.T) *testFixture {
    t.Helper()
    
    fixture := &testFixture{
        rrdPath: "testdata/port1.rrd",
    }

    fixture.httpServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        data, err := os.ReadFile(fixture.rrdPath)
        require.NoError(t, err)
        w.Write(data)
    }))
    
    t.Cleanup(func() {
        fixture.httpServer.Close()
    })

    return fixture
}

func TestRRDFile(t *testing.T) {
    tests := []struct {
        name     string
        location string  // specify whether to use URL or file path
        rrdName  string  // test the name field
        fn       func(*testing.T, *testFixture, string, string) 
    }{
        {
            name:     "OpenLocalFile",
            location: "testdata/port1.rrd",  // file path
            rrdName:  "port1",
            fn:       testOpenRRD,
        },
        {
            name:     "OpenHttpFile",
            location: "",  // httpServer.URL in the test
            rrdName:  "port2",
            fn:       testOpenRRD,
        },
        {
            name:     "ParseLastUpdate",
            location: "testdata/port1.rrd",
            rrdName:  "port1",
            fn:       testParseLastUpdate,
        },
        {
            name:     "ParseStepInterval",
            location: "testdata/port1.rrd",
            rrdName:  "port1",
            fn:       testParseStepInterval,
        },
        {
            name:     "ParseDS",
            location: "testdata/port1.rrd",
            rrdName:  "port1",
            fn:       testParseDS,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            fixture := setupTestFixture(t)
            location := tt.location
            if tt.location == "" {
                location = fixture.httpServer.URL
            }
            tt.fn(t, fixture, location, tt.rrdName)
        })
    }
}

func testOpenRRD(t *testing.T, f *testFixture, location string, name string) {
    rrdFile, err := rrd2prom.NewRRDFile(location, name)
    require.NoError(t, err)
    assert.Equal(t, location, rrdFile.Location)
    assert.Equal(t, name, rrdFile.Name)
}

func testParseLastUpdate(t *testing.T, f *testFixture, location string, name string) {
    rrdFile, err := rrd2prom.NewRRDFile(location, name)
    require.NoError(t, err)
    
    expected := time.Unix(int64(1735589344), 0)
    assert.Equal(t, expected, rrdFile.LastUpdate)
}

func testParseStepInterval(t *testing.T, f *testFixture, location string, name string) {
    rrdFile, err := rrd2prom.NewRRDFile(location, name)
    require.NoError(t, err)
    
    expected := 60 * time.Second
    assert.Equal(t, expected, rrdFile.Interval)
}

func testParseDS(t *testing.T, f *testFixture, location string, name string) {
    rrdFile, err := rrd2prom.NewRRDFile(location, name)
    require.NoError(t, err)
    
    assert.NotEmpty(t, rrdFile.DataSources)
    assert.Contains(t, rrdFile.DataSources, "traffic_in")
    assert.Contains(t, rrdFile.DataSources, "traffic_out")
}

func TestRRDFile_Update(t *testing.T) {
    tests := []struct {
        name     string
        fn       func(*testing.T, *testFixture)
    }{
        {"UpdateSuccessful", testUpdateSuccessful},
        {"UpdateChangedValues", testUpdateChangedValues},
        {"UpdateWithBadLocation", testUpdateBadLocation},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            fixture := setupTestFixture(t)
            tt.fn(t, fixture)
        })
    }
}

// test normal successful update
func testUpdateSuccessful(t *testing.T, f *testFixture) {
    rrdFile, err := rrd2prom.NewRRDFile(f.rrdPath, "port1")
    require.NoError(t, err)
    
    // store initial values
    // initialUpdate := rrdFile.LastUpdate
    initialVals := make(map[string]uint64)
    for name, ds := range rrdFile.DataSources {
        initialVals[name] = ds.LastValue
    }
    
    // update and verify we can read again
    err = rrdFile.Update()
    require.NoError(t, err)
    
    // values might be the same since we're using test data,
    // but we can at least verify the update completed
    assert.NotZero(t, rrdFile.LastUpdate)
    assert.NotEmpty(t, rrdFile.DataSources)
}

// test update with modified values in the file
func testUpdateChangedValues(t *testing.T, f *testFixture) {
    // create a copy of test RRD with modified values
    modifiedRRD := "testdata/modified_port1.rrd"
    data, err := os.ReadFile(f.rrdPath)
    require.NoError(t, err)
    
    err = os.WriteFile(modifiedRRD, data, 0644)
    require.NoError(t, err)
    defer os.Remove(modifiedRRD)
    
    rrdFile, err := rrd2prom.NewRRDFile(modifiedRRD, "port1")
    require.NoError(t, err)
    
    // store initial values
    // initialUpdate := rrdFile.LastUpdate
    initialVals := make(map[string]uint64)
    for name, ds := range rrdFile.DataSources {
        initialVals[name] = ds.LastValue
    }
    
    // simulate changes in the RRD file
    // in real test we'd modify the RRD file here through the rrd library
    // for now we'll just verify the update method runs
    
    err = rrdFile.Update()
    require.NoError(t, err)
}

// test update with invalid location
func testUpdateBadLocation(t *testing.T, f *testFixture) {
    rrdFile, err := rrd2prom.NewRRDFile(f.rrdPath, "port1")
    require.NoError(t, err)
    
    // change location to invalid path
    rrdFile.Location = "nonexistent.rrd"
    
    err = rrdFile.Update()
    assert.Error(t, err)
}

func testUpdateHTTP(t *testing.T, f *testFixture) {
    rrdFile, err := rrd2prom.NewRRDFile(f.httpServer.URL, "port1")
    require.NoError(t, err)
    
    // store initial values
    // initialUpdate := rrdFile.LastUpdate
    initialVals := make(map[string]uint64)
    for name, ds := range rrdFile.DataSources {
        initialVals[name] = ds.LastValue
    }
    
    err = rrdFile.Update()
    require.NoError(t, err)
}

func testUpdateHTTPError(t *testing.T, f *testFixture) {
    // create a server that returns errors
    errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusInternalServerError)
    }))
    defer errorServer.Close()
    
    rrdFile, err := rrd2prom.NewRRDFile(errorServer.URL, "port1")
    require.Error(t, err) // should fail on initial creation
    
    if rrdFile != nil {
        err = rrdFile.Update()
        assert.Error(t, err) // should fail on update
    }
}
