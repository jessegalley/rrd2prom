package rrd2prom

import (
	"context"
	"sync"
	"time"
)

// Metric represents a single data point from an RRD file
type Metric struct {
	Name      string
	Value     uint64
	Source    string
	Timestamp time.Time
}

// RRDManager handles multiple RRD files and their metric collection
type RRDManager struct {
	Files   []*RRDFile
	Metrics chan Metric
	Msgs    chan string
	Errors  chan error

	done   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRRDManager creates a new manager instance with the provided RRD files
func NewRRDManager(files []*RRDFile) (*RRDManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &RRDManager{
		Files:   files,
		Metrics: make(chan Metric, 1000),
		Msgs:    make(chan string, 1000),
		Errors:  make(chan error, 1000),
		done:    make(chan struct{}),
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Run starts the manager and all RRD file handlers
func (m *RRDManager) Run() error {
	// first send an initial message that we're starting
	m.Msgs <- "RRDManager starting up..."

	// start a handler for each RRD file
	for _, file := range m.Files {
		m.startHandler(file)
	}

	// wait for done signal
	<-m.done
	
	// cancel context for all handlers
	m.cancel()
	
	// wait for all handlers to complete
	m.wg.Wait()
	
	// send final message before closing channels
	m.Msgs <- "RRDManager shutting down..."
	
	// close channels safely
	m.closeChannels()

	return nil
}

// Stop signals the manager to stop all handlers and clean up
func (m *RRDManager) Stop() {
	close(m.done)
}

// startHandler creates and runs a goroutine to handle a single RRD file
func (m *RRDManager) startHandler(rrdFile *RRDFile) {
	m.wg.Add(1)
	
	go func() {
		defer m.wg.Done()

		// send initial message for this handler
		m.Msgs <- "Starting handler for " + rrdFile.Name
		
		ticker := time.NewTicker(rrdFile.Interval)
		defer ticker.Stop()

		// do an initial update immediately
		if err := rrdFile.Update(); err != nil {
			m.Errors <- err
		} else {
			now := time.Now()
			for dsName, ds := range rrdFile.DataSources {
				m.Metrics <- Metric{
					Name:      rrdFile.Name,
					Value:     ds.LastValue,
					Source:    dsName,
					Timestamp: now,
				}
			}
		}

		for {
			select {
			case <-m.ctx.Done():
				m.Msgs <- "Stopping handler for " + rrdFile.Name
				return
				
			case <-ticker.C:
				// update RRD file data
				if err := rrdFile.Update(); err != nil {
					m.Errors <- err
					continue
				}

				m.Msgs <- "Updated " + rrdFile.Name + " RRD file"

				// create and send metrics for each data source
				now := time.Now()
				for dsName, ds := range rrdFile.DataSources {
					metric := Metric{
						Name:      rrdFile.Name,
						Value:     ds.LastValue,
						Source:    dsName,
						Timestamp: now,
					}
					
					select {
					case m.Metrics <- metric:
					case <-m.ctx.Done():
						return
					}
				}
			}
		}
	}()
}

// closeChannels safely closes all channels used by the manager
func (m *RRDManager) closeChannels() {
	close(m.Metrics)
	close(m.Msgs)
	close(m.Errors)
}
