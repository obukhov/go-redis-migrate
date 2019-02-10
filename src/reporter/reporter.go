package reporter

import (
	"log"
	"sync/atomic"
	"time"
)

func NewReporter() *Reporter {
	return &Reporter{
		doneChannel: make(chan bool),
	}
}

type Reporter struct {
	scannedCount  uint64
	exportedCount uint64
	pushedCount   uint64

	start       time.Time
	doneChannel chan bool
}

func (r *Reporter) Start(reportPeriod time.Duration) {
	atomic.StoreUint64(&r.scannedCount, 0)
	atomic.StoreUint64(&r.exportedCount, 0)
	atomic.StoreUint64(&r.pushedCount, 0)

	r.start = time.Now()

	go r.reportingRoutine(reportPeriod)
}

func (r *Reporter) Stop() {
	r.doneChannel <- true
}

func (r *Reporter) AddScannedCounter(delta uint64) {
	atomic.AddUint64(&r.scannedCount, delta)
}
func (r *Reporter) AddExportedCounter(delta uint64) {
	atomic.AddUint64(&r.exportedCount, delta)
}
func (r *Reporter) AddPushedCounter(delta uint64) {
	atomic.AddUint64(&r.pushedCount, delta)
}

func (r *Reporter) Report() {
	log.Printf(
		"Scanned: %d Exported: %d Pushed: %d after %s\n",
		atomic.LoadUint64(&r.scannedCount),
		atomic.LoadUint64(&r.exportedCount),
		atomic.LoadUint64(&r.pushedCount),
		time.Since(r.start),
	)
}

func (r *Reporter) reportingRoutine(reportPeriod time.Duration) {
	timer := time.NewTicker(reportPeriod)
	for {
		select {
		case <-timer.C:
			r.Report()
		case <-r.doneChannel:
			timer.Stop()
			break
		}
	}
}
