package scanner

import (
	"github.com/mediocregopher/radix/v3"
	"github.com/obukhov/go-redis-migrate/src/reporter"
	"log"
	"sync"
)

type KeyDump struct {
	Key   string
	Value string
	Ttl   int
}

type RedisScannerOpts struct {
	Pattern          string
	ScanCount        int
	PullRoutineCount int
}

type redisScanner struct {
	client      radix.Client
	options     RedisScannerOpts
	reporter    *reporter.Reporter
	keyChannel  chan string
	dumpChannel chan KeyDump
}

func CreateScanner(client radix.Client, options RedisScannerOpts, reporter *reporter.Reporter) *redisScanner {
	return &redisScanner{
		client:      client,
		options:     options,
		reporter:    reporter,
		dumpChannel: make(chan KeyDump),
		keyChannel:  make(chan string),
	}
}

func (s *redisScanner) Start(wg *sync.WaitGroup) {
	wg.Add(1)

	wgPull := new(sync.WaitGroup)
	wgPull.Add(s.options.PullRoutineCount)

	go s.scanRoutine(wg)
	for i := 0; i < s.options.PullRoutineCount; i++ {
		go s.exportRoutine(wgPull)
	}

	wgPull.Wait()
	close(s.dumpChannel)
}

func (s *redisScanner) GetDumpChannel() chan KeyDump {
	return s.dumpChannel
}

func (s *redisScanner) scanRoutine(wg *sync.WaitGroup) {
	var key string
	scanOpts := radix.ScanOpts{
		Command: "SCAN",
		Count:   s.options.ScanCount,
	}

	if s.options.Pattern != "*" {
		scanOpts.Pattern = s.options.Pattern
	}

	radixScanner := radix.NewScanner(s.client, scanOpts)
	for radixScanner.Next(&key) {
		s.reporter.AddScannedCounter(1)
		s.keyChannel <- key
	}

	close(s.keyChannel)
	wg.Done()
}

func (s *redisScanner) exportRoutine(wg *sync.WaitGroup) {
	for {
		key, more := <-s.keyChannel

		if more {
			var value string
			var ttl int

			p := radix.Pipeline(
				radix.Cmd(&ttl, "PTTL", key),
				radix.Cmd(&value, "DUMP", key),
			)

			if err := s.client.Do(p); err != nil {
				log.Fatal(err)
			}

			if ttl < 0 {
				ttl = 0
			}

			s.reporter.AddExportedCounter(1)
			s.dumpChannel <- KeyDump{
				Key:   key,
				Ttl:   ttl,
				Value: value,
			}
		} else {
			break
		}
	}

	wg.Done()
}
