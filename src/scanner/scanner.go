package scanner

import (
	"github.com/mediocregopher/radix/v3"
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
	keyChannel  chan string
	dumpChannel chan KeyDump
}

func CreateScanner(client radix.Client, options RedisScannerOpts) *redisScanner {
	return &redisScanner{
		client:      client,
		options:     options,
		dumpChannel: make(chan KeyDump),
		keyChannel:  make(chan string),
	}
}

func (r *redisScanner) Start(wg *sync.WaitGroup) {
	wg.Add(1)

	wgPull := new(sync.WaitGroup)
	wgPull.Add(r.options.PullRoutineCount)

	go r.scanRoutine(wg)
	for i := 0; i < r.options.PullRoutineCount; i++ {
		go r.pullRoutine(wgPull)
	}

	wgPull.Wait()
	close(r.dumpChannel)
}

func (r *redisScanner) GetDumpChannel() chan KeyDump {
	return r.dumpChannel
}

func (r *redisScanner) scanRoutine(wg *sync.WaitGroup) {
	var key string
	scanOpts := radix.ScanOpts{
		Command: "SCAN",
		Count:   r.options.ScanCount,
	}

	if r.options.Pattern != "*" {
		scanOpts.Pattern = r.options.Pattern
	}

	radixScanner := radix.NewScanner(r.client, scanOpts)
	for radixScanner.Next(&key) {
		r.keyChannel <- key
	}

	close(r.keyChannel)
	wg.Done()
}

func (r *redisScanner) pullRoutine(wg *sync.WaitGroup) {
	for {
		key, more := <-r.keyChannel

		if more {
			var value string
			var ttl int

			p := radix.Pipeline(
				radix.Cmd(&ttl, "PTTL", key),
				radix.Cmd(&value, "DUMP", key),
			)

			if err := r.client.Do(p); err != nil {
				log.Fatal(err)
			}

			if ttl < 0 {
				ttl = 0
			}

			r.dumpChannel <- KeyDump{
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
