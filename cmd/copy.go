package cmd

import (
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"github.com/obukhov/go-redis-migrate/src/pusher"
	"github.com/obukhov/go-redis-migrate/src/reporter"
	"github.com/obukhov/go-redis-migrate/src/scanner"
	"github.com/spf13/cobra"
	"log"
	"sync"
	"time"
)

var pattern string
var scanCount, report, exportRoutines, pushRoutines int

var copyCmd = &cobra.Command{
	Use:   "copy [sourceHost:port] [targetHost:port]",
	Short: "Copy keys from source redis instance to destination by given pattern",
	Long:  ``,
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Start copying")

		clientSource, err := radix.DefaultClientFunc("tcp", args[0])
		if err != nil {
			log.Fatal(err)
		}

		clientTarget, err := radix.DefaultClientFunc("tcp", args[1])
		if err != nil {
			log.Fatal(err)
		}

		statusReporter := reporter.NewReporter()

		redisScanner := scanner.CreateScanner(
			clientSource,
			scanner.RedisScannerOpts{
				Pattern:          pattern,
				ScanCount:        scanCount,
				PullRoutineCount: exportRoutines,
			},
			statusReporter,
		)

		redisPusher := pusher.NewRedisPusher(clientTarget, redisScanner.GetDumpChannel(), statusReporter)

		waitingGroup := new(sync.WaitGroup)

		statusReporter.Start(time.Second * time.Duration(report))
		redisPusher.Start(waitingGroup, pushRoutines)
		redisScanner.Start(waitingGroup)

		waitingGroup.Wait()
		statusReporter.Stop()
		statusReporter.Report()

		fmt.Println("Finish copying")
	},
}

func init() {
	rootCmd.AddCommand(copyCmd)

	copyCmd.Flags().StringVar(&pattern, "pattern", "*", "Match pattern for keys")
	copyCmd.Flags().IntVar(&scanCount, "scanCount", 100, "COUNT parameter for redis SCAN command")
	copyCmd.Flags().IntVar(&report, "report", 1, "Report current status every N seconds")
	copyCmd.Flags().IntVar(&exportRoutines, "exportRoutines", 30, "Number of parallel export goroutines")
	copyCmd.Flags().IntVar(&pushRoutines, "pushRoutines", 30, "Number of parallel push goroutines")
}
