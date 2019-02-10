package cmd

import (
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"github.com/obukhov/go-redis-migrate/src/pusher"
	"github.com/obukhov/go-redis-migrate/src/scanner"
	"github.com/spf13/cobra"
	"log"
	"sync"
)

var pattern string
var scanCount, report, limit int

type keyDump struct {
	key   string
	value string
	ttl   int
}

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

		redisScanner := scanner.CreateScanner(clientSource, scanner.RedisScannerOpts{
			Pattern:          pattern,
			ScanCount:        100,
			PullRoutineCount: 10,
		})

		redisPusher := pusher.NewRedisPusher(clientTarget, redisScanner.GetDumpChannel())

		waitingGroup := new(sync.WaitGroup)

		redisPusher.Start(waitingGroup, 10)
		redisScanner.Start(waitingGroup)

		waitingGroup.Wait()

		fmt.Println("Finish copying")
	},
}

func init() {
	rootCmd.AddCommand(copyCmd)

	copyCmd.Flags().StringVar(&pattern, "pattern", "*", "Match pattern for keys")
	copyCmd.Flags().IntVar(&scanCount, "scanCount", 100, "COUNT parameter for redis SCAN command")
	copyCmd.Flags().IntVar(&report, "report", 1000, "After what number of keys copied to report time")
	copyCmd.Flags().IntVar(&limit, "limit", 0, "After what number of keys copied to stop (0 - unlimited)")
}
