package cmd

import (
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"log"
	"time"

	"github.com/spf13/cobra"
)

// copyCmd represents the copy command
var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy keys from source redis instance to destination by given pattern",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		start := time.Now()

		fmt.Println("Start copying")

		clientSource, err := radix.DefaultClientFunc("tcp", "localhost:63791")

		if err != nil {
			log.Fatal(err)
		}

		clientTarget, err := radix.DefaultClientFunc("tcp", "localhost:63792")
		if err != nil {
			log.Fatal(err)
		}

		scanner := radix.NewScanner(clientSource, radix.ScanOpts{
			Command: "SCAN",
			Pattern: "hello:*",
			Count:   100,
		})

		var key string
		counter := 0
		cycle := 0
		cycleStart := time.Now()
		for scanner.Next(&key) {

			var value string
			var ttl int

			p := radix.Pipeline(
				radix.Cmd(&ttl, "PTTL", key),
				radix.Cmd(&value, "DUMP", key),
			)

			if err := clientSource.Do(p); err != nil {
				panic(err)
			}

			if ttl < 0 {
				ttl = 0
			}

			err = clientTarget.Do(radix.FlatCmd(nil, "RESTORE", key, ttl, value, "REPLACE"))
			if err != nil {
				log.Fatal(err)
			}
			counter++
			cycle++

			if cycle == 1000 {
				log.Printf("Copied another 1000 in: %s", time.Since(cycleStart))
				cycle = 0
				cycleStart = time.Now()
			}
		}

		if err := scanner.Close(); err != nil {
			log.Fatal(err)
		}

		log.Printf("In total %d keys copied in %s", counter, time.Since(start))
	},
}

func init() {
	rootCmd.AddCommand(copyCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// copyCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// copyCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
