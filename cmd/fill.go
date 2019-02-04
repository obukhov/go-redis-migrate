package cmd

import (
	"errors"
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

var prefix, count []string
var cycles int

var fillCmd = &cobra.Command{
	Use:   "fill [host:port]",
	Short: "Create random keys in redis instance",
	Args:  cobra.MinimumNArgs(1),
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Filling redis with random data")

		randomMap, err := createRandomMap(prefix, count)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Random map: ", randomMap)

		pool, err := radix.NewPool("tcp", args[0], 10)
		if err != nil {
			log.Fatal(err)
		}

		rand.Seed(time.Now().UTC().UnixNano())

		for j := 0; j < cycles; j++ {
			for prefix, number := range randomMap {
				for i := 0; i < number; i++ {
					randVal := strconv.Itoa(rand.Int())
					err = pool.Do(radix.Cmd(nil, "SET", prefix+randVal, randVal))
					if err != nil {
						fmt.Println(err)
					}
				}
			}

			fmt.Printf("Cycle %d done\n", j)
		}
	},
}

func createRandomMap(prefix []string, count []string) (map[string]int, error) {
	randomMap := make(map[string]int)
	for key, val := range prefix {
		if key < len(count) {
			countForPrefix, err := strconv.Atoi(count[key])
			if err != nil {
				return nil, err
			}

			if countForPrefix <= 0 {
				return nil, errors.New("count cannot be zero or negative")
			}

			randomMap[val] = countForPrefix
		} else {
			randomMap[val] = 1
		}
	}

	return randomMap, nil
}

func init() {
	rootCmd.AddCommand(fillCmd)

	fillCmd.Flags().StringArrayVar(&prefix, "prefix", []string{"foobar:"}, "Prefixes to fill")
	fillCmd.Flags().StringArrayVar(&count, "count", []string{"1"}, "Count of keys to create for prefix in one cycle")
	fillCmd.Flags().IntVar(&cycles, "cycles", 1, "Cycles count to perform")

}
