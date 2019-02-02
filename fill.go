package main // import "github.com/obukhov/go-redis-migrate"

import (
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"math/rand"
	"strconv"
	"time"
)

func main() {
	fmt.Println("Filling redis with random data")

	pool, err := radix.NewPool("tcp", "localhost:63791", 10)
	if err != nil {
		fmt.Println(err)
		return
	}

	rand.Seed(time.Now().UTC().UnixNano())

	// map of random prefixed keys - key is a pefix, value is number of keys to generate in one iteration
	randomMap := map[string]int{
		"hello:":  100,
		"world:":  1,
		"foobar:": 20,
	}

	for j := 0; j < 1000; j++ {
		for prefix, number := range randomMap {
			for i := 0; i < number; i++ {
				randVal := strconv.Itoa(rand.Int())
				err = pool.Do(radix.Cmd(nil, "SET", prefix+randVal, randVal))
				if err != nil {
					fmt.Println(err)
				}
			}
		}

		fmt.Printf("Iteration %d done\n", j)
	}
}
