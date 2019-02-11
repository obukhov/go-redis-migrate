# go-redis-migrate

Script to copy data by keys pattern from one redis instance to another.

### Usage

```bash
go-redis-migrate copy [sourceHost:port] [targetHost:port] --pattern="prefix:*"
```

*Pattern* - can be glob-style pattern supported by [Redis SCAN](https://redis.io/commands/scan) command.

Other flags:
```bash
  --report int           Report current status every N seconds (default 1)
  --scanCount int        COUNT parameter for redis SCAN command (default 100)
  --exportRoutines int   Number of parallel export goroutines (default 30)
  --pushRoutines int     Number of parallel push goroutines (default 30)
```

### General idea

There are 3 main stages of copying keys:
1. Scanning keys in source
2. Dumping values and TTLs from source
3. Restoring values and TTLs in destination

Scanning is performed with a single goroutine, scanned keys are sent to keys channel (type is chan string). From keys 
channel N export goroutines are consuming keys and perform `DUMP` and `PTTL` for them as a pipeline command. Results
are combined in KeyDump structure and transferred to channel of this type. Another M push goroutines are consuming from 
the channel and perform `RESTORE` command on the destination instance.

To guarantee that all keys are exported and restored `sync.WaitingGroup` is used. To monitor current status there is a
separate goroutine that outputs value of atomic counters (for each stage) every K seconds. 

### Performance tests

Performed on a laptop with redis instances, running in docker.

#### Test #1
Source database: 453967 keys.
Keys to copy:     10000 keys.

|    | Version 1.0 (no concurrency) | Version 2.0 (read-write concurrency) |
|----|------------------------------|--------------------------------------|
| #1 |                       17.79s |                                4.82s |
| #2 |                       18.01s |                                5.88s |
| #3 |                       17.98s |                                5.06s |

#### Test #2
Source database: 453967 keys.
Keys to copy:    367610 keys.

|    | Version 1 (no concurrency) | Version 2.0 (read-write concurrency) |
|----|----------------------------|--------------------------------------|
| #1 |                   8m57.98s |                               58.78s |
| #2 |                   8m44.98s |                               55.35s |
| #3 |                   8m58.07s |                               57.25s |
