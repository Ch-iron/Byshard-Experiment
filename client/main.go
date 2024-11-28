package main

import (
	"paperexperiment"
	"paperexperiment/client/benchmark"
	"paperexperiment/client/client"
	"paperexperiment/client/db"
	"paperexperiment/types"
)

// Database implements bamboo.DB interface for benchmarking
type Database struct {
	client.Client
}

func (d *Database) Init() error {
	return nil
}

func (d *Database) Stop() error {
	return nil
}

func (d *Database) Write(k int, v []byte, shards []types.Shard) error {
	key := db.Key(k)
	err := d.Put(key, v, shards)
	return err
}

func main() {
	paperexperiment.Init()

	d := new(Database)
	d.Client = client.NewHTTPClient()
	b := benchmark.NewBenchmark(d)
	if b == nil {
		return
	}
	b.Run()
}
