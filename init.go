package paperexperiment

import (
	"flag"
	"net/http"

	"paperexperiment/config"
	"paperexperiment/log"
)

func Init() {
	flag.Parse()
	log.Setup()
	config.Configuration.Load()
	// setting.SetContractAddress()
	// for i := 1; i <= config.Configuration.ShardCount; i++ {
	// 	setting.SetLevelDBState(types.Shard(i))
	// }
	// fmt.Println("Finish Create StateDB")
	// time.Sleep(10 * time.Second)
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000
}
