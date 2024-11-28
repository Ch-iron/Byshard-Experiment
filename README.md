## Install
```
$ git clone https://github.com/Ch-iron/Paper-Experiment
$ cd Paper-Experiment
```
## Build
1. Client Build
```
$ cd bin
$ go build ../client
```

2. Node Build
```
$ cd bin
$ go build ../server
```

## Execute
### Locally Execute
Create a statedb directory inside the common directory and create as many directories inside it as there are shards.  
ex ) common/statedb/1  
Uncomment the for loop in init.go.
Execute the following command and wait for statedb to be created.
```
$ cd bin
$ ./solo_deploy.sh
```
Once all of the statedb is created, terminate the process with the following command and re-comment the for loop we uncommented earlier.
```
$ ./stop.sh
```
Divide the generated statedb by the number of nodes.  
ex ) common/statedb/1/1  
Execute the following command.
```
$ ./solo_deploy.sh
```
If you want to stop, execute the following command.
```
$ ./stop.sh
```

## Configuration
You can change the settings by modifying the common/config.json file.

shard_count: Number of total shards  
committee_number: Number of consensus committees in the shard  
benchmark/N: The number of total transactions to run  
benchmark/Throttle: Number of transactions per second to occur  

If you change the number of shards, you will also need to modify bin/deploy/config.conf.
In config.conf file, you can set the number of nodes per shard.