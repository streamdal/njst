# Our benchmark results

Benchmarks are _highly_ subjective - environment, hardware, configuration, time
of day, earth alignment and many other things can affect results.

This is what _we_ saw in our environment running in AWS (us-west-2).

## Our Benchmarks

* NATS cluster setup: 5 nodes; each w/ 16GB RAM, 4 cores @ 2.3Ghz
    * Config notes: _TODO_
* `njst` setup: 10 `njst` instances running in k8s; each w/ 128MB RAM, 2 cores @ 2.3Ghz

### Tiny

* 1 stream, 10,000 messages, no concurrency

#### Write Request

```json
{
	"description": "tiny",
	"nats": {
		"address": "127.0.0.1",
		"connection_per_stream": false
	},
	"write": {
		"num_nodes": 0,
		"num_streams": 1,
		"num_messages_per_stream": 10000,
		"num_workers_per_stream": 1,
		"msg_size_bytes": 1024,
		"keep_streams": true
	}
}
```

#### Write Results

```json
```

#### Read Request

```json
{
	"description": "per node testing",
	"nats": {
		"address": "127.0.0.1",
		"connection_per_stream": false
	},
	"read": {
		"write_id": "LYcKaCyy",
		"num_nodes": 2,
		"num_streams": 1,
		"num_messages_per_stream": 10000,
		"num_workers_per_stream": 1,
		"batch_size": 100
	}
}
```

#### Read Results

```json
```

--------------------------------------------------------------------------------

### Small

* 4 stream, 10,000 messages, 2 workers

#### Write Request

```json
{
	"description": "small",
	"nats": {
		"address": "127.0.0.1",
		"connection_per_stream": false
	},
	"write": {
		"num_nodes": 0,
		"num_streams": 4,
		"num_messages_per_stream": 10000,
		"num_workers_per_stream": 2,
		"msg_size_bytes": 1024,
		"keep_streams": true
	}
}
```

#### Write Results

```json
```

#### Read Request

```json
{
	"description": "small",
	"nats": {
		"address": "127.0.0.1",
		"connection_per_stream": false
	},
	"read": {
		"write_id": "VN39ouRR",
		"num_nodes": 0,
		"num_streams": 4,
		"num_messages_per_stream": 10000,
		"num_workers_per_stream": 2,
		"batch_size": 100
	}
}
```

#### Read Results

```json
```


--------------------------------------------------------------------------------

### Medium

* 25 stream, 100,000 messages, 2 workers

#### Write Request

```json
{
  "description": "medium",
  "nats": {
    "address": "127.0.0.1",
    "connection_per_stream": false
  },
  "write": {
    "num_nodes": 0,
    "num_streams": 25,
    "num_messages_per_stream": 100000,
    "num_workers_per_stream": 2,
    "msg_size_bytes": 1024,
    "keep_streams": true
  }
}
```

#### Write Results

```json
```

#### Read Request

```json
```

#### Read Results

```json
```
