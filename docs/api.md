# API spec

`njst` is controlled via a RESTish HTTP API.

* [GET /cluster](#get--cluster)
* [POST /bench](#post--bench)
* [GET /bench/:id](#get--bench--id)
* [DELETE /bench/:id](#delete--bench--id)
* [GET /version](#get--version)
* [GET /health-check](#get--health-check)

## GET /cluster

* **Description**: Get cluster info (such as nodes)
* **OK Response**: `200`
* **Error Response**: `!200`
* **Response type**: `application/json`
* **Sample response**

```json
{
  "nodes": [
    "uuid1",
    "uuid2",
    ".."
  ],
  "count": 2
}
```

---

## POST /bench
* **Description**: Create either a read or write benchmark job
  * To create a read benchmark, you should first populate streams with data by creating a write job
* **Request type**: `application/json`
* **Response type**: `application/json`
* **Sample response**:
```json
    {
      "id": "NFG9zrdi",
      "message": "benchmark created successfully"
    }
```
* **Sample READ request**:
    ```json
    {
      "description": "medium read test",
      "nats": {                           
        "address": "127.0.0.1",           // Defines which NATS cluster the benchmark will be ran on
        "connection_per_stream": false    // Whether njst will create a new NATS connection per stream
      },
      "read": {
        "write_id": "9iN0iDE8",
        "num_nodes": 1,                   // How many njst nodes should the benchmark run on; 0 == all
        "num_streams": 4,                 // How many streams to create
        "num_messages_per_stream": 10000, // Approximately how many messages to read per stream
        "num_workers_per_stream": 1,      // How many dedicated workers njst will use per stream
        "batch_size": 100                 // How many messages to try to read at a time
      }
    }
    ```
* **Sample WRITE request**:
    ```json
    {
      "description": "heavy write test",
      "nats": {
        "address": "127.0.0.1",
        "connection_per_stream": false
      },
      "write": {
        "num_nodes": 1,
        "num_streams": 10,
        "num_messages_per_stream": 100000,
        "num_workers_per_stream": 4,
        "msg_size_bytes": 1024,
        "keep_streams": true              // Whether to keep the streams after the benchmark is done; by default, streams are deleted
      }
    }
    ```

## GET /bench/:id
* **Description**: Get stats for a specific job
* **Request**: None
* **Response type**: `application/json`
* **Sample response**:
```json
{
    "status": {
      "status": "in-progress",
      "message": "benchmark is in progress; ticker",
      "job_id": "7HTnAX19",
      "node_id": "57ac570a",
      "elapsed_seconds": 22,
      "avg_msg_per_sec_per_node": 18180.28,
      "avg_msg_per_sec_all_nodes": 18180.28,
      "total_processed": 100000,
      "total_errors": 0,
      "started_at": "2022-04-27T21:32:10.661846-07:00",
      "ended_at": "2022-04-27T21:32:32.562739-07:00"
    },
	"settings": {
		"description": "heavy write test",
        "nats": {
          "address": "127.0.0.1",
          "connection_per_stream": false
        },
		"write": {
			"num_streams": 10,
			"num_nodes": 1,
			"num_messages_per_stream": 100000,
			"num_workers_per_stream": 4,
			"num_replicas": 0,
			"msg_size_bytes": 1024,
			"keep_streams": true
		},
		"id": "NFG9zrdi"
	}
}
```

## DELETE /bench/:id
* **Description**: Stop specified job + delete results and settings from NATS
* **Request**: None
* **OK Response**:
* **Response type**: `application/json`
* **Sample response**:
```json
{
  "message": "benchmark deleted successfully"
}
```


## GET /version

* **Description**: Get version info for the current njst node
* **OK Response**: `200`
* **Error Response**: `!200`
* **Response type**: `application/json`
* **Sample response**

```json
{
  "version": "0.0.1"
}
```

## GET /health-check

* **Description**: Get health check info
* **OK Response**: `200`
* **Error Response**: `!200`
* **Response type**: `text/plain`
* **Sample response**

```text
OK
```
