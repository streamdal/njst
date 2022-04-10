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
      "read": {
        "write_id": "9iN0iDE8",
        "num_nodes": 1,
        "num_streams": 4,
        "num_messages_per_stream": 10000,
        "num_workers_per_stream": 1,
        "batch_size": 1000
      }
    }
    ```
* **Sample WRITE request**:
    ```json
    {
      "description": "heavy write test",
      "write": {
        "num_nodes": 1,
        "num_streams": 10,
        "num_messages_per_stream": 100000,
        "num_workers_per_stream": 4,
        "msg_size_bytes": 1024,
        "keep_streams": true
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
		"message": "benchmark is in progress",
		"job_id": "NFG9zrdi",
		"node_id": "cf31e536",
		"elapsed_seconds": 45,
		"avg_msg_per_sec": 894,
		"total_processed": 40231,
		"total_errors": 0,
		"started_at": "2022-04-02T23:02:51.909912-07:00",
		"ended_at": "0001-01-01T00:00:00Z"
	},
	"settings": {
		"description": "heavy write test",
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
