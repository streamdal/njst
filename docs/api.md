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
      "description": "read test",
      "read": {
        "write_id": "MoyKvzIL",
        "num_nodes": 2,
        "num_streams": 1,
        "num_messages_per_stream": 1000000,
        "num_workers_per_stream": 2,
        "batch_size": 1000
      },
            "nats" : {
          "address":"localhost:4222",
          "shared_connection": false
      }
    }
```
* **Sample WRITE request**:
```json
{
      "description": "heavy write test",
      "write": {
        "num_nodes": 2,
        "num_streams": 1,
        "num_messages_per_stream": 1000000,
        "num_workers_per_stream": 2,
        "msg_size_bytes": 128,
        "keep_streams": true,
        "batch_size": 1000
      },
      "nats" : {
          "address":"localhost:4222",
          "shared_connection": false
      }
}
```

## GET /bench/:id
* **Description**: Get stats for a specific job
* **Request**: None
* **Response type**: `application/json`
* **Sample response**:
```json

    "status": {
        "status": "completed",
        "message": "benchmark completed; final",
        "job_id": "OPjBPOwW",
        "elapsed_seconds": 3.04,
        "avg_msg_per_sec_per_node": 82943.22,
        "total_msg_per_sec_all_nodes": 165886.45,
        "total_processed": 500000,
        "total_errors": 0,
        "started_at": "2022-05-03T16:59:13.498444-07:00",
        "ended_at": "2022-05-03T16:59:16.546326-07:00"
    },
    "settings": {
        "description": "read test",
        "nats": {
            "address": "localhost:4222",
            "shared_connection": false
        },
        "read": {
            "write_id": "MoyKvzIL",
            "num_streams": 1,
            "num_nodes": 2,
            "nodes": null,
            "num_messages_per_stream": 1000000,
            "num_workers_per_stream": 2,
            "batch_size": 1000,
            "strategy": ""
        },
        "id": "OPjBPOwW"
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
