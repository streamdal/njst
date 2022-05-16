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
* **Notes**:
  * `num_nodes`: Number of nodes that will participate in the benchmark; 0 == all nodes
  * `shared_connection`: in `nats` section will cause workers to share the NATS connection
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
* **Params**
  * `full`: Will include stats with node reports (default: false)
* **Response type**: `application/json`
* **Sample response**:
```json
{
  "status": {
    "status": "completed",
    "message": "benchmark completed; final",
    "job_id": "gmZwIhh1",
    "elapsed_seconds": 5.76,
    "avg_msg_per_sec_per_node": 21592.29,
    "total_msg_per_sec_all_nodes": 215922.93,
    "total_processed": 1000000,
    "total_errors": 0,
    "started_at": "2022-05-16T05:28:18.811787061Z",
    "ended_at": "2022-05-16T05:28:24.573479216Z",
    "node_reports": [
      {
        "streams": [
          {
            "workers": [
              {
                "WorkerID": "f4d10574-njst-gmZwIhh1-0-0",
                "processed": 100000,
                "errors": 0,
                "elapsed_seconds": 2.83,
                "avg_msg_per_sec": 35332.09
              }
            ]
          }
        ]
      },
      {
        "streams": [
          {
            "workers": [
              {
                "WorkerID": "cdafa59b-njst-gmZwIhh1-0-0",
                "processed": 100000,
                "errors": 0,
                "elapsed_seconds": 3.86,
                "avg_msg_per_sec": 25887.27
              }
            ]
          }
        ]
      },
      {
        "streams": [
          {
            "workers": [
              {
                "WorkerID": "807851af-njst-gmZwIhh1-0-0",
                "processed": 100000,
                "errors": 0,
                "elapsed_seconds": 3.99,
                "avg_msg_per_sec": 25013.37
              }
            ]
          }
        ]
      },
      {
        "streams": [
          {
            "workers": [
              {
                "WorkerID": "24ab64f6-njst-gmZwIhh1-0-0",
                "processed": 100000,
                "errors": 0,
                "elapsed_seconds": 4.81,
                "avg_msg_per_sec": 20771.16
              }
            ]
          }
        ]
      },
      {
        "streams": [
          {
            "workers": [
              {
                "WorkerID": "873a9264-njst-gmZwIhh1-0-0",
                "processed": 100000,
                "errors": 0,
                "elapsed_seconds": 5.26,
                "avg_msg_per_sec": 18990.88
              }
            ]
          }
        ]
      },
      {
        "streams": [
          {
            "workers": [
              {
                "WorkerID": "705fbd21-njst-gmZwIhh1-0-0",
                "processed": 100000,
                "errors": 0,
                "elapsed_seconds": 5.31,
                "avg_msg_per_sec": 18806.01
              }
            ]
          }
        ]
      },
      {
        "streams": [
          {
            "workers": [
              {
                "WorkerID": "a345ebd7-njst-gmZwIhh1-0-0",
                "processed": 100000,
                "errors": 0,
                "elapsed_seconds": 5.48,
                "avg_msg_per_sec": 18221.15
              }
            ]
          }
        ]
      },
      {
        "streams": [
          {
            "workers": [
              {
                "WorkerID": "6466020b-njst-gmZwIhh1-0-0",
                "processed": 100000,
                "errors": 0,
                "elapsed_seconds": 5.52,
                "avg_msg_per_sec": 18089.01
              }
            ]
          }
        ]
      },
      {
        "streams": [
          {
            "workers": [
              {
                "WorkerID": "34a47370-njst-gmZwIhh1-0-0",
                "processed": 100000,
                "errors": 0,
                "elapsed_seconds": 5.73,
                "avg_msg_per_sec": 17447.69
              }
            ]
          }
        ]
      },
      {
        "streams": [
          {
            "workers": [
              {
                "WorkerID": "fb6b30c8-njst-gmZwIhh1-0-0",
                "processed": 100000,
                "errors": 0,
                "elapsed_seconds": 5.75,
                "avg_msg_per_sec": 17364.3
              }
            ]
          }
        ]
      }
    ]
  },
  "settings": {
    "description": "tiny",
    "nats": {
      "address": "dev-nats.default.svc.cluster.local",
      "shared_connection": false
    },
    "write": {
      "num_streams": 1,
      "num_nodes": 10,
      "num_messages_per_stream": 1000000,
      "num_workers_per_stream": 1,
      "num_replicas": 0,
      "batch_size": 100,
      "msg_size_bytes": 24,
      "keep_streams": true
    },
    "id": "gmZwIhh1"
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
