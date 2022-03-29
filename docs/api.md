# API spec

* [GET /cluster](#get--cluster)
* [GET /bench](#get--bench)
* [GET /bench/:id](#get--bench--id)
* [POST /bench](#post--bench)
* [DELETE /bench/:id](#delete--bench--id)
* [GET /health-check](#get--health-check)
* [GET /version](#get--version)

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

## GET /bench

* **Description**: Get all in-memory benchmarks
* **OK Response**: `200`
* **Error Response**: `!200`
* **Response type**: `application/json`
* **Sample response**

```json

```

---

## GET /bench/:id

* **Description**: Get info about a specific benchmark
* **OK Response**: `200`
* **Error Response**: `!200`
* **Response type**: `application/json`
* **Sample response**

```json

```

---

## POST /bench

* **Description**: Create & start a new benchmark
* **OK Response**: `200`
* **Error Response**: `!200`
* **Response type**: `application/json`
* **Sample response**

```json

```

---

## DELETE /bench/:id

* **Description**: Stop and delete a benchmark by id
* **OK Response**: `200`
* **Error Response**: `!200`
* **Response type**: `application/json`
* **Sample response**

```json

```

---

## GET /health-check

* **Description**: Get health check info
* **OK Response**: `200`
* **Error Response**: `!200`
* **Response type**: `text/plain`
* **Sample response**

```text
OK
```

---

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
