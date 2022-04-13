# njst

`njst` is a [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) 
_distributed_ benchmark and testing tool.

NOTE: While benchmarks are dumb, this will at least give you a _general_ idea 
about the performance capabilities of your NATS cluster.

## Features

* Distributed by default
* No leader, no followers
* Cloud native - works best in k8s
* Simple HTTP REST'ish API for job control
* Ability to perform *massively parallel* tests to (attempt to) simulate real-world stress

## Usage

1. Modify and use the following [k8s deploy config](./deploy.dev.yaml) to deploy 3+ `nsjt` instance(s)
   to your kubernetes cluster.
    1. `kubectl apply -f deploy.dev.yaml`
2. Talk to any of the `njst` nodes via the [HTTP API](docs/api.md) to manage jobs

NOTE: To do anything useful with `njst`, you'll want to be able to talk to its API.
Since there's no service or ingres defined, you'll probably want to do a port-forward
to one of the pods via `kubectl`:
   * `kubectl port-forward njst-deployment-78d7b584cd-5sf9c 5000:5000`

## Why?

When evaluating a new technology such as a message bus/queue, in addition to
single node read/write performance, you'll probably want to also perform 
"real-world"-like tests to see how your cluster behaves.

NATS Jetstream is _amazing_ but aside from using `nats bench` to perform basic
read/write tests, we wanted to get insight into how our cluster will perform
under more realistic load (without updating all applications).

We created `njst` to do this. Maybe you'll find it useful too.

## You Should Know

`njst` is "dumb-distributed". Meaning, there are no concepts of leaders or
followers, there is no "election" and there is no consensus. 

In other words:

* When you submit a new job, the job will only be ran by the cluster members
that are currently connected to the cluster 
  * If new members join the cluster, they will have no idea about the pre-existing
  jobs.

* There is no auth - we have no need for it. If you want it, feel free to add it.

## Internal Flow

The general job flow can visualized via this confusing mermaid diagram:

```mermaid
graph TD;
    A(Create new test job)--1. HTTP-API-->B(njst cluster);
    B--2. Emit new job message-->NATS
    N1(njst nodes)--3. Get new job message -->NATS
    N1--4. Start job-->Results
    Results--5. Send results back-->NATS   
    NATS--6. Here are the results-->B
```
 
1. Client talks to any `njst` node via HTTP API to create a new test job
2. `njst` node emits a new job message to the cluster (via NATS)
3. All `njst` nodes get the new job message
4. All `njst` nodes start the job
5. All `njst` nodes send their results back via NATS KV
6. All `njst` nodes listen for result completions in result KV and analyze the result
7. Any `njst` node can now respond to a "status" HTTP call for a specific job

## Our Benchmarks

You can read our benchmark results [here](./docs/benchmarks.md).
