# Our benchmark results

Benchmarks are _highly_ subjective - environment, hardware, configuration, time
of day, earth alignment and many other things can affect results.

This is what _we_ saw in our environment running in AWS (us-west-2).

## Our Benchmarks

* NATS cluster setup: 5 nodes; each w/ 16GB RAM, 4 cores @ 2.3Ghz
    * Config notes: _TODO_
* `njst` setup: 10 `njst` instances running in k8s; each w/ 128MB RAM, 2 cores @ 2.3Ghz

### Small

**1 stream, 10,000 messages, no concurrency**

#### Write Results

#### Read Results

--------------------------------------------------------------------------------
### Medium

**4 stream, 10,000 messages, 2 workers**

#### Write Results

#### Read Results

--------------------------------------------------------------------------------

### Heavy

**25 stream, 100,000 messages, 2 workers**

#### Write results
#### Read results
