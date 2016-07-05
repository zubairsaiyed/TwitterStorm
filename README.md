# tweet-storm

tweet-storm is a sub-module of the [TwtrTrkr project](https://github.com/zubairsaiyed/TwtrTrkr). It is a Storm topology primarily designed to overcome the single node memory limitation of Lucene Luwak. The topology effectively distributes the core abstraction of Luwak, the inverted query index, across multiple nodes using Storm's shuffle stream grouping. Incoming tweets are then broadcast across all the partitions of the inverted query index to ensure no matches are missed.

The tweets corresponding to active system queries are then further processed by the NLTK VADER python library by leveraging Storm's multilang support. Tweet sentiment values are then grouped by query so an aggregate value can be determined.

Finally the aggregates values are published to the Redis PubSub framework and picked up by the web-server backend.

## Requirements :

* Apache Kafka v0.8.1.1
* Apache Zookeeper
* Apache Storm v1.0.1
* NLTK VADER
* Jedis (Redis Java client)
* Apache Maven
* Oracle JDK 1.7 (64 bit)

## Design

The storm topology necessary to shuffle user queries and broadcast tweets before generating aggregate sentiment is as follows.

![Storm topology](https://github.com/zubairsaiyed/TwtrTrkr/blob/master/images/topology.png)

## Running

tweet-storm must first be configured with Apache Kafka server details and Redis connection details via a configuration file at `resources\config.properties`.

The storm project can then be built using the `mvn assembly:assembly` command. Finally the jar can be loaded into Storm as follows:

```
storm jar target/TwitterStorm-1.0-SNAPSHOT-jar-with-dependencies.jar com.zubairsaiyed.twitter.TwitterTopology tweet-storm-topology remote
```
