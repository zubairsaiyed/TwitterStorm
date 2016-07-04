# twitter-storm

twitter-storm is a Storm topology designed to overcome the single node limitation of Lucene Luwak. The Storm topology effectively distributes the core abstraction of Luwak, the inverted query index, across multiple nodes using Storm's shuffle stream grouping. Incoming tweets are then broadcast across all the partitions of the inverted query index to ensure no matches are missed.

The tweets corresponding to active system queries are then further processed by the NLTK VADER python library by leveraging Storm's multilang support. Tweet sentiment values are then grouped by query so an aggregate value can be determined.

Finally the aggregates values are published to the Redis PubSub framework and picked up by the web-server backend.

## Requirements :

Apache Kafka 1.0.1
Apache Zookeeper ( required for Kafka)
Apache Storm
NLTK Vader
Apache Maven
Oracle JDK 1.7 (64 bit )

# Design

The storm topology necessary to shuffle user queries and broadcast tweets before generating aggregate sentiment is as follows.

![Storm topology](https://github.com/zubairsaiyed/TwtrTrkr/blob/master/images/topology.png)
