package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.OffsetRequest;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SchemeAsMultiScheme; 
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.UUID;

public class TwitterTopology {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterTopology.class);

  public static void main(String[] args) throws Exception {
    KafkaSpout kafkaQuerySpout = buildKafkaQuerySpout();
    KafkaSpout kafkaTwitterSpout = buildKafkaTwitterSpout();
    FanBolt fanBolt1 = new FanBolt();
    FanBolt fanBolt2 = new FanBolt();
    FanBolt fanBolt3 = new FanBolt();
    LuwakSearchBolt luwakSearchBolt1 = new LuwakSearchBolt();
    LuwakSearchBolt luwakSearchBolt2 = new LuwakSearchBolt();
    LuwakSearchBolt luwakSearchBolt3 = new LuwakSearchBolt();
    RedisClientBolt redisClientBolt = new RedisClientBolt();
    VaderBolt vaderBolt = new VaderBolt();

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("kafka_query_spout", kafkaQuerySpout, 2);
    builder.setSpout("kafka_twitter_spout", kafkaTwitterSpout, 2);
    //builder.setBolt("luwak_search", luwakSearchBolt, 8).allGrouping("kafka_query_spout").customGrouping("kafka_twitter_spout", new MultiGrouping());
    builder.setBolt("fan1", fanBolt1, 1).shuffleGrouping("kafka_twitter_spout").allGrouping("kafka_query_spout");
    builder.setBolt("fan2", fanBolt2, 1).shuffleGrouping("kafka_twitter_spout").allGrouping("kafka_query_spout");
    builder.setBolt("fan3", fanBolt3, 1).shuffleGrouping("kafka_twitter_spout").allGrouping("kafka_query_spout");
    builder.setBolt("luwak_search1", luwakSearchBolt1, 2).customGrouping("fan1", new MultiGrouping());
    builder.setBolt("luwak_search2", luwakSearchBolt2, 2).customGrouping("fan2", new MultiGrouping());
    builder.setBolt("luwak_search3", luwakSearchBolt3, 2).customGrouping("fan3", new MultiGrouping());
    builder.setBolt("vader", vaderBolt, 6).shuffleGrouping("luwak_search1").shuffleGrouping("luwak_search2").shuffleGrouping("luwak_search3");
    builder.setBolt("redis_client", redisClientBolt, 3).fieldsGrouping("vader", new Fields("queryId"));

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    } else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }

  private static KafkaSpout buildKafkaQuerySpout() {
    String zkHostPort = "localhost:2181";   // a zookeeper node
    String topic = "query-topic";             // kafka topic

    String zkSpoutId = UUID.randomUUID().toString();
    BrokerHosts zkHosts = new ZkHosts(zkHostPort);
    
    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, "/"+topic, zkSpoutId);
    spoutCfg.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutCfg.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
    return kafkaSpout;
  }

  private static KafkaSpout buildKafkaTwitterSpout() {
    String zkHostPort = "localhost:2181";   // a zookeeper node
    String topic = "twitter-topic";             // kafka topic

    String zkSpoutId = UUID.randomUUID().toString();
    BrokerHosts zkHosts = new ZkHosts(zkHostPort);
    
    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, "/"+topic, zkSpoutId);
    spoutCfg.scheme = new SchemeAsMultiScheme(new TweetScheme());
    spoutCfg.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
    return kafkaSpout;
  }
}
