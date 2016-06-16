package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.OffsetRequest;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
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
    KafkaSpout kspout = buildKafkaSentenceSpout();
    TwitterFilterBolt twitterFilterBolt = new TwitterFilterBolt();

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("kafka_spout", kspout, 2);
    builder.setBolt("twitter_filter", twitterFilterBolt, 8).shuffleGrouping("kafka_spout");

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

  private static KafkaSpout buildKafkaSentenceSpout() {
    String zkHostPort = "ec2-52-200-190-83.compute-1.amazonaws.com:2181";   // a zookeeper node
    String topic = "twitter-topic";             // kafka topic

    String zkSpoutId = UUID.randomUUID().toString();
    BrokerHosts zkHosts = new ZkHosts(zkHostPort);
    
    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, "/"+topic, zkSpoutId);
    spoutCfg.scheme = new SchemeAsMultiScheme(new TweetScheme());
    spoutCfg.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();

    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
    return kafkaSpout;
  }
}
