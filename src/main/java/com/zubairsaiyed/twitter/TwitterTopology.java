package com.zubairsaiyed.twitter;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
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

public class TwitterTopology {

  private static final String SENTENCE_SPOUT_ID = "kafka-sentence-spout";
  private static final String SPLIT_BOLT_ID = "acking-split-bolt";
  private static final String COUNT_BOLT_ID = "acking-count-bolt";
  private static final String REPORT_BOLT_ID = "acking-report-bolt";

  public static void main(String[] args) throws Exception {

    KafkaSpout kspout = buildKafkaSentenceSpout();
    SplitSentenceBolt splitBolt = new SplitSentenceBolt();
    WordCountBolt countBolt = new WordCountBolt();
    ReportBolt reportBolt = new ReportBolt();

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SENTENCE_SPOUT_ID, kspout, 1);
    builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
    builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
    builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }

  private static KafkaSpout buildKafkaSentenceSpout() {
    String zkHostPort = "52.200.190.83:2181";   // a zookeeper node
    String topic = "twitter-topic";             // kafka topic

    String zkRoot = "/acking-kafka-sentence-spout";
    String zkSpoutId = "acking-sentence-spout";
    ZkHosts zkHosts = new ZkHosts(zkHostPort);
    
    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
    return kafkaSpout;
  }
}
