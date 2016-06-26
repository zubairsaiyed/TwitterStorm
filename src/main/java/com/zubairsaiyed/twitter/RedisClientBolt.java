package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;

import twitter4j.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class RedisClientBolt extends BaseRichBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisClientBolt.class);
  private OutputCollector collector;
  private static JedisPool pool;
  private HashMap<String, Long> map;
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map cfg, TopologyContext context, OutputCollector outCollector) {
    collector = outCollector;
    pool = new JedisPool(new JedisPoolConfig(), "ec2-52-1-176-95.compute-1.amazonaws.com");
    map = new HashMap<String, Long>();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //declarer.declare(new Fields("tweet"));
  }

  @Override
  public void execute(Tuple tuple) {
	String queryId = tuple.getStringByField("queryId");
	Double sentiment = (Double)tuple.getValueByField("sentiment");
	//Status status = (Status)tuple.getValueByField("tweet");
	try (Jedis jedis = pool.getResource()) {
		jedis.auth("zclusterredispassword");
		jedis.publish(queryId, Double.toString(sentiment)+ " " + tuple.getStringByField("tweetText"));
	}
        collector.ack(tuple); 
  }

  @Override
  public void cleanup() {
	pool.destroy();
  }
}
