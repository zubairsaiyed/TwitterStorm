package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import twitter4j.*;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TwitterFilterBolt extends BaseRichBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterFilterBolt.class);
  private OutputCollector collector;
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map cfg, TopologyContext context, OutputCollector outCollector) {
    collector = outCollector;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tweet_id", "tweet_text"));
  }

  @Override
  public void execute(Tuple tuple) {
        Status status = (Status)tuple.getValue(0); 
        if(status.getLang().toLowerCase()=="en") { 
        	LOGGER.info("User " + status.getUser().getScreenName() + " tweeted " + status.getText()); 
        	collector.emit(tuple, new Values(status.getId(), status.getUser().getScreenName()));
        } 
        collector.ack(tuple); 
  }
}
