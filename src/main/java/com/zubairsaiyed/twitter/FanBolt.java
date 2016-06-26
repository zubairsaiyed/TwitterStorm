package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;

import twitter4j.*;

import org.apache.lucene.analysis.standard.StandardAnalyzer;

import uk.co.flax.luwak.DocumentMatches;
import uk.co.flax.luwak.InputDocument;
import uk.co.flax.luwak.Matches;
import uk.co.flax.luwak.Monitor;
import uk.co.flax.luwak.MonitorQuery;
import uk.co.flax.luwak.QueryMatch;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FanBolt extends BaseRichBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(FanBolt.class);
  private OutputCollector collector;
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map cfg, TopologyContext context, OutputCollector outCollector) {
    collector = outCollector;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "value"));
  }

  @Override
  public void execute(Tuple tuple) {
        String sourcename = tuple.getSourceComponent(); 
	String tupleType;

        if(sourcename.toLowerCase().contains("query")) {
		tupleType = "query";
        } else {
		tupleType = "tweet";
        }
        
	collector.emit(tuple, new Values(tupleType, tuple.getValue(0)));
        collector.ack(tuple); 
  }
}
