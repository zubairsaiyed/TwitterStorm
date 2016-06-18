package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.UUID;

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

public class LuwakSearchBolt extends BaseRichBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterFilterBolt.class);
  private OutputCollector collector;
  private Monitor monitor;
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map cfg, TopologyContext context, OutputCollector outCollector) {
    collector = outCollector;
    try {
    Monitor monitor = new Monitor(new LuceneQueryParser("text", new StandardAnalyzer()), new TermFilteredPresearcher());
    } catch (Exception e) { }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tweet"));
  }

  @Override
  public void execute(Tuple tuple) {
        String sourcename = tuple.getSourceComponent(); 

        if(sourcename.toLowerCase().contains("query")){
            MonitorQuery mq = new MonitorQuery("query"+UUID.randomUUID().toString(), "text:trump");
            try {
                    monitor.update(mq);
            } catch (Exception e) { }

        } else {
            Status status = (Status)tuple.getValue(0);
            InputDocument doc = InputDocument.builder("doc"+UUID.randomUUID().toString()).addField("text", status.getText(), new StandardAnalyzer()).build();
            try {
                    Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
                    for(DocumentMatches<QueryMatch> match : matches) {
                        System.out.println("Query: " + match.toString() + " matched document " + status.getText());
			collector.emit(tuple, new Values(status));
                    }
            } catch (Exception e) { }
        }
        
        collector.ack(tuple); 
  }
}
