package com.zubairsaiyed.twitter;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {

  private static final long serialVersionUID = 3092938699134129356L;
  
  private OutputCollector collector;
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map cfg, TopologyContext topologyCtx, OutputCollector outCollector) {
    collector = outCollector;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  @Override
  public void execute(Tuple tuple) {
    Object value = tuple.getValue(0);
    String sentence = null;
    if (value instanceof String) {
      sentence = (String) value;

    } else {
      // Kafka returns bytes
      byte[] bytes = (byte[]) value;
      try {
        sentence = new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }      
    }

    String[] words = sentence.split("\\s+");
    for (String word : words) {
      collector.emit(tuple, new Values(word));
    }
    collector.ack(tuple);
  }
}
