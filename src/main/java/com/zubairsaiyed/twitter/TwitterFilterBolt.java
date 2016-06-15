package com.zubairsaiyed.twitter;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

public class TwitterFilterBolt extends BaseRichBolt {

  private static final long serialVersionUID = 3092938699134129356L;
  
  private static final ObjectMapper mapper = new ObjectMapper();

  private static final Logger LOGGER = Logger.getLogger(TwitterFilterBolt.class);
 
  private OutputCollector collector;
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map cfg, TopologyContext topologyCtx, OutputCollector outCollector) {
    collector = outCollector;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tweet_id", "tweet_text"));
  }

  @Override
  public void execute(Tuple input) {
        LOGGER.debug("filttering incoming tweets");
        String json = input.getString(0);
        try
        {
            JsonNode root = mapper.readValue(json, JsonNode.class);
            long id;
            String text;
            if (root.get("lang") != null &&
                "en".equals(root.get("lang").textValue()))
            {
                if (root.get("id") != null && root.get("text") != null)
                {
                    id = root.get("id").longValue();
                    text = root.get("text").textValue();
                    collector.emit(new Values(id, text));
                    System.out.println("TWEET PROCESSED: " + id + " " + text);
                }
                else
                    LOGGER.debug("tweet id and/ or text was null");
            }
            else
                LOGGER.debug("Ignoring non-english tweet");
        }
        catch (IOException ex)
        {
            LOGGER.error("IO error while filtering tweets", ex);
            LOGGER.trace(null, ex);
        }
  }

  public Map<String, Object> getComponentConfiguration() { return null; }

}
