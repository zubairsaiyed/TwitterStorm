package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
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

public class LuwakSearchBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuwakSearchBolt.class);
    private OutputCollector collector;
    private Monitor monitor;

    @Override @SuppressWarnings("rawtypes")
    public void prepare(Map cfg, TopologyContext context, OutputCollector outCollector) {
        collector = outCollector;
        // initialize queries
        try {
            monitor = new Monitor(new LuceneQueryParser("text", new StandardAnalyzer()), new TermFilteredPresearcher());
        } catch (Exception e) {
            logger.error("Unable to initialize Luwak Monitor!");
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // output tweet text and list of matching queryIds
        declarer.declare(new Fields("tweetText", "matches"));
    }

    @Override
    public void execute(Tuple tuple) {
        String type = tuple.getStringByField("type");
        MonitorQuery mq;

        if(type.toLowerCase().equals("query")) {
            // query messages are used to update Luwak query index
            String query = (String)tuple.getValueByField("value");
            if (query.startsWith("delete:::",0)) {
                // process query deletion request
                try {
                    monitor.deleteById(query.substring(9));
                } catch (Exception e) {
                    logger.error("Unable to delete following query from QueryMonitor: " + query.substring(9));
                    e.printStackTrace();
                }
            } else {
                // process new query request
                mq = new MonitorQuery(Integer.toString(query.toLowerCase().hashCode()), "text:"+query);
                try {
                    monitor.update(mq);
                } catch (Exception e) {
                    logger.error("Unable to add following query to QueryMonitor: " + query);
                    e.printStackTrace();
                }
            }
        } else {
            // tweet messages are passed through QueryMatcher to determine matching queries
            Status status = (Status)tuple.getValueByField("value");
            InputDocument doc = InputDocument.builder(Long.toString(status.getId())).addField("text", status.getText(), new StandardAnalyzer()).build();
            try {
                Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
                for(DocumentMatches<QueryMatch> match : matches) {
                    ArrayList<String> list = new ArrayList<String>();
                    for (QueryMatch mat : match) {
                        list.add(mat.getQueryId());
                    }
                    if (list.size() > 0)
                        // emit tweet text and list of matching queries to be analyzed by VADER
                        collector.emit(tuple, new Values(status.getText(), list));
                }
            } catch (Exception e) {
                logger.error("Unable to process following tweet through QueryMonitor: " + status.getId());
                e.printStackTrace();
            }
        }

        collector.ack(tuple);
    }
}
