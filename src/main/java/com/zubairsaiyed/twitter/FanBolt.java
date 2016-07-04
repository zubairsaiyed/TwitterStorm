package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
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
    private int count;

    @Override @SuppressWarnings("rawtypes")
    public void prepare(Map cfg, TopologyContext context, OutputCollector outCollector) {
        collector = outCollector;
        count = 0;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // declare output streams for each luwak grouping and another for all queries
        declarer.declareStream("query_stream", new Fields("type", "value"));
        declarer.declareStream("tweet_stream1", new Fields("type", "value"));
        declarer.declareStream("tweet_stream2", new Fields("type", "value"));
        declarer.declareStream("tweet_stream3", new Fields("type", "value"));
    }

    @Override
    public void execute(Tuple tuple) {
        String sourcename = tuple.getSourceComponent();
        String tupleType, stream;

        if(sourcename.toLowerCase().contains("query")) {
            // emit all query messages to query stream
            tupleType = "query";
            stream = "query_stream";
        } else {
            // distribute tweet messages round robin across tweet streams
            tupleType = "tweet";
            stream = "tweet_stream" + (count++%3 + 1);
        }

        collector.emit(stream, tuple, new Values(tupleType, tuple.getValue(0)));
        collector.ack(tuple);
    }
}
