package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
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

    private final int ROLLING_COUNT = 5;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisClientBolt.class);
    private OutputCollector collector;
    private static JedisPool pool;
    private HashMap<String, Double> averages;
    private HashMap<String, LinkedList<Double> > queues;
    private Properties prop;

    @Override @SuppressWarnings("rawtypes")
    public void prepare(Map cfg, TopologyContext context, OutputCollector outCollector) {
        collector = outCollector;
        prop = new Properties();

        // load configuration parameters
        try (InputStream input = RedisClientBolt.class.getClassLoader().getResourceAsStream("config.properties")) {
            prop.load(input);
        } catch (IOException ex) {
            logger.error("Unable to read configuration file! Make sure configuration parameters are defined.");
            ex.printStackTrace();
        }
        pool = new JedisPool(new JedisPoolConfig(), prop.getProperty("redis_host"));
        averages = new HashMap<String, Double>();
        queues = new HashMap<String, LinkedList<Double> >();
    }

    // no output fields since result is pushed directly to Redis
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple tuple) {
        String queryId = tuple.getStringByField("queryId");
        Double sentiment = (Double)tuple.getValueByField("sentiment");

        // ignore tweets with sentiment rating of 0 (VADER was unable to process)
        if (sentiment != 0) {
            if (!averages.containsKey(queryId) || !queues.containsKey(queryId)) {
                // if query is new, create new LL to track rolling average
                LinkedList<Double> q = new LinkedList<Double>();
                q.add(sentiment);
                averages.put(queryId, sentiment);
                queues.put(queryId, q);
            } else {
                // otherwise update rolling average
                LinkedList<Double> q = queues.get(queryId);
                Double avg = averages.get(queryId);
                int size = 0;
                if (q != null) size = q.size();
                if (q.size() >= ROLLING_COUNT) {
                    averages.put(queryId, (avg*size-q.peek()+sentiment)/size);
                    q.poll();
                    q.add(sentiment);
                } else {
                    averages.put(queryId, (avg*size+sentiment)/(size+1));
                    q.add(sentiment);
                }
            }

            // publish newly calculated rolling average to query channel
            try (Jedis jedis = pool.getResource()) {
                jedis.auth(prop.getProperty("redis_password"));
                jedis.publish(queryId, Double.toString(averages.get(queryId))+ " " + tuple.getStringByField("tweetText"));
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        pool.destroy();
    }
}
