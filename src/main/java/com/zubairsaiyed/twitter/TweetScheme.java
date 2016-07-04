package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.nio.ByteBuffer;
import twitter4j.*;
import org.apache.storm.spout.Scheme;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public final class TweetScheme implements Scheme {

    private static final Logger LOGGER = LoggerFactory.getLogger(TweetScheme.class);

    @Override
    public List<Object> deserialize(ByteBuffer buff) {
        try {
            Status status = TwitterObjectFactory.createStatus(StringScheme.deserializeString(buff));
            // ignore non-english tweets
            if(status.getLang().equalsIgnoreCase("en"))
                return new Values(status);
        } catch (Exception e) {
            // ignore non-parsable tweets
            LOGGER.debug("ERROR: Could not parse tweet!");
        }
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("tweet");
    }
}
