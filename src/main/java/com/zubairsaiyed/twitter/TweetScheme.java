package com.zubairsaiyed.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List; 
import java.nio.ByteBuffer;
 
import twitter4j.TwitterObjectFactory;
import org.apache.storm.spout.Scheme; 
import org.apache.storm.kafka.StringScheme; 
import org.apache.storm.tuple.Fields; 
import org.apache.storm.tuple.Values; 
 
public final class TweetScheme implements Scheme { 

	private static final Logger LOGGER = LoggerFactory.getLogger(TweetScheme.class);

	@Override 
	public List<Object> deserialize(ByteBuffer buff) { 
		try { 
			return new Values(TwitterObjectFactory.createStatus(StringScheme.deserializeString(buff))); 
		} catch (Exception e) { 
			// ignore non-parsable tweets 
			LOGGER.debug("ERROR: Could not parse tweet!");
			return null; 
		} 
	} 

	@Override 
	public Fields getOutputFields() { 
		return new Fields("tweet"); 
	} 

}
