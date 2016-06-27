package com.zubairsaiyed.twitter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;

public class MultiGrouping implements CustomStreamGrouping, Serializable {
    private Random random;
    private ArrayList<List<Integer>> choices;
    private AtomicInteger current;
    private List<Integer> allTasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();
        choices = new ArrayList<List<Integer>>(targetTasks.size());
	allTasks = new ArrayList();
        for (Integer i: targetTasks) {
            choices.add(Arrays.asList(i));
            allTasks.add(i);
        }
        Collections.shuffle(choices, random);
        current = new AtomicInteger(0);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
	String type = values.get(0).toString();
	if (type.equals("query")) {
		int rightNow;
		int size = choices.size();
		while (true) {
		    rightNow = current.incrementAndGet();
		    if (rightNow < size) {
			return choices.get(rightNow);
		    } else if (rightNow == size) {
			current.set(0);
			//This should be thread safe so long as ArrayList does not have any internal state that can be messed up by multi-treaded access.
			Collections.shuffle(choices, random);
			return choices.get(0);
		    }
		    //race condition with another thread, and we lost
		    // try again
		}
	} else {
		return allTasks;
	}
    }
}
