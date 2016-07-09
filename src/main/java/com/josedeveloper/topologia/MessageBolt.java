package com.josedeveloper.topologia;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class MessageBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	
	@SuppressWarnings("unused")
	private OutputCollector collector;
	
	@Override
	public void execute(Tuple tuple) {
		String message = tuple.getString(0);
		System.out.println("--> " + message);
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
