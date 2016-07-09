package com.josedeveloper.topologia;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitMQSpout extends BaseRichSpout {

	private static final long serialVersionUID = -5875062340173997062L;
	
	private SpoutOutputCollector collector;
	BlockingQueue<String> messages;
	
	private final static String QUEUE_NAME = "data";
	
	@Override
	public void nextTuple() {
		String message;
        while ((message = messages.poll()) != null) {
        	collector.emit(new Values(message)); //emitimos el mensaje dentro de la topologia
        }
		
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		messages = new ArrayBlockingQueue<String>(100);
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    Connection connection;
		
	    try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();
			
			channel.queueDeclare(QUEUE_NAME, true, false, false, null);
			
			Consumer consumer = new DefaultConsumer(channel) {
			      @Override
			      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			          throws IOException {
			        String message = new String(body, "UTF-8");
			        try {
						messages.put(message);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			      }
			    };
		    channel.basicConsume(QUEUE_NAME, true, consumer);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e1) {
			e1.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields( "message" ) ); //declaramos los campos que enviaremos a la topologia
	}

}
