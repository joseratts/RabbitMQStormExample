package com.josedeveloper.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class SendMessagesToRabbit {
    
    public static final String message = "MENSAJE DE PRUEBA";
    public static final int NUM_MENSAJES = 10000;
    
    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {	
		sendMessage();
    }
    
    public static void sendMessage() throws IOException, InterruptedException, TimeoutException {
    	ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    factory.setPort(5672);
	    factory.setUsername("guest"); //usuario por defecto de rabbitmq
	    factory.setPassword("guest"); //password por defecto de rabbitmq
	    factory.setVirtualHost("/");

	    Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		for (int i = 0; i < NUM_MENSAJES; i++) {
			final String msg = message + i;
			channel.basicPublish("", "data", null, msg.getBytes());
		}
			

		channel.close();
	    connection.close();
    }
 
}
