package com.josedeveloper.topologia;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class RabbitMQTopologyExample {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RabbitMQSpout());
        builder.setBolt("bolt", new MessageBolt())
                .shuffleGrouping("spout");

        Config conf = new Config();
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(200000);
        cluster.killTopology("test");
        cluster.shutdown();
	}

}
