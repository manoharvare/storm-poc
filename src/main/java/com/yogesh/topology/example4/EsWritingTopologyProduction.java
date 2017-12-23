package com.yogesh.topology.example4;

import com.yogesh.example4.spout.EsRandomDataGeneratorSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.elasticsearch.storm.EsBolt;

public class EsWritingTopologyProduction {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        config.setDebug(true);
        config.put("es.nodes",args[1]);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new EsRandomDataGeneratorSpout(), 10);
        builder.setBolt("es-bolt", new EsBolt(args[2]), 10).shuffleGrouping("spout");
        StormSubmitter.submitTopology(args[0], config,builder.createTopology());
    }
}
