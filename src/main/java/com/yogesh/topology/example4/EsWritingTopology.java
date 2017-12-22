package com.yogesh.topology.example4;

import com.yogesh.example4.spout.EsRandomDataGeneratorSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.elasticsearch.storm.EsBolt;

public class EsWritingTopology {
    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.setDebug(true);
        config.put("es.nodes","localhost:9200");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new EsRandomDataGeneratorSpout(), 10);
        builder.setBolt("es-bolt", new EsBolt("storm/docs"), 1).shuffleGrouping("spout");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("EsDataDumpTopology", config,
                builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
