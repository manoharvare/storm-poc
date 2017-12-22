package com.yogesh.topology.example2;

import com.yogesh.example2.bolt.ObjectCreatorBolt;
import com.yogesh.example2.spout.RandomDataGeneratorSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.topology.TopologyBuilder;

public class DataGeneratorStormTopology {
    public static void main(String[] args) throws InterruptedException {
        String url = "mongodb://localhost:27017/"+args[0];
        String collectionName = args[1];
        Config config = new Config();
        config.setDebug(true);

        MongoMapper mapper = new SimpleMongoMapper()
                .withFields("firstName", "middleName", "lastName", "gender", "dob", "mobileNumber", "city", "state", "country", "pinCode");

        MongoInsertBolt mongoBolt = new MongoInsertBolt(url, collectionName, mapper)
                .withOrdered(true)
                .withFlushIntervalSecs(10);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("random-data-generator", new RandomDataGeneratorSpout(),1);
        builder.setBolt("mongo-save-bolt", mongoBolt,1).shuffleGrouping("random-data-generator");
        builder.setBolt("object-creator-bolt", new ObjectCreatorBolt())
                .shuffleGrouping("random-data-generator");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("DataGeneratorTopology", config,
                builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
