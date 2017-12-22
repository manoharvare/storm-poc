package com.yogesh.topology.example5;

import com.yogesh.example5.spout.EsMongoDataSourceSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.elasticsearch.storm.EsBolt;

/**
 * @Author Yogesh Bombe
 * @ToDo Add Following in program argument
 * args[0]-TOPOLOGY-NAME
 * args[1]-MONGO-URL-WITH-DB-NAME
 * agrs[2]=MONGO-COLLECTION-NAME
 * agrs[3]=ES-NODES
 * agrs[4]=ES_INDEX_TYPE
 * <p>
 * EsMongoDataDumpTopology mongodb://localhost:27017/storm ssl localhost:9200 storm/docs
 * storm jar /home/hduser/app/ES-MONGO-STORM-TOPOLOGY.jar com.yogesh.topology.example5.MixTopologyProduction EsMongoDataDumpTopology mongodb://localhost:27017/storm ssl localhost:9200 storming/docs
 */
public class MixTopologyProduction {
    private static String SPOUT = "spout";
    private static String ES_BOLT = "es-bolt";
    private static String MONGO_BOLT = "mongo-bolt";
    private static String TOPOLOGY = "EsMongoDataDumpTopology";
    private static String URL = "mongodb://localhost:27017/storm";
    private static String[] FIELDS = {"sslId", "firstName", "middleName", "lastName", "gender", "dob", "mobileNumber", "city", "state", "country", "pinCode", "married", "institutionId", "product", "cibilScore", "processedBy", "applicationStatus", "income", "age"};
    private static String ES_NODE = "localhost:9200";
    private static String ES_INDEX_TYPE = "storm/docs";
    private static String COLLECTION_NAME = "ssl";

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        MongoMapper mapper = new SimpleMongoMapper()
                .withFields(FIELDS);

        MongoInsertBolt mongoBolt = new MongoInsertBolt(args[1], args[2], mapper)
                .withOrdered(true)
                .withFlushIntervalSecs(10);

        Config config = new Config();
        config.setDebug(true);
        config.setDebug(true);
        config.put("es.nodes", args[3]);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT, new EsMongoDataSourceSpout(), 1);
        builder.setBolt(MONGO_BOLT, mongoBolt, 1).shuffleGrouping(SPOUT);
        builder.setBolt(ES_BOLT, new EsBolt(args[4]), 1).shuffleGrouping(SPOUT);
        StormSubmitter.submitTopology(args[0], config, builder.createTopology());

    }
}
