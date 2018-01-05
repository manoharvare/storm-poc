package com.yogesh.topology.mongospoutesbolt;

import com.mongodb.BasicDBObject;
import com.yogesh.example5.spout.EsMongoDataSourceSpout;
import com.yogesh.mongospoutesbolt.bolt.MongoObjectCleanerBolt;
import com.yogesh.mongospoutesbolt.spout.MongoOpLogSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.elasticsearch.storm.EsBolt;

/**
 * @Author Yogesh Bombe
 */
public class OpsLogProcessingTopology {
    private static final String DATA_CLEANING_BOLT = "data-cleaning-bolt";
    private static final String MONGO_SPOUT = "mongo-spout";
    private static final String TOPOLOGY = "OpsLogProcessingTopology";
    private static final String ES_BOLT = "es-data-dump-bolt";
    private static final String[] FIELDS = {"sslId", "firstName", "middleName", "lastName", "gender", "dob", "mobileNumber", "city", "state", "country", "pinCode", "married", "institutionId", "product", "cibilScore", "processedBy", "applicationStatus", "income", "age"};
    private static final String SPOUT = "spout";
    private static final String MONGO_BOLT = "mongo-bolt";
    private static final String url = "mongodb://localhost:37017/debug";
    private static final String collectionName = "ssl";

    public static void main(String[] args) {
        BasicDBObject query = new BasicDBObject();
        query.put("ns", "debug.ssl");
        Config conf = new Config();
        conf.put("es.nodes", "localhost:9200");
        conf.setDebug(true);

        MongoMapper mapper = new SimpleMongoMapper()
                .withFields(FIELDS);

        MongoInsertBolt mongoBolt = new MongoInsertBolt(url, collectionName, mapper)
                .withOrdered(true)
                .withFlushIntervalSecs(10);

        // Build a topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT, new EsMongoDataSourceSpout(), 1);
        builder.setBolt(MONGO_BOLT, mongoBolt, 1).shuffleGrouping(SPOUT);
        builder.setSpout(MONGO_SPOUT, new MongoOpLogSpout("mongodb://localhost:37018", query, "debug.ssl"), 1);
        builder.setBolt(DATA_CLEANING_BOLT, new MongoObjectCleanerBolt(), 1).shuffleGrouping(MONGO_SPOUT);
        builder.setBolt(ES_BOLT, new EsBolt("debug/docs"), 1).shuffleGrouping(DATA_CLEANING_BOLT);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(TOPOLOGY, conf, builder.createTopology());

    }
}
