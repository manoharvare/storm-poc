package com.yogesh.topology.mongospoutesbolt;

import com.mongodb.BasicDBObject;
import com.yogesh.example5.spout.EsMongoDataSourceSpout;
import com.yogesh.mongospoutesbolt.bolt.MongoObjectCleanerBolt;
import com.yogesh.mongospoutesbolt.spout.MongoOpLogSpout;
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
 * @ToDo
 * storm jar REAL-TIME-PROCESSING-POC-TOPOLOGY
 * @classpath com.yogesh.topology.mongospoutesbolt.OpsLogProcessingTopologyProduction
 * @program-agruments OpsLogRealTimeProcessingTopology mongodb://localhost:27017/storm ssl mongodb://localhost:27017 storm.ssl localhost:9200 realtimeprocessing/docs 1 true
 */
public class OpsLogProcessingTopologyProduction {
    private static String TOPOLOGY = "OpsLogRealTimeProcessingTopology";
    private static String SPOUT = "spout";
    private static String MONGO_BOLT = "mongo-bolt";
    private static String DATA_CLEANING_BOLT = "data-cleaning-bolt";
    private static String MONGO_SPOUT = "mongo-spout";
    private static String ES_BOLT = "es-data-dump-bolt";
    private static String[] FIELDS = {"sslId", "firstName", "middleName", "lastName", "gender", "dob", "mobileNumber", "city", "state", "country", "pinCode", "married", "institutionId", "product", "cibilScore", "processedBy", "applicationStatus", "income", "age"};
    private static String MONGO_URL_FOR_WRITE = "mongodb://localhost:27017/storm";
    private static String COLLECTION_NAME = "ssl";
    private static String OP_LOG_URL = "mongodb://localhost:27017";
    private static String OP_LOG_FILTER_NAMESPACE = "storm.ssl";
    private static String ES_NODES = "localhost:9200";
    private static String ES_INDEX_AND_TYPE = "realtimeprocessing/docs";
    private static int PARALLELISM_HINT = 1;
    private static Boolean DEBUG_MODE = true;

    private static void initializeInputParameter(String[] args) {
        TOPOLOGY = args[0];//Name of Topology
        MONGO_URL_FOR_WRITE = args[1];//url for mongo connection use for write data into the mongodb.
        COLLECTION_NAME = args[2];//Collection Name for Mongo writing into the mongo
        OP_LOG_URL = args[3];
        OP_LOG_FILTER_NAMESPACE = args[4];
        ES_NODES = args[5];
        ES_INDEX_AND_TYPE = args[6];
        PARALLELISM_HINT = Integer.parseInt(args[7].trim());
        DEBUG_MODE = Boolean.parseBoolean(args[8].trim());
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        if (args.length > 0) {
            initializeInputParameter(args);
        }

        MongoMapper mapper = new SimpleMongoMapper()
                .withFields(FIELDS);

        MongoInsertBolt mongoBolt = new MongoInsertBolt(MONGO_URL_FOR_WRITE, COLLECTION_NAME, mapper)
                .withOrdered(true)
                .withFlushIntervalSecs(10);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SPOUT, new EsMongoDataSourceSpout(), PARALLELISM_HINT);
        builder.setBolt(MONGO_BOLT, mongoBolt, PARALLELISM_HINT).shuffleGrouping(SPOUT);
        builder.setSpout(MONGO_SPOUT, new MongoOpLogSpout(OP_LOG_URL, new BasicDBObject(), OP_LOG_FILTER_NAMESPACE), PARALLELISM_HINT);
        builder.setBolt(DATA_CLEANING_BOLT, new MongoObjectCleanerBolt(), PARALLELISM_HINT).shuffleGrouping(MONGO_SPOUT);
        builder.setBolt(ES_BOLT, new EsBolt(ES_INDEX_AND_TYPE), PARALLELISM_HINT).shuffleGrouping(DATA_CLEANING_BOLT);
        Config conf = new Config();
        conf.put("es.nodes", ES_NODES);
        conf.setDebug(DEBUG_MODE);
        StormSubmitter.submitTopology(TOPOLOGY, conf, builder.createTopology());
    }
}
