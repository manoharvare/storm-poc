package com.yogesh.topology.example3;

import com.yogesh.example3.spout.RandomDataGeneratorSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

public class HbaseDataDumpTopology {
    public static void main(String[] args) throws InterruptedException {

        Config config = new Config();
        config.setDebug(true);
        Map<String, Object> hbConf = new HashMap<>();
        if (args.length > 0) {
            hbConf.put("hbase.rootdir", args[0]);
        }
        config.put("hbase.conf", hbConf);
        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("uuid")
                .withColumnFields(new Fields("firstName", "middleName", "lastName", "gender", "dob", "mobileNumber", "city", "state", "country", "pinCode", "married"))
                .withColumnFamily("cf");

        HBaseBolt hbase = new HBaseBolt("randomData", mapper);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("random-data-generator", new RandomDataGeneratorSpout(), 1);
        builder.setBolt("hbase-save", hbase, 1).fieldsGrouping("random-data-generator", new Fields("uuid"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("HbaseDataDumpTopology", config,
                builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
