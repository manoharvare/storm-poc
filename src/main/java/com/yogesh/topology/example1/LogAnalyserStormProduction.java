package com.yogesh.topology.example1;

import com.yogesh.example1.bolt.CallLogCounterBolt;
import com.yogesh.example1.bolt.CallLogCreatorBolt;
import com.yogesh.example1.spout.FakeCallLogReaderSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class LogAnalyserStormProduction {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());
        builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt())
                .shuffleGrouping("call-log-reader-spout");
        builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt())
                .fieldsGrouping("call-log-creator-bolt", new Fields("call"));
        StormSubmitter.submitTopology("LogAnalyserStorm", config,
                builder.createTopology());
    }
}
