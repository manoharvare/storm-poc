package com.yogesh.example2.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import static com.yogesh.util.StormUtil.*;

import java.util.Map;

public class RandomDataGeneratorSpout implements IRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private TopologyContext topologyContext;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.topologyContext = topologyContext;
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        while (true){
            this.spoutOutputCollector.emit(new Values(generateFirstName(), generateMiddleName(),generateLastName(),generateGender(),generateDob(),generateMobileNumber(),generateCity(),generateState(),generateCountry(),generatePinCode(),generateIsMarried()));
        }

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("firstName", "middleName", "lastName","gender","dob","mobileNumber","city","state","country","pinCode","married"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
