package com.yogesh.example2.bolt;

import com.yogesh.example2.model.CustomerInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ObjectCreatorBolt implements IRichBolt {

    private OutputCollector outputCollector;
    private CustomerInfo customerInfo;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        customerInfo = CustomerInfo.builder()
                .firstName(tuple.getString(0))
                .middleName(tuple.getString(1))
                .lastName(tuple.getString(2))
                .gender(tuple.getString(3))
                .dob(tuple.getString(4))
                .mobileNumber(tuple.getString(5))
                .city(tuple.getString(6))
                .state(tuple.getString(7))
                .country(tuple.getString(8))
                .pinCode(tuple.getString(9))
                .build();
        outputCollector.emit(new Values(customerInfo));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("customerInfo"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
