package com.yogesh.mongospoutesbolt.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.util.Map;

public class MongoObjectCleanerBolt implements IRichBolt {
  private Map map;
  private TopologyContext topologyContext;
  private OutputCollector outputCollector;
  private static String[] FIELDS = {"sslId", "firstName", "middleName", "lastName", "gender", "dob", "mobileNumber", "city", "state", "country", "pinCode", "married", "institutionId", "product", "cibilScore", "processedBy", "applicationStatus", "income", "age"};


  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.map = map;
    this.topologyContext = topologyContext;
    this.outputCollector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    String _id = tuple.getString(0);
    String mongoObject = tuple.getString(1);
    Document document = Document.parse(mongoObject);
    this.outputCollector.emit(new Values(
      document.getString("sslId"),
      document.getString("firstName"),
      document.getString("middleName"),
      document.getString("lastName"),
      document.getString("gender"),
      document.getString("dob"),
      document.getString("mobileNumber"),
      document.getString("city"),
      document.getString("state"),
      document.getString("country"),
      document.getString("pinCode"),
      document.getString("married"),
      document.getString("institutionId"),
      document.getString("product"),
      document.getInteger("cibilScore"),
      document.getString("processedBy"),
      document.getString("applicationStatus"),
      document.getInteger("income"),
      document.getInteger("age")));
  }

  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(FIELDS));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
