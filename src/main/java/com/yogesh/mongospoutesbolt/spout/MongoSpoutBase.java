package com.yogesh.mongospoutesbolt.spout;


import com.mongodb.BasicDBObject;
import com.yogesh.mongospoutesbolt.spout.async.MongoSpoutTask;
import com.yogesh.mongospoutesbolt.spout.declaration.MongoObjectGrabber;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class MongoSpoutBase extends BaseRichSpout {
    static Logger LOG = Logger.getLogger(MongoSpoutBase.class);
    protected MongoObjectGrabber mapper;
    protected Map conf;
    protected TopologyContext context;
    protected SpoutOutputCollector collector;
    protected LinkedBlockingQueue<BasicDBObject> queue = new LinkedBlockingQueue<BasicDBObject>(1);
    private String dbName;
    private BasicDBObject query;
    private String url;
    private MongoSpoutTask spoutTask;
    private String[] collectionNames;
    private String filterByNamespace;

    public MongoSpoutBase(String url, String dbName, String[] collectionNames, BasicDBObject query, MongoObjectGrabber mapper) {
        this.url = url;
        this.dbName = dbName;
        this.collectionNames = collectionNames;
        this.query = query;
    }

    public MongoSpoutBase(String url, String dbName, String[] collectionNames, BasicDBObject query, String filterByNamespace, MongoObjectGrabber mapper) {
        this.url = url;
        this.dbName = dbName;
        this.collectionNames = collectionNames;
        this.query = query;
        this.filterByNamespace = filterByNamespace;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("document"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
        this.spoutTask = new MongoSpoutTask(this.queue, this.url, this.dbName, this.collectionNames, this.query, this.filterByNamespace);
        Thread thread = new Thread(this.spoutTask);
        thread.start();
    }

    @Override
    public void close() {
        this.spoutTask.stopThread();
    }

    protected abstract void processNextTuple();

    @Override
    public void nextTuple() {
        processNextTuple();
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }
}
