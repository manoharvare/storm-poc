package com.yogesh.mongospoutesbolt.spout;

import com.mongodb.BasicDBObject;
import com.yogesh.mongospoutesbolt.spout.declaration.MongoObjectGrabber;
import org.apache.log4j.Logger;
import org.apache.storm.tuple.Values;

import java.io.Serializable;

public class MongoOpLogSpout extends MongoSpoutBase implements Serializable {

    private static final long serialVersionUID = 5498284114575395939L;
    static Logger LOG = Logger.getLogger(MongoOpLogSpout.class);

    private static String[] collectionNames = {"oplog.rs"};
    private String filterByNamespace;

    public MongoOpLogSpout(String url) {
        super(url, "local", collectionNames, null, null);
    }

    public MongoOpLogSpout(String url, BasicDBObject query) {
        super(url, "local", collectionNames, query, null);
    }

    public MongoOpLogSpout(String url, String filterByNamespace) {
        super(url, "local", collectionNames, null, null);
        this.filterByNamespace = filterByNamespace;
    }

    public MongoOpLogSpout(String url, BasicDBObject query, String filterByNamespace) {
        super(url, "local", collectionNames, query, filterByNamespace, null);
        this.filterByNamespace = filterByNamespace;
    }

    public MongoOpLogSpout(String url, MongoObjectGrabber mapper) {
        super(url, "local", collectionNames, null, mapper);
    }

    public MongoOpLogSpout(String url, BasicDBObject query, MongoObjectGrabber mapper) {
        super(url, "local", collectionNames, query, mapper);
    }

    public MongoOpLogSpout(String url, String filterByNamespace, MongoObjectGrabber mapper) {
        super(url, "local", collectionNames, null, mapper);
        this.filterByNamespace = filterByNamespace;
    }

    public MongoOpLogSpout(String url, BasicDBObject query, String filterByNamespace, MongoObjectGrabber mapper) {
        super(url, "local", collectionNames, query, mapper);
        this.filterByNamespace = filterByNamespace;
    }


    @Override
    protected void processNextTuple() {
        BasicDBObject object = this.queue.poll();
        if (object != null) {
            System.out.println("processNextTuple:--->"+ object.toJson());
            this.collector.emit(new Values(object.toJson()));
        }
    }


}
