package com.yogesh.mongospoutesbolt.spout;

import com.mongodb.BasicDBObject;
import com.yogesh.mongospoutesbolt.spout.async.MongoSpoutTask;
import com.yogesh.mongospoutesbolt.spout.declaration.MongoObjectGrabber;
import org.apache.log4j.Logger;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.io.Serializable;

public class MongoOpLogSpout extends MongoSpoutBase implements Serializable {

    private static final long serialVersionUID = 5498284114575395939L;
    static Logger LOG = Logger.getLogger(MongoOpLogSpout.class);

    private static String[] collectionNames = {"oplog.rs"};
    private String filterByNamespace;
    private MongoSpoutTask spoutTask;

    public MongoOpLogSpout(String url) {
        super(url, "local", collectionNames, null, null);
        this.spoutTask = new MongoSpoutTask();
    }

    public MongoOpLogSpout(String url, BasicDBObject query) {
        super(url, "local", collectionNames, query, null);
        this.spoutTask = new MongoSpoutTask();
    }

    public MongoOpLogSpout(String url, String filterByNamespace) {
        super(url, "local", collectionNames, null, null);
        this.filterByNamespace = filterByNamespace;
        this.spoutTask = new MongoSpoutTask();
    }

    public MongoOpLogSpout(String url, BasicDBObject query, String filterByNamespace) {
        super(url, "local", collectionNames, query, null);
        this.filterByNamespace = filterByNamespace;
        this.spoutTask = new MongoSpoutTask();
    }

    public MongoOpLogSpout(String url, MongoObjectGrabber mapper) {
        super(url, "local", collectionNames, null, mapper);
        this.spoutTask = new MongoSpoutTask();
    }

    public MongoOpLogSpout(String url, BasicDBObject query, MongoObjectGrabber mapper) {
        super(url, "local", collectionNames, query, mapper);
        this.spoutTask = new MongoSpoutTask();
    }

    public MongoOpLogSpout(String url, String filterByNamespace, MongoObjectGrabber mapper) {
        super(url, "local", collectionNames, null, mapper);
        this.filterByNamespace = filterByNamespace;
        this.spoutTask = new MongoSpoutTask();
    }

    public MongoOpLogSpout(String url, BasicDBObject query, String filterByNamespace, MongoObjectGrabber mapper) {
        super(url, "local", collectionNames, query, mapper);
        this.filterByNamespace = filterByNamespace;
        this.spoutTask = new MongoSpoutTask();
    }

    @Override
    protected void processNextTuple() {
        BasicDBObject object = this.queue.poll();
        Document targetDbObject;
        if (object != null) {
            String operation = object.get("op").toString();
            String objectId ;
            if (operation.equals("i")) {
                targetDbObject = (Document) object.get("o");
                objectId = targetDbObject.get("_id").toString();
                this.collector.emit(new Values(objectId.toString(), targetDbObject.toJson()));
            } else if (operation.equals("u")) {
                String[] parts = filterByNamespace.split("\\.", 2);
                String database = parts[0], collection = parts[1];
                if (object.get("o2") != null && ((Document) object.get("o2")).get("_id") != null) {
                    objectId = ((Document) object.get("o2")).get("_id").toString();
                    targetDbObject = spoutTask.getUpdatedDocument(objectId, database, collection);
                    if (null != objectId && null != targetDbObject) {
                        this.collector.emit(new Values(objectId, targetDbObject.toJson()));
                    }
                }
            } else {

            }

        } else {
            System.out.println("Empty Object");
        }
    }
}
