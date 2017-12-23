package com.yogesh.mongospoutesbolt.spout;

import com.mongodb.BasicDBObject;
import com.yogesh.mongospoutesbolt.spout.declaration.MongoObjectGrabber;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.List;

public class MongoCappedCollectionSpout extends MongoSpoutBase implements Serializable {

    private static final long serialVersionUID = 1221725440580018348L;

    static Logger LOG = Logger.getLogger(MongoCappedCollectionSpout.class);

    public MongoCappedCollectionSpout(String url, String collectionName) {
        super(url, null, new String[]{collectionName}, null, null);
    }

    public MongoCappedCollectionSpout(String url, String collectionName, MongoObjectGrabber mapper) {
        super(url, null, new String[]{collectionName}, null, mapper);
    }

    public MongoCappedCollectionSpout(String url, String collectionName, BasicDBObject query) {
        super(url, null, new String[]{collectionName}, query, null);
    }

    public MongoCappedCollectionSpout(String url, String collectionName, BasicDBObject query, MongoObjectGrabber mapper) {
        super(url, null, new String[]{collectionName}, query, mapper);
    }

    @Override
    protected void processNextTuple() {
        BasicDBObject object = this.queue.poll();
        // If we have an object, let's process it, map and emit it
        if (object != null) {
            // Map the object to a tuple
            List<Object> tuples = null;//this.mapper.map(object);

            // Fetch the object Id
            ObjectId objectId = (ObjectId) object.get("_id");

            // Emit the tuple collection
            this.collector.emit(tuples, objectId);
        }
    }
}
