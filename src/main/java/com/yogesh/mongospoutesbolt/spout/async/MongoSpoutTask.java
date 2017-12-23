package com.yogesh.mongospoutesbolt.spout.async;

import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// We need to handle the actual messages in an internal thread to ensure we never block, so we will be using a non blocking queue between the
// driver and the db
public class MongoSpoutTask implements Callable<Boolean>, Runnable, Serializable {
    private static final long serialVersionUID = 4440209304544126477L;
    static Logger LOG = Logger.getLogger(MongoSpoutTask.class);
    FindIterable<Document> documents;
    private LinkedBlockingQueue<BasicDBObject> queue;
    private static MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    private MongoCursor<Document> cursor;
    private BasicDBObject query;

    private AtomicBoolean running = new AtomicBoolean(true);
    private String[] collectionNames;

    public MongoSpoutTask(){ }

    public MongoSpoutTask(LinkedBlockingQueue<BasicDBObject> queue, String url, String dbName, String[] collectionNames, BasicDBObject query) {
        this.queue = queue;
        this.collectionNames = collectionNames;
        this.query = query;

        initializeMongo(url, dbName);
    }

    private void initializeMongo(String url, String dbName) {
        mongoClient = new MongoClient(new MongoClientURI(url));
        database = mongoClient.getDatabase(dbName);
    }

    public void stopThread() {
        running.set(false);
    }

    @Override
    public Boolean call() throws Exception {
        String collectionName = locateValidOpCollection(collectionNames);
        if (collectionName == null)
            throw new Exception("Could not locate any of the collections provided or not capped collection");
        collection = this.database.getCollection(collectionName);
        documents = collection.find(query);
        documents.noCursorTimeout(true).cursorType(CursorType.Tailable).maxAwaitTime(100, TimeUnit.SECONDS);
        cursor = documents.iterator();
        while (running.get()) {
            try {
                if (this.cursor.hasNext()) {
                    if (LOG.isInfoEnabled()) LOG.info("Fetching a new item from MongoDB cursor");
                    this.queue.put(new BasicDBObject(this.cursor.next()));
                } else {
                    Thread.sleep(50);
                }
            } catch (Exception e) {
                if (running.get()) throw new RuntimeException(e);
            }
        }
        return true;
    }

    public Document getUpdatedDocument(String id, String dbName, String collectionName) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> sslCustomerApplication = database.getCollection(collectionName);
        BasicDBObject query = new BasicDBObject();
        query.put("_id", id);
        FindIterable<Document> documents = sslCustomerApplication.find(query);
        MongoCursor<Document> iterator = documents.iterator();
        Document doc =null;
        while (iterator.hasNext()){
            doc = iterator.next();
        }
        return doc;
    }

    private String locateValidOpCollection(String[] collectionNames) {
        String collectionName = null;
        for (int i = 0; i < collectionNames.length; i++) {
            String name = collectionNames[i];
            collection = this.database.getCollection(name);
            documents = collection.find().sort(new BasicDBObject("$natural", -1)).limit(1);
            cursor = documents.iterator();
            if (cursor.hasNext()) {
                collectionName = name;
                break;
            }
        }
        return collectionName;
    }


    @Override
    public void run() {
        try {
            call();
        } catch (Exception e) {
            LOG.error(e);
        }
    }
}
