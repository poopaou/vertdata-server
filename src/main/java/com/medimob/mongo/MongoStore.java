package com.medimob.mongo;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.UpdateOptions;

import java.util.Objects;

/**
 * MONGO Store verticle.
 *
 * Created by cyrille on 12/03/16.
 */
public class MongoStore extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(MongoStore.class);

    public static final String QUERY_TOPIC  = "com.medimob.data_QUERY";
    public static final String UPDATE_TOPIC = "com.medimob.data_UPDATE";
    public static final String CREATE_TOPIC = "com.medimob.data_CREATE";
    public static final String DELETE_TOPIC = "com.medimob.data_DELETE";


    private MongoClient mongoClient;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        logger.debug("Starting mongo client");

        mongoClient = MongoClient.createNonShared(vertx, config());
        Objects.requireNonNull(mongoClient);

        EventBus bus = vertx.eventBus();
        Objects.requireNonNull(bus);

        MessageConsumer<JsonObject> consumer;

        consumer= bus.consumer(QUERY_TOPIC);
        consumer.handler(this::doQuery);

        consumer= bus.consumer(UPDATE_TOPIC);
        consumer.handler(this::doUpdate);

        consumer= bus.consumer(CREATE_TOPIC);
        consumer.handler(this::doCreate);

        consumer= bus.consumer(DELETE_TOPIC);
        consumer.handler(this::doDelete);

    }

    @Override
    public void stop() throws Exception {
        super.stop();
        mongoClient.close();

    }

    private void doQuery(Message<JsonObject> message){
        logger.debug("Query request received");
        logger.trace("Message body : {0}", message.body());

        final JsonObject body = message.body();
        Objects.requireNonNull(body);

        final String collection = Operation.getCollectionOrThrow(body);
        final JsonObject query = Operation.getQueryOrThrow(body);

        if (body.getBoolean(Operation.UNIQUE, false)){
            final JsonObject projections = body.getJsonObject(Operation.PROJECTION);

            logger.debug("Querying collection {0} for single doc : query {1}, projection {3}", collection, query, projections);
            mongoClient.findOne(collection, query, projections, result -> handleResponse(message, result));
        }
        else {
            logger.debug("Querying collection {0} for doc : query {1}", collection, query);
            mongoClient.find(collection, query, result -> handleResponse(message, result));
        }

    }

    private void doUpdate(Message<JsonObject> message){
        logger.debug("Update request received");
        logger.trace("Message body {0}", message.body());

        final JsonObject body = message.body();
        Objects.requireNonNull(body);

        final String collection = Operation.getCollectionOrThrow(body);
        final JsonObject query = Operation.getQueryOrThrow(body);
        final JsonObject document = Operation.getDocumentOrThrow(body);

        logger.debug("updating collection {0}, query {1}", collection, query);
        mongoClient.update(collection, query, document, result -> handleResponse(message, result));

    }

    private void doCreate(Message<JsonObject> message){
        logger.debug("Create request received");
        logger.trace("Message body {0}", message.body());

        final JsonObject body = message.body();
        Objects.requireNonNull(body);

        final String collection = Operation.getCollectionOrThrow(body);
        final JsonObject document = Operation.getDocumentOrThrow(body);

        logger.debug("insert in collection {0}", collection);
        mongoClient.insert(collection, document, result -> handleResponse(message, result));

    }

    private void doDelete(Message<JsonObject> message){
        logger.debug("Query received");
        logger.trace("Message body {0}", message.body());

        final JsonObject body = message.body();
        Objects.requireNonNull(body);

        final String collection = Operation.getCollectionOrThrow(body);
        final JsonObject query = Operation.getQueryOrThrow(body);

        logger.debug("removing for collection {0}, qyery {1}", collection, query);
        mongoClient.remove(collection, query, result -> handleResponse(message, result));

    }

    private <T> void handleResponse(Message<JsonObject> message, AsyncResult<T> result){
        if (result.succeeded()){
            final T res = result.result();
            if (res instanceof String){
                logger.debug("New document id {0}", res);
            }
            message.reply(res);
        } else {
            Throwable cause = result.cause();
            cause.printStackTrace();
            logger.warn("Request error occured", cause);

            message.fail(500, cause.getMessage());
        }
    }
}
