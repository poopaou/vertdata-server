package com.medimob.web;

import com.medimob.data.Document;
import com.medimob.data.SyncDataService;
import com.medimob.mongo.MongoStore;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

/**
 * Created by cyrille on 14/03/16.
 */
public class WebServer extends AbstractVerticle {

    private final Logger logger = LoggerFactory.getLogger(WebServer.class);

    private HttpServer server;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        server = vertx.createHttpServer();
        logger.debug("Starting web server");

        DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config().getJsonObject("mongo"));

        vertx.deployVerticle(new MongoStore(), options);
        vertx.deployVerticle(new SyncDataService());

        // Init router.
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.route(HttpMethod.GET, "/")
                .handler(routingCtx -> {
                   routingCtx.response().end("WebServer version: " + config().getString("version") + " is running");
                });

        router.route(HttpMethod.HEAD, "/data")
                .handler(routingCtx -> {

                });

        router.route(HttpMethod.POST, "/data")
                .consumes("application/json")
                .produces("application/json")
                .handler(routingCtx -> {
                    JsonObject value = new JsonObject();
                    value.put("Collection", "data");

                    JsonObject bodyAsJson = routingCtx.getBodyAsJson();
                    logger.debug("Post {0}", bodyAsJson);
                    value.put("Document", bodyAsJson);
                    routingCtx
                            .vertx()
                            .eventBus()
                            .send(SyncDataService.CREATE, value, event -> {
                                if (event.failed()){
                                    event.cause().printStackTrace();
                                    routingCtx.response()
                                            .setStatusCode(500)
                                            .end("failed");
                                } else {
                                    routingCtx.response().end(event.result().body().toString());

                                }
                            });
                });

        router.route(HttpMethod.PUT, "/data/:id")
                .consumes("application/json")
                .produces("application/json")
                .handler(routingCtx -> {
                    String id = routingCtx.request().getParam("id");

                    JsonObject value = new JsonObject();
                    value.put("Collection", "data");

                    JsonObject bodyAsJson = routingCtx.getBodyAsJson();
                    bodyAsJson.put(Document.ID, id);
                    logger.debug("Post {0}", bodyAsJson);
                    value.put("Document", bodyAsJson);
                    routingCtx
                            .vertx()
                            .eventBus()
                            .send(SyncDataService.UPDATE, value, event -> {
                                if (event.failed()){
                                    event.cause().printStackTrace();
                                    routingCtx.response()
                                            .setStatusCode(500)
                                            .end("failed");
                                } else {
                                    routingCtx.response().end(event.result().body().toString());

                                }
                            });
                });

        router.route(HttpMethod.HEAD, "/data/:id")
                .produces("application/json")
                .handler(routingCtx -> {
                    String id = routingCtx.request().getParam("id");

                });

        router.route(HttpMethod.GET, "/data/:id")
                .consumes("application/json")
                .produces("application/json")
                .handler(routingCtx -> {
                    String id = routingCtx.request().getParam("id");


                });

        server.requestHandler(router::accept)
                .listen(8080, event -> {
                   if (event.failed()){
                       startFuture.fail(event.cause());
                   } else {
                       startFuture.complete();
                   }
                });
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        super.stop(stopFuture);
        if (server != null){
            server.close(event -> {
                if (event.succeeded()){
                    stopFuture.complete();
                } else {
                    stopFuture.fail(event.cause());
                }
            });
        } else {
            stopFuture.complete();
        }
    }
}
