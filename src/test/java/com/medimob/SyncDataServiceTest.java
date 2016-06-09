package com.medimob;

import com.medimob.data.Document;
import com.medimob.data.SyncDataService;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.runtime.Network;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoServiceVerticle;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.concurrent.CompletableFuture;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by cysondag on 08/06/2016.
 */
@RunWith(VertxUnitRunner.class)
public class SyncDataServiceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncDataServiceTest.class);

  private static MongodExecutable exe;

  private Vertx vertx;

  @BeforeClass
  public static void startMongo() throws Exception {
    LOGGER.debug("on before class");
    IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
        .defaultsWithLogger(Command.MongoD, LOGGER)
        .build();

    IMongodConfig config = new MongodConfigBuilder()
        .version(Version.Main.PRODUCTION)
        .net(new Net(27017, Network.localhostIsIPv6()))
        .build();

    exe = MongodStarter.getInstance(runtimeConfig).prepare(config);
    exe.start();
  }

  @AfterClass
  public static void stopMongo() {
    if (exe != null) {
      exe.stop();
    }
  }

  @Before
  public void deployVerticle(TestContext context) {
    LOGGER.debug("on before");
    vertx = Vertx.vertx();

    DeploymentOptions options = new DeploymentOptions();
    options.setConfig(new JsonObject().put("address", "mongo"));

    Async async = context.async();

    Future<String> mongoFuture = Future.future();
    vertx.deployVerticle(new MongoServiceVerticle(), options, mongoFuture.completer());

    Future<String> syncDataFuture = Future.future();
    vertx.deployVerticle(new SyncDataService(), options, syncDataFuture.completer());

    CompositeFuture.all(mongoFuture, syncDataFuture).setHandler(ar -> {
      if (ar.succeeded()) {
        async.complete();
      } else {
        context.fail();
      }
    });
  }

  @Test
  public void insertTest(TestContext context) {
    Async async = context.async();
    createDocument(new JsonObject()).thenAccept(result -> {
      LOGGER.debug("response : {}", result);
      // Test change document.
      context.assertNotNull(result);
      context.assertNotNull(result.getString(Document.ID));
      context.assertNotNull(result.getString(Document.REVISION_ID));
      context.assertEquals(true, result.getBoolean("insert", false));
      // End test.
      async.complete();
    });
  }

  @Test
  public void updateSuccessTest(TestContext context) {
    Async async = context.async();
    createDocument(new JsonObject()).thenCompose(result -> {
      JsonObject document = new JsonObject()
          .put(Document.ID, result.getString(Document.ID))
          .put(Document.REVISION_ID, result.getString(Document.REVISION_ID))
          .put("test", "test");
      return updateDocument(document);
    }).thenAccept(result -> {
      LOGGER.debug("response : {}", result);
      // Test change array.
      context.assertNotNull(result);
      context.assertNotNull(result.getString(Document.ID));
      context.assertNotNull(result.getString(Document.REVISION_ID));
      context.assertEquals(true, result.getBoolean("update", false));
      // End test.
      async.complete();
    });
  }

  @Test
  public void queryTest(TestContext context) {
    Async async = context.async();
    JsonObject doc = new JsonObject()
        .put("test", "test");

    createDocument(doc).thenCompose(result -> queryDocument(result.getString(Document.ID), null))
        .thenAccept(result -> {
          LOGGER.debug("response : {}", result);
          // Test change array.
          context.assertNotNull(result);
          context.assertNotNull(result.getString(Document.ID));
          context.assertNotNull(result.getString(Document.REVISION_ID));
          context.assertEquals("test", result.getString("test"));
          // End test.
          async.complete();
        });
  }

  @Test
  public void queryNotFoundTest(TestContext context) {
    Async async = context.async();
    queryDocument("", null)
        .thenAccept(result -> {
          context.assertTrue(false, "should not return values");
          async.complete();
        })
        .exceptionally(e -> {
          context.assertTrue(true);
          async.complete();
          return null;
        });
  }

  private CompletableFuture<JsonObject> createDocument(JsonObject document) {
    CompletableFuture<JsonObject> promise = new CompletableFuture<>();
    EventBus bus = vertx.eventBus();
    JsonObject insert = new JsonObject()
        .put("Collection", "test")
        .put("Document", document);

    bus.<JsonObject>send(SyncDataService.CREATE, insert, result -> {
      if (result.succeeded()) {
        promise.complete(result.result().body());
      } else {
        promise.cancel(true);
      }
    });
    return promise;
  }

  private CompletableFuture<JsonObject> updateDocument(JsonObject document) {
    CompletableFuture<JsonObject> promise = new CompletableFuture<>();
    EventBus bus = vertx.eventBus();
    JsonObject update = new JsonObject()
        .put("Collection", "test")
        .put("Document", document);

    bus.<JsonObject>send(SyncDataService.UPDATE, update, result -> {
      if (result.succeeded()) {
        promise.complete(result.result().body());
      } else {
        promise.completeExceptionally(result.cause());
      }
    });
    return promise;
  }

  private CompletableFuture<JsonObject> queryDocument(String documentId, String revisionId) {
    CompletableFuture<JsonObject> promise = new CompletableFuture<>();
    EventBus bus = vertx.eventBus();
    JsonObject update = new JsonObject()
        .put("Collection", "test")
        .put("DocumentId", documentId)
        .put("RevisionId", revisionId);

    bus.<JsonObject>send(SyncDataService.FIND, update, result -> {
      if (result.succeeded()) {
        promise.complete(result.result().body());
      } else {
        promise.completeExceptionally(result.cause());
      }
    });
    return promise;
  }
}
