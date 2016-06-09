package com.medimob.data;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoService;
import io.vertx.ext.mongo.UpdateResult;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.eventbus.EventBus;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import rx.Observable;

import static com.medimob.data.Jsons.applyDiff;
import static com.medimob.data.Jsons.assertHasValue;
import static com.medimob.data.Jsons.createJsonDiffBetweenDocuments;

/**
 * Created by cyrille on 17/03/16.
 */
public class SyncDataService extends io.vertx.rxjava.core.AbstractVerticle {

  public static final String CREATE = "com.medimob.data.sync_CREATE";
  public static final String UPDATE = "com.medimob.data.sync_UPDATE";
  public static final String DELETE = "com.medimob.data.sync_DELETE";
  public static final String FIND = "com.medimob.data.sync_FIND";
  public static final String BULK = "com.medimob.data.sync_BULK";

  /**
   * Revisions conllection name.
   */
  private static final String REVISIONS_CNAME = "revisions";
  private MongoService mongo;

  /**
   * Build document to the given revision state.
   *
   * @param changes doc revisions.
   * @param targetRevisionId targeted revision.
   * @return document.
   * @throws RevisionNotFound if revision has not been founded in document changes.
   */
  @Nonnull
  private static JsonObject createDocumentForRevisionId(@Nonnull JsonArray changes,
      @Nonnull String targetRevisionId) throws RevisionNotFound {
    Objects.requireNonNull(changes, "missing changes");
    Objects.requireNonNull(targetRevisionId, "missing revision target");

    JsonObject result = new JsonObject();
    boolean found = false;
    final int size = changes.size();
    JsonObject change;
    for (int i = 0; i < size; i++) {
      change = changes.getJsonObject(i);
      result = applyDiff(change.getJsonArray(DocChange.DIFFS), result);
      if (change.getString(DocChange.REV).equals(targetRevisionId)) {
        found = true;
        break;
      }
    }

    if (!found) {
      throw new RevisionNotFound(
          String.format("Canno't found revision with id %s", targetRevisionId));
    }
    return result;
  }

  /**
   * Creates a new empty pending revision.
   *
   * @return new revision
   */
  @Nonnull
  private static JsonObject createNewRevision() {
    return new JsonObject()
        .put(Revision.DATE, Instant.now())
        .put(Revision.STATE, Revision.State.PENDING)
        .put(Revision.CHANGES, new JsonArray());
  }

  /**
   * Create a new revision change object form document change.
   *
   * @param workingRevId the current working revision id.
   * @param change the document change.
   * @return new revision change.
   */
  @Nonnull
  private static JsonObject createRevisionChange(String workingRevId, JsonObject change) {
    return new JsonObject().put(workingRevId, change);
  }

  @Override
  public void start() throws Exception {
    super.start();
    // Register mongo proxy service.
    mongo = MongoService.createEventBusProxy(getVertx(), config().getString("address"));

    EventBus eventBus = vertx.eventBus();
    eventBus.<JsonObject>consumer(CREATE)
        .toObservable()
        .subscribe(msg -> {
          JsonObject body = msg.body();

          assertHasValue(body, "Collection");
          assertHasValue(body, "Document");

          String collection = body.getString("Collection");
          JsonObject document = body.getJsonObject("Document");

          final JsonObject revision = createNewRevision();
          doCreate(REVISIONS_CNAME, revision)
              .concatMap(revId -> createDocument(collection, revId, document))
              .concatMap(this::pushChangeToPendingRevision)
              .concatMap(this::endRevisionPending)
              .concatMap(this::findDoneRevision)
              .map(this::toRevisionResponse)
              .subscribe(
                  msg::reply,
                  e -> {
                    e.printStackTrace();
                    msg.fail(500, e.getMessage());
                  });
        });

    eventBus.<JsonObject>consumer(UPDATE)
        .toObservable()
        .subscribe(msg -> {
          JsonObject body = msg.body();

          assertHasValue(body, "Collection");
          assertHasValue(body, "Document");

          String collection = body.getString("Collection");
          JsonObject document = body.getJsonObject("Document");

          JsonObject revision = createNewRevision();
          doCreate(REVISIONS_CNAME, revision)
              .concatMap(revId -> updateDocument(revId, collection, document))
              .concatMap(this::pushChangeToPendingRevision)
              .concatMap(this::endRevisionPending)
              .concatMap(this::findDoneRevision)
              .map(this::toRevisionResponse)
              .subscribe(
                  msg::reply,
                  e -> {
                    e.printStackTrace();
                    msg.fail(500, e.getMessage());
                  });
        });

    eventBus.<JsonObject>consumer(DELETE)
        .toObservable()
        .subscribe(msg -> {
          JsonObject body = msg.body();

          assertHasValue(body, "Collection");
          assertHasValue(body, "DocumentId");
          assertHasValue(body, "RevisionId");

          String collection = body.getString("Collection");
          String documentId = body.getString("DocumentId");
          String deletedRevisionId = body.getString("RevisionId");

          JsonObject revision = createNewRevision();
          doCreate(REVISIONS_CNAME, revision)
              .concatMap(revId -> deleteDocument(collection, documentId, revId, deletedRevisionId))
              .concatMap(this::pushChangeToPendingRevision)
              .concatMap(this::endRevisionPending)
              .concatMap(this::findDoneRevision)
              .map(this::toRevisionResponse)
              .subscribe(
                  msg::reply,
                  e -> {
                    e.printStackTrace();
                    msg.fail(500, e.getMessage());
                  });
        });

    eventBus.<JsonObject>consumer(FIND)
        .toObservable()
        .subscribe(msg -> {
          JsonObject body = msg.body();

          assertHasValue(body, "Collection");
          assertHasValue(body, "DocumentId");

          String collection = body.getString("Collection");
          String documentId = body.getString("DocumentId");

          queryDocument(collection, documentId, body.getString("RevisionId"))
              .subscribe(
                  msg::reply,
                  e -> {
                    e.printStackTrace();
                    msg.fail(500, e.getMessage());
                  });
        });

    eventBus.<JsonObject>consumer(BULK).toObservable(); //TODO bulk.
  }

  private JsonObject toRevisionResponse(@Nonnull JsonObject revision) {
    JsonObject flattenRevision;
    JsonArray changes = revision.getJsonArray(Revision.CHANGES);
    if (changes.size() == 1) {
      Map<String, Object> docChange = changes.getJsonObject(0).getMap();
      flattenRevision = new JsonObject(docChange);
      flattenRevision
          .put(Document.ID, flattenRevision.getString(Change.DOCUMENT_ID))
          .put(Document.REVISION_ID, revision.getString(Revision.ID))
          .remove(Change.DOCUMENT_ID);
    } else {
      flattenRevision = new JsonObject(revision.getMap());
      flattenRevision.put(Document.REVISION_ID, revision.getString(Revision.ID))
          .remove(Change.DOCUMENT_ID);
    }
    return flattenRevision;
  }

  /**
   * Create new document.
   *
   * @param collection the document collection's name.
   * @param workingRevId the current revision
   * @param document doc data.
   * @return revision change observable.
   */
  private Observable<JsonObject> createDocument(@Nonnull String collection,
      @Nonnull String workingRevId, @Nonnull JsonObject document) {
    Objects.requireNonNull(workingRevId, "missing revision id");
    Objects.requireNonNull(collection, "missing collection name");
    Objects.requireNonNull(document, "missing document");

    // Build change
    Instant now = Instant.now();
    JsonArray diff = Jsons.createJsonDiffBetweenDocuments(new JsonObject(), document);
    return doCreate(
        collection,
        new JsonObject()
            .put(Document.REVISION_ID, workingRevId)
            .put(Document.CREATE_DATE, now)
            .put(Document.CHANGES, new JsonArray().add(buildDocumentDiff(workingRevId, now, diff))))
        .single(result -> result != null)
        .map(docId -> createRevisionChange(
            workingRevId,
            new JsonObject()
                .put(Change.DOCUMENT_ID, docId)
                .put(Change.INSERT, true)
        ));
  }

  /**
   * Update document.
   *
   * @param workingRevId new revision id.
   * @param collection document collection's name.
   * @param document document value.
   * @return revision change observable.
   */
  private Observable<JsonObject> updateDocument(@Nonnull String workingRevId,
      @Nonnull String collection, @Nonnull JsonObject document) {
    Objects.requireNonNull(workingRevId, "missing revision id");
    Objects.requireNonNull(collection, "missing collection name");
    Objects.requireNonNull(document, "missing document");

    assertHasValue(document, Document.ID);
    assertHasValue(document, Document.REVISION_ID);

    String docId = document.getString(Document.ID);
    String targetRevId = document.getString(Document.REVISION_ID);
    return doFindOne(
        collection,
        new JsonObject().put(Document.ID, docId),
        null)
        .first(result -> result != null)
        .concatMap(srcDocument -> {
          JsonObject revisionDocument = createDocumentForRevisionId(
              srcDocument.getJsonArray(Document.CHANGES),
              targetRevId
          );
          JsonArray diffs = createJsonDiffBetweenDocuments(revisionDocument, document);
          Instant now = Instant.now();
          JsonObject change = buildDocumentDiff(workingRevId, now, diffs);

          // Check if document has been deleted or if targetRevision is not the current document revision's.
          if (srcDocument.getBoolean(Document.DELETED, false) || !srcDocument.getString(
              Document.REVISION_ID).equals(targetRevId)) {
            change.put(DocChange.CONFLICT, targetRevId);
            return pushConflictToDocument(collection, docId, workingRevId, change);
          } else {
            // Build document change.
            return doUpdate(
                collection,
                new JsonObject()
                    .put(Document.ID, docId)
                    .put(Document.REVISION_ID, targetRevId),
                new JsonObject()
                    .put(
                        "$push",
                        new JsonObject().put(Document.CHANGES, change)
                    )
                    .put(
                        "$set",
                        new JsonObject()
                            .put(Document.UPDATE_DATE, now)
                            .put(Document.REVISION_ID, workingRevId)
                    )
            ).concatMap(result -> {
              if (result == 0L) {
                change.put(DocChange.CONFLICT, targetRevId);
                return pushConflictToDocument(collection, docId, workingRevId, change);
              } else {
                return Observable.just(createRevisionChange(
                    workingRevId,
                    new JsonObject()
                        .put(Change.DOCUMENT_ID, docId)
                        .put(Change.UPDATE, true)
                ));
              }
            });
          }
        });
  }

  @Nonnull
  private Observable<JsonObject> deleteDocument(@Nonnull String collection,
      @Nonnull String documentId, @Nonnull String revisionId, String deletedRevisionId) {
    Objects.requireNonNull(revisionId, "missing revision id");
    Objects.requireNonNull(collection, "missing collection name");
    Objects.requireNonNull(documentId, "missing document id");

    Instant now = Instant.now();
    return doUpdate(
        collection,
        new JsonObject()
            .put(Document.ID, documentId)
            .put(Document.REVISION_ID, deletedRevisionId),
        new JsonObject()
            .put(
                "$set",
                new JsonObject()
                    .put(Document.DELETED, true)
                    .put(Document.UPDATE_DATE, now)
                    .put(Document.REVISION_ID, revisionId)
            ))
        .single()
        .map(result -> {
          if (1L == result) {
            return new JsonObject()
                .put(Change.DOCUMENT_ID, documentId)
                .put(Change.DELETE, true);
          } else {
            return null;
          }
        });
  }

  @Nonnull
  private Observable<JsonObject> queryDocument(@Nonnull String collection,
      @Nonnull String documentId, @Nullable String revisionId) {
    Objects.requireNonNull(collection, "missing collection name");
    Objects.requireNonNull(documentId, "missing document id");

    return doFindOne(collection, new JsonObject().put(Document.ID, documentId), null)
        .single(result -> result != null)
        .map(result -> {
          // If no request revision has been specified target last document rev.
          String targetRevision = (revisionId != null) ? revisionId : result.getString
              (Document.REVISION_ID);

          JsonObject document = createDocumentForRevisionId(
              result.getJsonArray(Document.CHANGES),
              targetRevision
          );
          document.put(Document.ID, result.getString(Document.ID));
          document.put(Document.REVISION_ID, targetRevision);
          return document;
        });
  }

  /**
   * Create document change diff object.
   *
   * @param revisionId the revision.
   * @param now now date
   * @param diff json diff
   * @return diff object.
   */
  private JsonObject buildDocumentDiff(@Nonnull String revisionId, @Nonnull Instant now,
      @Nullable JsonArray diff) {
    return new JsonObject()
        .put(DocChange.REV, revisionId)
        .put(DocChange.DATE, now)
        .put(DocChange.DIFFS, diff);
  }

  /**
   * Add conflict to document.
   *
   * @param collection collection name
   * @param documentId the document id.
   * @param workingRevisionId the conflict revision
   * @return document changes.
   */
  @Nonnull
  private Observable<JsonObject> pushConflictToDocument(@Nonnull String collection,
      @Nonnull String documentId, @Nonnull String workingRevisionId, @Nonnull JsonObject confict) {
    Objects.requireNonNull(collection, "missing collection name");
    Objects.requireNonNull(documentId, "missing document id");
    Objects.requireNonNull(workingRevisionId, "missing conflict revision");
    Objects.requireNonNull(confict, "missing document change");

    return doUpdate(
        collection,
        new JsonObject().put(Document.ID, documentId),
        new JsonObject().put("$push", new JsonObject().put(Document.CONFLICTS, confict)))
        .first()
        .map(res -> createRevisionChange(workingRevisionId, new JsonObject()
            .put(Change.DOCUMENT_ID, documentId)
            .put(Change.CONFLICT, true))
        );
  }

  /**
   * Push changes to the given pending revision.
   *
   * @param changes changes to be pushed.
   * @return revision id observable.
   */
  @Nonnull
  private Observable<String> pushChangeToPendingRevision(@Nonnull JsonObject changes) {
    Objects.requireNonNull(changes, "missing change");

    return Observable.from(changes.fieldNames())
        .flatMap(revision -> {
          JsonObject change = changes.getJsonObject(revision);
          return doUpdate(
              REVISIONS_CNAME,
              new JsonObject()
                  .put(Revision.ID, revision)
                  .put(Revision.STATE, Revision.State.PENDING),
              new JsonObject()
                  .put("$push", new JsonObject().put(Revision.CHANGES, change)))
              .map(res -> revision);
        });
  }

  /**
   * End's revision with the given id.
   *
   * @param revisionId the revision id to close.
   * @return revision id.
   */
  @Nonnull
  public Observable<String> endRevisionPending(@Nonnull final String revisionId) {
    Objects.requireNonNull(revisionId, "missing revisionId");

    ObservableFuture<UpdateResult> observable = RxHelper.observableFuture();
    mongo.update(
        REVISIONS_CNAME,
        new JsonObject().put(Revision.ID, revisionId).put(Revision.STATE, Revision.State.PENDING),
        new JsonObject().put("$set", new JsonObject().put(Revision.STATE, Revision.State.DONE)),
        observable.toHandler()
    );
    return observable.map(res -> revisionId);
  }

  /**
   * Gets done revision.
   *
   * @param revisionId the revision id.
   * @return the revision observable.
   */
  @Nonnull
  public Observable<JsonObject> findDoneRevision(@Nonnull String revisionId) {
    Objects.requireNonNull(revisionId, "missing revisionId");

    JsonObject query = new JsonObject()
        .put(Revision.ID, revisionId)
        .put(Revision.STATE, Revision.State.DONE);
    return doFindOne(REVISIONS_CNAME, query, null);
  }

  /**
   * Do create (insert) document in db.
   *
   * @param collection the collection name where data will be inserted.
   * @param document the document to add.
   * @return the document _id.
   */
  @Nonnull
  private Observable<String> doCreate(@Nonnull String collection, @Nonnull JsonObject document) {
    Objects.requireNonNull(collection, "missing collection name");
    Objects.requireNonNull(document, "missing document");

    ObservableFuture<String> future = RxHelper.observableFuture();
    mongo.insert(collection, document, future.toHandler());
    return future;
  }

  /**
   * Do update document in db.
   *
   * @param collection the document collection.
   * @param query the update query.
   * @param update the update object.
   * @return update result count observable.
   */
  @Nonnull
  private Observable<Long> doUpdate(@Nonnull String collection, @Nonnull JsonObject query,
      @Nonnull JsonObject update) {
    Objects.requireNonNull(collection, "missing collection name");
    Objects.requireNonNull(query, "missing query");
    Objects.requireNonNull(update, "missing document");

    ObservableFuture<UpdateResult> future = RxHelper.observableFuture();
    mongo.update(collection, query, update, future.toHandler());
    return future.map(result -> result.wasAcknowledged() ? result.getModifiedCount() : 0L);
  }

  /**
   * Do find one document with projection.
   *
   * @param collection the document collection's.
   * @param query the query object.
   * @param projection the projection object.
   * @return query result observable.
   */
  @Nonnull
  private Observable<JsonObject> doFindOne(@Nonnull String collection, @Nonnull JsonObject query,
      @Nullable JsonObject projection) {
    Objects.requireNonNull(collection);
    Objects.requireNonNull(query);

    ObservableFuture<JsonObject> future = RxHelper.observableFuture();
    mongo.findOne(collection, query, projection, future.toHandler());
    return future;
  }
}
