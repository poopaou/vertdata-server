package com.medimob.data;

import com.medimob.mongo.MongoStore;
import com.medimob.mongo.Operation;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.core.eventbus.Message;
import rx.Observable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Objects;

import static com.medimob.data.Jsons.*;

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

    @Override
    public void start() throws Exception {
        super.start();

        EventBus eventBus = vertx.eventBus();

        eventBus.<JsonObject>consumer(CREATE)
                .toObservable()
                .subscribe(msg -> {
                    JsonObject body = msg.body();

                    assertHasValue(body, "Collection");
                    String collection = body.getString("Collection");

                    assertHasValue(body, "Document");
                    JsonObject document = body.getJsonObject("Document");

                    final JsonObject revision = Revision.createNewRevision();
                    doCreate(REVISIONS_CNAME, revision)
                            .concatMap(revId -> createDocument(revId, collection, document))
                            .concatMap(this::pushChangeToRevision)
                            .concatMap(this::endRevision)
                            .concatMap(this::findDoneRevision)
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
                    String collection = body.getString("Collection");

                    assertHasValue(body, "Document");
                    JsonObject document = body.getJsonObject("Document");

                    JsonObject revision = Revision.createNewRevision();
                    doCreate(REVISIONS_CNAME, revision)
                            .concatMap(revId -> updateDocument(revId, collection, document))
                            .concatMap(this::pushChangeToRevision)
                            .concatMap(this::endRevision)
                            .concatMap(this::findDoneRevision)
                            .subscribe(
                                    msg::reply,
                                    e -> {
                                        e.printStackTrace();
                                        msg.fail(500, e.getMessage());
                                    });
                });

        //.subscribe(this::create);

        eventBus.<JsonObject>consumer(DELETE)
                .toObservable();
        //.subscribe(this::create);

        eventBus.<JsonObject>consumer(FIND)
                .toObservable();
        //.subscribe(this::create);

        eventBus.<JsonObject>consumer(BULK)
                .toObservable();
        //.subscribe(this::create);
    }

    /**
     * Create new document.
     *
     * @param revisionId the current revision
     * @param collection the document collection's name.
     * @param document   doc data.
     * @return revision change observable.
     */
    private Observable<JsonObject> createDocument(@Nonnull String revisionId, @Nonnull String collection, @Nonnull JsonObject document) {
        Objects.requireNonNull(revisionId, "missing revision id");
        Objects.requireNonNull(collection, "missing collection name");
        Objects.requireNonNull(document, "missing document");

        // Build change
        Instant now = Instant.now();
        JsonArray diff = Jsons.getDiff(new JsonObject(), document);
        return doCreate(
                collection,
                newJsonObject()
                        .put(Document.REVISION_ID, revisionId)
                        .put(Document.CREATE_DATE, now)
                        .put(Document.CHANGES, new JsonArray()
                                .add(buildDocumentDiff(revisionId, now, diff))
                        )
        )
                .single()
                .map(docId -> newJsonObject()
                        .put(revisionId, newJsonObject()
                                .put(Change.DOCUMENT_ID, docId)
                                .put(Change.INSERT, true)
                        )
                );
    }

    /**
     * Update document
     *
     * @param newRevisionId new revision id.
     * @param collection document collection's name.
     * @param document document value.
     * @return revision change observable.
     */
    private Observable<JsonObject> updateDocument(@Nonnull String newRevisionId, @Nonnull String collection, @Nonnull JsonObject document) {
        Objects.requireNonNull(newRevisionId, "missing revision id");
        Objects.requireNonNull(collection, "missing collection name");
        Objects.requireNonNull(document, "missing document");

        assertHasValue(document, Document.ID);
        String documentId = document.getString(Document.ID);

        assertHasValue(document, Document.REVISION_ID);
        String targetRevision = document.getString(Document.REVISION_ID);

        return doFindOne(collection, newJsonObject().put(Document.ID, documentId))
                .single()
                .concatMap(dbDoc -> {
                    final String sourceRevision = dbDoc.getString(Document.REVISION_ID);
                    JsonObject revisionDocument = documentToRevision(dbDoc.getJsonArray(Document.CHANGES), targetRevision);
                    JsonArray diffs = getDiff(revisionDocument, removeProperties(document, Document.PROTECTED_PROPERTIES));
                    if (diffs.size() == 0) {
                        // No changes has been made --> skip process.
                        return Observable.empty();
                    }

                    Instant now = Instant.now();
                    JsonObject change = buildDocumentDiff(newRevisionId, now, diffs);

                    // Check if document has been deleted or if targetRevision is not the current document revision.
                    if (dbDoc.getBoolean(Document.DELETED, false) || !targetRevision.equals(sourceRevision)) {
                        // Set document to the targeted revision.
                        // Compute diffs from revision.
                        return addConflictToDocument(collection, documentId, targetRevision, change);
                    } else {
                        // Build document change.
                        return doUpdate(
                                collection,
                                newJsonObject()
                                        .put(Document.ID, documentId)
                                        .put(Document.REVISION_ID, targetRevision),
                                newJsonObject()
                                        .put("$push", newJsonObject()
                                                .put(Document.CHANGES, change)
                                        )
                                        .put("$set", newJsonObject()
                                                .put(Document.UPDATE_DATE, now)
                                                .put(Document.REVISION_ID, newRevisionId)
                                        )
                        ).concatMap(result -> {
                            if (result == null || result == 0L) {
                                return addConflictToDocument(collection, documentId, targetRevision, change);
                            } else {
                                return Observable.just(newJsonObject(Change.DOCUMENT_ID, documentId).put(Change.UPDATE, true));
                            }
                        });
                    }
                });
    }

    @Nonnull
    private Observable<JsonObject> deleteDocument(@Nonnull String newRevisionId, @Nonnull String collection, @Nonnull String documentId, String targetRevision) {
        Objects.requireNonNull(newRevisionId, "missing revision id");
        Objects.requireNonNull(collection, "missing collection name");
        Objects.requireNonNull(documentId, "missing document id");

        Instant now = Instant.now();

        return doUpdate(
                collection,
                newJsonObject()
                        .put(Document.ID, documentId)
                        .put(Document.REVISION_ID, targetRevision),
                newJsonObject()
                        .put("$set", newJsonObject()
                                .put(Document.DELETED, true)
                                .put(Document.UPDATE_DATE, now)
                                .put(Document.REVISION_ID, newRevisionId)
                        )
        )
                .single()
                .map(result -> {
                    if (1L == result){
                        return newJsonObject()
                                .put(Change.DOCUMENT_ID, documentId)
                                .put(Change.DELETE, true);
                    } else {
                        return null;
                    }

                });
    }

    /**
     * Create document change diff object.
     * @param revisionId the revision.
     * @param now now date
     * @param diff json diff
     * @return diff object.
     */
    private JsonObject buildDocumentDiff(@Nonnull String revisionId, @Nonnull Instant now, @Nullable JsonArray diff) {
        return Jsons.newJsonObject()
                .put(DocChange.REV, revisionId)
                .put(DocChange.DATE, now)
                .put(DocChange.DIFFS, diff);
    }

    /**
     * Build document to the given revision state.
     *
     * @param changes doc revisions.
     * @param targetRevisionId targeted revision.
     * @return document.
     * @throws RevisionNotFound if revision has not been founded in document changes.
     */
    @Nonnull
    private JsonObject documentToRevision(@Nonnull JsonArray changes, @Nonnull String targetRevisionId) throws RevisionNotFound {
        Objects.requireNonNull(changes, "missing changes");
        Objects.requireNonNull(targetRevisionId, "missing revision target");

        JsonObject result = newJsonObject();
        boolean found = false;
        final int size = changes.size();
        JsonObject change;
        for (int i = 0; i < size; i++) {
            change = changes.getJsonObject(i);
            applyDiff(change.getJsonArray(DocChange.DIFFS), result);
            if (change.getString(DocChange.REV).equals(targetRevisionId)) {
                found = true;
                break;
            }
        }

        if(!found){
            throw new RevisionNotFound(String.format("Canno't found revision with id %s", targetRevisionId));
        }
        return result;
    }

    /**
     * Add conflict to document.
     *
     * @param collection collection name
     * @param documentId the document id.
     * @param conflictRevision the conflict revision
     * @return document changes.
     */
    @Nonnull
    private Observable<JsonObject> addConflictToDocument(@Nonnull String collection, @Nonnull  String documentId, @Nonnull String conflictRevision, @Nonnull JsonObject changes) {
        Objects.requireNonNull(collection, "missing collection name");
        Objects.requireNonNull(documentId, "missing document id");
        Objects.requireNonNull(conflictRevision, "missing conflict revision");
        Objects.requireNonNull(changes, "missing document change");

        changes.put(DocChange.CONFLICT, conflictRevision);
        return doUpdate(
                collection,
                newJsonObject()
                        .put(Document.ID, documentId),
                newJsonObject()
                        .put("$push", newJsonObject()
                                .put(Document.CONFLICTS, changes)
                        )
        )
                .first()
                .map(res -> newJsonObject()
                        .put(Change.DOCUMENT_ID, documentId)
                        .put(Change.CONFLICT, true)
                );
    }

    /**
     * Push changes to the given pending revision.
     *
     * @param changes changes to be pushed.
     * @return revision id observable.
     */
    @Nonnull
    private Observable<String> pushChangeToRevision(@Nonnull JsonObject changes) {
        Objects.requireNonNull(changes, "missing change");
        return Observable.from(changes.fieldNames())
                .flatMap(revision -> {
                    JsonObject change = changes.getJsonObject(revision);
                    return doUpdate(
                            REVISIONS_CNAME,
                            newJsonObject()
                                    .put(Revision.ID, revision)
                                    .put(Revision.STATE, Revision.State.PENDING),
                            newJsonObject()
                                    .put("$push", newJsonObject()
                                            .put(Revision.CHANGES, change)))
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
    public Observable<String> endRevision(@Nonnull final String revisionId) {
        Objects.requireNonNull(revisionId, "missing revisionId");
        return doUpdate(
                REVISIONS_CNAME,
                newJsonObject(Revision.ID, revisionId).put(Revision.STATE, Revision.State.PENDING),
                newJsonObject("$set", newJsonObject(Revision.STATE, Revision.State.DONE))
        )
                .map(result -> revisionId);
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

        return doFindOne(REVISIONS_CNAME, query);
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
        JsonObject operation = new JsonObject()
                .put(Operation.COLLECTION, collection)
                .put(Operation.DOCUMENT, document);

        return vertx.eventBus()
                .<String>sendObservable(MongoStore.CREATE_TOPIC, operation)
                .map(Message::body);
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
    private Observable<Long> doUpdate(@Nonnull String collection, @Nonnull JsonObject query, @Nonnull JsonObject update) {
        Objects.requireNonNull(collection, "missing collection name");
        Objects.requireNonNull(query, "missing query");
        Objects.requireNonNull(update, "missing document");
        JsonObject operation = new JsonObject()
                .put(Operation.COLLECTION, collection)
                .put(Operation.QUERY, query)
                .put(Operation.DOCUMENT, update);

        return vertx.eventBus()
                .<Long>sendObservable(MongoStore.UPDATE_TOPIC, operation)
                .map(Message::body);
    }

    /**
     * Do find one document.
     *
     * @param collection the document collection's.
     * @param query the query object.
     * @return query result observable.
     */
    @Nonnull
    private Observable<JsonObject> doFindOne(@Nonnull String collection, @Nonnull JsonObject query) {
        return doFindOne(collection, query, null);
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
    private Observable<JsonObject> doFindOne(@Nonnull String collection, @Nonnull JsonObject query, @Nullable JsonObject projection) {
        Objects.requireNonNull(collection);
        Objects.requireNonNull(query);
        JsonObject operation = new JsonObject()
                .put(Operation.QUERY, query)
                .put(Operation.COLLECTION, collection)
                .put(Operation.PROJECTION, projection)
                .put(Operation.UNIQUE, true);

        return vertx.eventBus()
                .<JsonObject>sendObservable(MongoStore.QUERY_TOPIC, operation)
                .flatMap(resp -> {
                    JsonObject result = resp.body();
                    if (result == null) {
                        String msg = String.format("Cannot find %s in collection %s", query, collection);
                        return Observable.error(new IllegalStateException(msg));
                    }
                    return Observable.just(result);
                });
    }

}
