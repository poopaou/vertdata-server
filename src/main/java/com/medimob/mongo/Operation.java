package com.medimob.mongo;

import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Created by cyrille on 17/03/16.
 */
public final class Operation {

    /**
     *  Collection name.
     *  Type : {@link String}.
     */
    public static final String COLLECTION = "Collection";

    /**
     * Query / Update / Delete query.
     * Type : {@link io.vertx.core.json.JsonObject}
     */
    public static final String QUERY = "Query";

    /**
     * Document value.
     * Type : {@link JsonObject}
     */
    public static final String DOCUMENT = "Document";

    /**
     * Query find unique object.
     * Type : {@link Boolean}
     */
    public static final String UNIQUE = "Unique";

    /**
     * Query unique projection.
     * Type : {@link JsonObject}
     */
    public static final String PROJECTION = "Projection";


    private Operation() {
        throw new IllegalStateException("no instance !");
    }


    static String getCollectionOrThrow(@Nonnull JsonObject jsonObject){
        final String value = jsonObject.getString(COLLECTION);
        assertArgumentNotNull(value, COLLECTION);
        return value;
    }

    static JsonObject getDocumentOrThrow(@Nonnull JsonObject jsonObject){
        final JsonObject value = jsonObject.getJsonObject(DOCUMENT);
        assertArgumentNotNull(value, DOCUMENT);
        return value;
    }

    static JsonObject getQueryOrThrow(@Nonnull JsonObject jsonObject){
        final JsonObject value = jsonObject.getJsonObject(QUERY);
        assertArgumentNotNull(value, QUERY);
        return value;
    }

    static JsonObject getProjectionOrThrow(@Nonnull JsonObject jsonObject){
        final JsonObject value = jsonObject.getJsonObject(PROJECTION);
        assertArgumentNotNull(value, PROJECTION);
        return value;
    }

    private static void assertArgumentNotNull(Object o, String argName){
        if (Objects.isNull(o)){
            throw new IllegalStateException(String.format("Missing argument named %s", argName));
        }
    }
}
