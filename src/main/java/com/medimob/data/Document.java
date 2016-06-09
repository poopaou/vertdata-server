package com.medimob.data;

import io.vertx.core.json.JsonObject;
import javax.annotation.Nonnull;

/**
 * Document's Meta properties keys.
 * <p>
 * Created by cyrille on 13/03/16.
 */
public final class Document {

  /**
   * Document id.
   * Type : {@link String}
   */
  public static final String ID = "_id";

  /**
   * Document change revisions.
   * Type : {@link io.vertx.core.json.JsonArray}
   */
  public static final String CHANGES = "_changes";

  /**
   * Document conflict revisions.
   * Type : {@link io.vertx.core.json.JsonArray}
   */
  public static final String CONFLICTS = "_conflicts";

  /**
   * Document creation date.
   * Type : {@link java.time.Instant}
   */
  public static final String CREATE_DATE = "_createDate";

  /**
   * Document creation date.
   * Type : {@link java.time.Instant}
   */
  public static final String UPDATE_DATE = "_updateDate";

  /**
   * Document current revision id.
   * Type : {@link String}
   */
  public static final String REVISION_ID = "_revisionId";

  /**
   * Deleted flag.
   * Type : {@link Boolean}
   */
  public static final String DELETED = "_deleted";

  /**
   * Document protected properties names.
   */
  public static final String[] PROTECTED_PROPERTIES = new String[] {
      ID, CHANGES, CONFLICTS, CREATE_DATE, UPDATE_DATE, REVISION_ID
  };

  private Document() {
    throw new IllegalStateException("no instance !");
  }

  @Nonnull
  public static String getIdOrThrow(@Nonnull JsonObject document) {
    Jsons.assertHasValue(document, ID);
    return document.getString(ID);
  }

  @Nonnull
  public static String getRevisionOrThrow(@Nonnull JsonObject document) {
    Jsons.assertHasValue(document, REVISION_ID);
    return document.getString(REVISION_ID);
  }
}
