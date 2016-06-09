package com.medimob.data;

/**
 * Created by cyrille on 10/03/16.
 */
public final class Revision {

  public static final String CHANGES = "changes";
  /**
   * Revision id.
   * Type : {@link String}
   */
  public static final String ID = "_id";
  /**
   * Deleted flag.
   * Type : {@link Boolean}
   */
  public static final String DELETED = "deleted";
  /**
   * Revision date.
   * Type : {@link java.time.Instant}
   */
  public static final String DATE = "date";
  /**
   * Revision value.
   * Type : {@link io.vertx.core.json.JsonObject}
   */
  public static final String VALUE = "value";
  /**
   * Revision state.
   * Type : {@link State}
   */
  public static final String STATE = "state";

  private Revision() {
    throw new IllegalStateException("no instance !");
  }

  public static final class State {
    public static final String PENDING = "PENDING";
    public static final String DONE = "DONE";
  }
}