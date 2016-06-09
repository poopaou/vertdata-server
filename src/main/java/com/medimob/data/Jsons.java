package com.medimob.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.flipkart.zjsonpatch.JsonDiff;
import com.flipkart.zjsonpatch.JsonPatch;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Created by cyrille on 01/04/16.
 */
public final class Jsons {

  private Jsons() {
    throw new IllegalStateException("No instance !");
  }

  /**
   * Assert that the given {@link JsonObject} contains non null values for the given property name.
   *
   * @param object the given {@link JsonObject}
   * @param property the non null property name.
   */
  public static void assertHasValue(@Nonnull JsonObject object, @Nonnull String property) {
    Objects.requireNonNull(object);
    Objects.requireNonNull(object.getValue(property));
  }

  /**
   * Get Json Diff between given source and target {@link JsonObject}.
   *
   * @param source the source json object.
   * @param target the target json object.
   * @return json diff array.
   */
  public static JsonArray createJsonDiffBetweenDocuments(@Nonnull JsonObject source,
      @Nonnull JsonObject target) {
    Objects.requireNonNull(source);
    Objects.requireNonNull(target);

    JsonNode sourceNode = Json.mapper.valueToTree(source);
    JsonNode targetNode = Json.mapper.valueToTree(target);

    JsonNode diffNode = JsonDiff.asJson(sourceNode, targetNode);
    return new JsonArray(diffNode.toString());
  }

  /**
   * Apply Json diff on the given {@link JsonObject}.
   *
   * @param diff json diff array.
   * @param object the json object where the diff will be applied.
   * @return json object with diff.
   */
  public static JsonObject applyDiff(@Nonnull JsonArray diff, @Nonnull JsonObject object) {
    Objects.requireNonNull(diff);
    Objects.requireNonNull(object);

    JsonNode diffNode = Json.mapper.valueToTree(diff);
    JsonNode sourceNode = Json.mapper.valueToTree(object);

    JsonNode resultNode = JsonPatch.apply(diffNode, sourceNode);
    return new JsonObject(resultNode.toString());
  }

  /**
   * Remove properties form the given {@link JsonObject}.
   *
   * @param document the given json object.
   * @param properties the properties to be removed.
   * @return new json object minus listed properties names.
   */
  @Nonnull
  public static JsonObject removeProperties(@Nonnull JsonObject document, String... properties) {
    Objects.requireNonNull(document, "missing document");
    JsonObject copy = document.copy();
    if (properties != null) {
      for (String prop : properties) {
        copy.remove(prop);
      }
    }
    return copy;
  }
}
