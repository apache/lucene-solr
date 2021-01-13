package org.apache.solr.cluster.placement;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class PlacementModificationException extends PlacementException {
  private final Map<String, String> rejectedModifications = new HashMap<>();

  public PlacementModificationException() {
    super();
  }

  public PlacementModificationException(String message) {
    super(message);
  }

  public PlacementModificationException(String message, Throwable cause) {
    super(message, cause);
  }

  public PlacementModificationException(Throwable cause) {
    super(cause);
  }

  public void addRejectedModification(String modification, String reason) {
    rejectedModifications.put(modification, reason);
  }

  public Map<String, String> getRejectedModifications() {
    return rejectedModifications;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    if (!rejectedModifications.isEmpty()) {
        sb.append(": ")
          .append(rejectedModifications.size())
          .append(" rejections:");
      rejectedModifications.forEach((modification, reason) ->
          sb.append("\n")
              .append(modification)
              .append("\t")
              .append(reason));

    }
    return sb.toString();
  }
}
