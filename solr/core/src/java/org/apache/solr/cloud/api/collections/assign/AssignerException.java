package org.apache.solr.cloud.api.collections.assign;

/**
 *
 */
public class AssignerException extends Exception {
  public AssignerException(String message) {
    super(message);
  }

  public AssignerException(String message, Throwable cause) {
    super(message, cause);
  }

  public AssignerException(Throwable cause) {
    super(cause);
  }
}
