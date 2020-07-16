package org.apache.solr.cloud.api.collections.assign;

/**
 *
 */
public class NoopDecision extends BaseAssignDecision {
  private final String reason;

  public NoopDecision(String reason, AssignRequest request) {
    super(request);
    this.reason = reason;
  }

  public String getReason() {
    return reason;
  }

  public static NoopDecision of(String reason, AssignRequest request) {
    return new NoopDecision(reason, request);
  }
}
