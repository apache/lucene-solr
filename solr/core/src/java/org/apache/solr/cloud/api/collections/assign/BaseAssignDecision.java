package org.apache.solr.cloud.api.collections.assign;

/**
 *
 */
public abstract class BaseAssignDecision implements AssignDecision {

  protected final AssignRequest request;

  protected BaseAssignDecision(AssignRequest request) {
    this.request = request;
  }

  @Override
  public AssignRequest getAssignRequest() {
    return request;
  }
}
