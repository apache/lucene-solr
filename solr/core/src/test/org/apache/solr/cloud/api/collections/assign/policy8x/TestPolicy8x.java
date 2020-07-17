package org.apache.solr.cloud.api.collections.assign.policy8x;

import java.util.Collections;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.api.collections.assign.AddReplicaRequest;
import org.apache.solr.cloud.api.collections.assign.AssignDecisions;
import org.apache.solr.cloud.api.collections.assign.AssignRequest;
import org.apache.solr.cloud.api.collections.assign.ReplicaType;
import org.apache.solr.cloud.autoscaling.sim.SimCloudManager;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.TimeSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestPolicy8x extends SolrTestCaseJ4 {

  SolrCloudManager cloudManager;

  @Before
  public void initCluster() throws Exception {
    cloudManager = SimCloudManager.createCluster(100, TimeSource.get("simTime:50"));
  }

  @After
  public void shutdownCluster() throws Exception {
    if (cloudManager != null) {
      cloudManager.close();
    }
  }

  @Test
  public void testBasics() throws Exception {
    SolrCloudManagerAssignerClusterState initialState = new SolrCloudManagerAssignerClusterState(cloudManager);
    Policy8xAssigner assigner = new Policy8xAssigner();
    List<AssignRequest> requests = Collections.singletonList(new AddReplicaRequest("foo", "bar", ReplicaType.NRT, null, null, null));
    AssignDecisions decisions = assigner.computeAssignments(initialState, requests);
  }
}
