package org.apache.solr.cloud.api.collections.assign.policy8x;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.cloud.api.collections.assign.AddReplicaDecision;
import org.apache.solr.cloud.api.collections.assign.AddReplicaRequest;
import org.apache.solr.cloud.api.collections.assign.Assigner;
import org.apache.solr.cloud.api.collections.assign.AssignerClusterState;
import org.apache.solr.cloud.api.collections.assign.AssignerException;
import org.apache.solr.cloud.api.collections.assign.AssignDecision;
import org.apache.solr.cloud.api.collections.assign.AssignDecisions;
import org.apache.solr.cloud.api.collections.assign.AssignRequest;
import org.apache.solr.cloud.api.collections.assign.NoopDecision;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.util.TimeSource;

/**
 *
 */
public class Policy8xAssigner implements Assigner {

  private TimeSource timeSource = TimeSource.NANO_TIME;

  @Override
  public AssignDecisions computeAssignments(AssignerClusterState initialState,
                                            List<AssignRequest> requests) throws InterruptedException, AssignerException {
    AssignerCloudManager cloudManager = new AssignerCloudManager(initialState, timeSource);
    try {
      AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
      List<AssignDecision> decisions = new ArrayList<>();
      // XXX this handles only Add requests
      for (AssignRequest req : requests) {
        if (req instanceof AddReplicaRequest) {
          AddReplicaRequest areq = (AddReplicaRequest) req;
          int nrtReplicas = areq.getType() == Replica.Type.NRT ? 1 : 0;
          int tlogReplicas = areq.getType() == Replica.Type.TLOG ? 1 : 0;
          int pullReplicas = areq.getType() == Replica.Type.PULL ? 1 : 0;
          List<ReplicaPosition> positions = PolicyHelper.getReplicaLocations(areq.getCollection(), autoScalingConfig,
              cloudManager, null, Collections.singletonList(areq.getShard()),
              nrtReplicas, tlogReplicas, pullReplicas, areq.getNodeSet() != null ? new ArrayList(areq.getNodeSet()) : null);
          positions.forEach(pos -> {
            decisions.add(new AddReplicaDecision(req, pos.node));
          });
        } else {
          decisions.add(NoopDecision.of("unsupported", req));
        }
      }
      // XXX we should return the new state here
      return new AssignDecisions(initialState, decisions);
    } catch (IOException e) {
      throw new AssignerException(e);
    }
  }
}
