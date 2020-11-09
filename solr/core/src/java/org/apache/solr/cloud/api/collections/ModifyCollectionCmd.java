package org.apache.solr.cloud.api.collections;

import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;

public class ModifyCollectionCmd implements OverseerCollectionMessageHandler.Cmd {

  private final OverseerCollectionMessageHandler ocmh;
  private final TimeSource timeSource;

  public ModifyCollectionCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
    this.timeSource = ocmh.cloudManager.getTimeSource();
  }

  @Override
  public AddReplicaCmd.Response call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {

    clusterState = new CollectionMutator(ocmh.cloudManager).modifyCollection(clusterState, message);

    AddReplicaCmd.Response response = new AddReplicaCmd.Response();
    response.clusterState = clusterState;
    return response;
  }
}
