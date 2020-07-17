package org.apache.solr.cloud.api.collections.assign.policy8x;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable;
import org.apache.solr.cloud.api.collections.assign.AssignerClusterState;
import org.apache.solr.cloud.api.collections.assign.AssignerNodeState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.Utils;

/**
 *
 */
public class AssignerNodeStateProvider implements NodeStateProvider {

  private final AssignerClusterState state;

  public AssignerNodeStateProvider(AssignerClusterState state) {
    this.state = state;
  }

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    AssignerNodeState nodeState = state.getNodeState(node, tags, Collections.emptyList());
    Map<String, Object> values = new HashMap<>();
    values.putAll(nodeState.getProperties());
    values.put(Variable.Type.FREEDISK.tagName, nodeState.getFreeDiskGB());
    values.put(Variable.Type.TOTALDISK.tagName, nodeState.getTotalDiskGB());
    return values;
  }

  @Override
  public Map<String, Map<String, List<Replica>>> getReplicaInfo(String node, Collection<String> keys) {
    AssignerNodeState nodeState = state.getNodeState(node, Collections.emptyList(), keys);
    Map<String, Map<String, List<Replica>>> replicas = new HashMap<>();
    nodeState.getReplicas().forEach(ar -> {
      Map<String, List<Replica>> perColl = replicas.computeIfAbsent(ar.getCollection(), Utils.NEW_HASHMAP_FUN);
      List<Replica> perShard = perColl.computeIfAbsent(ar.getShard(), Utils.NEW_ARRAYLIST_FUN);
      Replica r = new Replica(ar.getName(), ar.getNode(), ar.getCollection(), ar.getShard(),
          ar.getCore(), Replica.State.getState(ar.getState()),
          Replica.Type.get(ar.getType()), ar.getProperties());
      perShard.add(r);
    });
    return replicas;
  }

  @Override
  public void close() throws IOException {

  }
}
