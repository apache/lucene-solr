package org.apache.solr.cloud.api.collections.assign.policy8x;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.cloud.api.collections.assign.AssignerClusterState;
import org.apache.solr.cloud.api.collections.assign.AssignerCollectionState;
import org.apache.solr.cloud.api.collections.assign.AssignerShardState;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;

/**
 *
 */
public class AssignerClusterStateProvider implements ClusterStateProvider {

  private final AssignerClusterState state;
  private final ClusterState clusterState;

  public AssignerClusterStateProvider(AssignerClusterState state) {
    this.state = state;
    // build ClusterState
    Map<String, DocCollection> collections = new HashMap<>();
    state.getCollections().forEach(coll -> {
      AssignerCollectionState collState = state.getCollectionState(coll, Collections.emptySet());
      Map<String, Slice> slices = new HashMap<>();
      collState.getShards().forEach(shard -> {
        AssignerShardState shardState = collState.getShardState(shard);
        Map<String, Replica> replicas = new HashMap<>();
        shardState.getReplicas().forEach(ar -> {
          Replica r = new Replica(ar.getName(), ar.getNode(), ar.getCollection(), ar.getShard(),
              ar.getCore(), Replica.State.getState(ar.getState().toString()),
              Replica.Type.get(ar.getType().toString()), ar.getProperties());
          replicas.put(r.getName(), r);
        });
        Slice slice = new Slice(shard, replicas, shardState.getProperties(), coll);
        slices.put(slice.getName(), slice);
      });
      Map<String, Object> routerProp = (Map<String, Object>) collState.getProperties().getOrDefault(DocCollection.DOC_ROUTER, Collections.singletonMap("name", DocRouter.DEFAULT_NAME));
      DocRouter router = DocRouter.getDocRouter((String)routerProp.getOrDefault("name", DocRouter.DEFAULT_NAME));
      DocCollection docCollection = new DocCollection(coll, slices, collState.getProperties(), router, -1);
      collections.put(docCollection.getName(), docCollection);
    });
    clusterState = new ClusterState(state.getLiveNodes(), collections);
  }

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    return null;
  }

  @Override
  public Set<String> getLiveNodes() {
    return state.getLiveNodes();
  }

  @Override
  public List<String> resolveAlias(String alias) {
    throw new UnsupportedOperationException("resolveAlias");
  }

  @Override
  public Map<String, String> getAliasProperties(String alias) {
    throw new UnsupportedOperationException("getAliasProperties");
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return clusterState;
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return state.getProperties();
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    DocCollection docCollection = clusterState.getCollectionOrNull(coll);
    if (docCollection == null) {
      return null;
    }
    return docCollection.getPolicyName();
  }

  @Override
  public void connect() {

  }

  @Override
  public void close() throws IOException {

  }
}
