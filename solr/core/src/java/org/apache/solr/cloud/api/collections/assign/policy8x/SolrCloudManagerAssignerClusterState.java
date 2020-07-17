package org.apache.solr.cloud.api.collections.assign.policy8x;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.cloud.api.collections.assign.AssignerClusterState;
import org.apache.solr.cloud.api.collections.assign.AssignerCollectionState;
import org.apache.solr.cloud.api.collections.assign.AssignerNodeState;
import org.apache.solr.cloud.api.collections.assign.AssignerReplica;
import org.apache.solr.cloud.api.collections.assign.AssignerShardState;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;

/**
 *
 */
public class SolrCloudManagerAssignerClusterState implements AssignerClusterState {
  private final Map<String, Object> properties = new HashMap<>();
  private final SolrCloudManager cloudManager;

  public SolrCloudManagerAssignerClusterState(SolrCloudManager cloudManager) throws Exception {
    this.cloudManager = cloudManager;
    AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
    properties.put(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, Utils.toJSONString(autoScalingConfig));
  }

  @Override
  public Set<String> getNodes() {
    return cloudManager.getClusterStateProvider().getLiveNodes();
  }

  @Override
  public Set<String> getLiveNodes() {
    return cloudManager.getClusterStateProvider().getLiveNodes();
  }

  @Override
  public Collection<String> getCollections() {
    try {
      return cloudManager.getClusterStateProvider().getClusterState().getCollectionStates().keySet();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public AssignerNodeState getNodeState(String nodeName, Collection<String> nodeKeys, Collection<String> replicaKeys) {
    final NodeStateProvider nodeStateProvider = cloudManager.getNodeStateProvider();
    return new AssignerNodeState() {
      @Override
      public Collection<AssignerReplica> getReplicas() {
        Map<String, Map<String, List<Replica>>> replicaInfos = nodeStateProvider.getReplicaInfo(nodeName, replicaKeys);
        List<AssignerReplica> replicas = new ArrayList<>();
        replicaInfos.forEach((coll, shards) -> {
          shards.forEach((shard, infos) -> {
            infos.forEach(info -> {
              AssignerReplica ar = new AssignerReplica(info.getName(), info.getNodeName(), info.getCollection(),
                  info.getShard(), info.getCoreName(), info.getType().toString(), info.getState().toString(),
                  info.getProperties());
              replicas.add(ar);
            });
          });
        });
        return replicas;
      }

      @Override
      public long getTotalDiskGB() {
        return -1;
      }

      @Override
      public long getFreeDiskGB() {
        return -1;
      }

      @Override
      public Map<String, Object> getProperties() {
        return nodeStateProvider.getNodeValues(nodeName, nodeKeys);
      }
    };
  }

  @Override
  public AssignerCollectionState getCollectionState(String collectionName, Collection<String> replicaKeys) {
    ClusterState.CollectionRef collRef = cloudManager.getClusterStateProvider().getState(collectionName);
    if (collRef == null) {
      return null;
    }
    final DocCollection coll = collRef.get();
    return new AssignerCollectionState() {
      @Override
      public String getCollection() {
        return collectionName;
      }

      @Override
      public Collection<String> getShards() {
        return coll.getSlicesMap().keySet();
      }

      @Override
      public AssignerShardState getShardState(String shardName) {
        Slice slice = coll.getSlice(shardName);
        if (slice == null) {
          return null;
        }
        return new AssignerShardState() {
          @Override
          public String getName() {
            return slice.getName();
          }

          @Override
          public Collection<AssignerReplica> getReplicas() {
            return slice.getReplicas().stream()
                .map(r -> new AssignerReplica(
                    r.getName(),
                    r.getNodeName(),
                    r.getCollection(),
                    r.getShard(),
                    r.getCoreName(),
                    r.getType().toString(),
                    r.getState().toString(),
                    r.getProperties())).collect(Collectors.toList());
          }

          @Override
          public Map<String, Object> getProperties() {
            return slice.getProperties();
          }
        };
      }

      @Override
      public Map<String, Object> getProperties() {
        return coll.getProperties();
      }
    };
  }

  @Override
  public Map<String, Object> getProperties() {
    return properties;
  }
}
