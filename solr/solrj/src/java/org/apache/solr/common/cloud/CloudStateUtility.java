package org.apache.solr.common.cloud;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class CloudStateUtility {
  
  public static Stat exists(SolrZkClient zkClient) throws KeeperException, InterruptedException{
    return zkClient.exists(ZkStateReader.CLUSTER_STATE, null);
  }
  
  public static void create(CloudState state, SolrZkClient zkClient) throws UnsupportedEncodingException, KeeperException, InterruptedException, IOException {
    zkClient.create(ZkStateReader.CLUSTER_STATE,
        CloudState.store(state), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
  }

  public static CloudState get(SolrZkClient zkClient, Stat stat)
      throws KeeperException, InterruptedException {
    byte[] data = zkClient.getData(ZkStateReader.CLUSTER_STATE, null, stat);
    if (data == null) return new CloudState();
    CloudState state = CloudState.load(data);
    return state;
  }
  
  public static void update(SolrZkClient zkClient, CloudState state, Stat stat) throws KeeperException, InterruptedException, IOException {
    zkClient.setData(ZkStateReader.CLUSTER_STATE,
        CloudState.store(state));
  }

}