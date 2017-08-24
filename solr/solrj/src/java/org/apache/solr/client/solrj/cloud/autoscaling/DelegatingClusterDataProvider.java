package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;

/**
 *
 */
public class DelegatingClusterDataProvider implements ClusterDataProvider {
  protected ClusterDataProvider delegate;

  public DelegatingClusterDataProvider(ClusterDataProvider delegate) {
    this.delegate = delegate;
  }

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    return delegate.getNodeValues(node, tags);
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    return delegate.getReplicaInfo(node, keys);
  }

  @Override
  public Collection<String> getLiveNodes() {
    return delegate.getLiveNodes();
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return delegate.getClusterProperties();
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return delegate.getClusterState();
  }

  @Override
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws ConnectException, InterruptedException, IOException {
    return delegate.getAutoScalingConfig(watcher);
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    return delegate.getPolicyNameByCollection(coll);
  }

  @Override
  public boolean hasData(String path) throws IOException {
    return delegate.hasData(path);
  }

  @Override
  public List<String> listData(String path) throws NoSuchElementException, IOException {
    return delegate.listData(path);
  }

  @Override
  public VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException {
    return delegate.getData(path, watcher);
  }

  @Override
  public void makePath(String path) throws IOException {
    delegate.makePath(path);
  }

  @Override
  public void createData(String path, byte[] data, CreateMode mode) throws IOException {
    delegate.createData(path, data, mode);
  }

  @Override
  public void removeData(String path, int version) throws NoSuchElementException, IOException {
    delegate.removeData(path, version);
  }

  @Override
  public void setData(String path, byte[] data, int version) throws NoSuchElementException, IOException {
    delegate.setData(path, data, version);
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws IOException {
    return delegate.multi(ops);
  }

  @Override
  public SolrResponse request(SolrRequest req) throws IOException {
    return delegate.request(req);
  }

  @Override
  public HttpClient getHttpClient() {
    return delegate.getHttpClient();
  }

  @Override
  public DistributedQueueFactory getDistributedQueueFactory() {
    return delegate.getDistributedQueueFactory();
  }
}
