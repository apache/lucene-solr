/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;

/**
 * This interface abstracts the details of dealing with Zookeeper and Solr from the autoscaling framework.
 */
public interface ClusterDataProvider extends Closeable {
  /**
   * Get the value of each tag for a given node
   *
   * @param node node name
   * @param tags tag names
   * @return a map of tag vs value
   */
  Map<String, Object> getNodeValues(String node, Collection<String> tags);

  /**
   * Get the details of each replica in a node. It attempts to fetch as much details about
   * the replica as mentioned in the keys list. It is not necessary to give all details
   * <p>
   * the format is {collection:shard :[{replicadetails}]}
   */
  Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys);

  /**
   * Get the current set of live nodes.
   */
  Collection<String> getLiveNodes();

  ClusterState getClusterState() throws IOException;

  Map<String, Object> getClusterProperties();

  default <T> T getClusterProperty(String key, T defaultValue) {
    T value = (T) getClusterProperties().get(key);
    if (value == null)
      return defaultValue;
    return value;
  }

  AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws ConnectException, InterruptedException, IOException;

  default AutoScalingConfig getAutoScalingConfig() throws ConnectException, InterruptedException, IOException {
    return getAutoScalingConfig(null);
  }

  /**
   * Get the collection-specific policy
   */
  String getPolicyNameByCollection(String coll);

  @Override
  default void close() throws IOException {
  }

  // ZK-like methods

  boolean hasData(String path) throws IOException;

  List<String> listData(String path) throws NoSuchElementException, IOException;

  class VersionedData {
    public final int version;
    public final byte[] data;

    public VersionedData(int version, byte[] data) {
      this.version = version;
      this.data = data;
    }
  }

  VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException;

  default VersionedData getData(String path) throws NoSuchElementException, IOException {
    return getData(path, null);
  }

  // mutators

  void makePath(String path) throws IOException;

  void createData(String path, byte[] data, CreateMode mode) throws IOException;

  void removeData(String path, int version) throws NoSuchElementException, IOException;

  void setData(String path, byte[] data, int version) throws NoSuchElementException, IOException;

  List<OpResult> multi(final Iterable<Op> ops) throws IOException;

  // Solr-like methods

  SolrResponse request(SolrRequest req) throws IOException;

  HttpClient getHttpClient();

  interface DistributedQueueFactory {
    DistributedQueue makeQueue(String path) throws IOException;
    void removeQueue(String path) throws IOException;
  }

  DistributedQueueFactory getDistributedQueueFactory();
}
