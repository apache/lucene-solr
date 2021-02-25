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
package org.apache.solr.cloud;

import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.ConnectionManager.IsClosed;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>A ZK-based distributed queue. Optimized for single-consumer,
 * multiple-producer: if there are multiple consumers on the same ZK queue,
 * the results should be correct but inefficient.</p>
 *
 * <p>This implementation (with help from subclass {@link OverseerTaskQueue}) is used for the
 * <code>/overseer/collection-queue-work</code> queue used for Collection and Config Set API calls to the Overseer.</p>
 *
 * <p><i>Implementation note:</i> In order to enqueue a message into this queue, a {@link CreateMode#EPHEMERAL_SEQUENTIAL} response node is created
 * and watched at <code>/overseer/collection-queue-work/qnr-<i>monotonically_increasng_id</i></code>, then a corresponding
 * {@link CreateMode#PERSISTENT} request node reusing the same id is created at <code>/overseer/collection-queue-work/qn-<i>response_id</i></code>.</p>
 */
public class ZkDistributedQueue implements DistributedQueue {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String PREFIX = "qn-";

  /**
   * Theory of operation:
   * <p>
   * Under ordinary circumstances we neither watch nor poll for children in ZK.
   * Instead we keep an in-memory list of known child names.  When the in-memory
   * list is exhausted, we then fetch from ZK.
   * <p>
   * We only bother setting a child watcher when the queue has no children in ZK.
   */
  private static final Object _IMPLEMENTATION_NOTES = null;
  public static final byte[] DATA = new byte[0];

  final String dir;

  final SolrZkClient zookeeper;

  final Stats stats;
  private final int maxQueueSize;

  public ZkDistributedQueue(SolrZkClient zookeeper, String dir) {
    this(zookeeper, dir, new Stats());
  }

  public ZkDistributedQueue(SolrZkClient zookeeper, String dir, Stats stats) {
    this(zookeeper, dir, stats, 0);
  }

  public ZkDistributedQueue(SolrZkClient zookeeper, String dir, Stats stats, int maxQueueSize) {
    this(zookeeper, dir, stats, maxQueueSize, null);
  }

  public ZkDistributedQueue(SolrZkClient zookeeper, String dir, Stats stats, int maxQueueSize, IsClosed higherLevelIsClosed) {
    this.dir = dir;

    this.zookeeper = zookeeper;
    this.stats = stats;
    this.maxQueueSize = maxQueueSize;
  }

  public void remove(Collection<String> paths) throws KeeperException, InterruptedException {
    if (paths.isEmpty()) return;
    List<Op> ops = new ArrayList<>();
    for (String path : paths) {
      ops.add(Op.delete(dir + "/" + path, -1));
    }
    for (int from = 0; from < ops.size(); from += 1000) {
      int to = Math.min(from + 1000, ops.size());
      if (from < to) {
        try {
          zookeeper.multi(ops.subList(from, to), true);
        } catch (KeeperException.NoNodeException e) {
          // don't know which nodes are not exist, so try to delete one by one node
          for (int j = from; j < to; j++) {
            try {
              zookeeper.delete(ops.get(j).getPath(), -1, true);
            } catch (KeeperException.NoNodeException e2) {
              if (log.isDebugEnabled()) {
                log.debug("Can not remove node which is not exist : {}", ops.get(j).getPath());
              }
            }
          }
        }
      }
    }
  }

  private static Set<String> OPERATIONS = new HashSet<>();
  static {
    OPERATIONS.add("state");
    OPERATIONS.add("leader");
    OPERATIONS.add(OverseerAction.DOWNNODE.toLower());
    OPERATIONS.add(OverseerAction.RECOVERYNODE.toLower());
    OPERATIONS.add("updateshardstate");
  }

  /**
   * Inserts data into queue.  If there are no other queue consumers, the offered element
   * will be immediately visible when this method returns.
   */
  @Override
  public void offer(byte[] data, boolean retryOnExpiration) throws KeeperException, InterruptedException {

    // TODO - if too many items on the queue, just block
    zookeeper.create(dir + "/" + PREFIX, data, CreateMode.PERSISTENT_SEQUENTIAL, true, retryOnExpiration);
  }

  @Override
  public void offer(byte[] data) throws KeeperException, InterruptedException {
    offer(data, true);
  }

  public Stats getZkStats() {
    return stats;
  }

  public SolrZkClient getZookeeper() {
    return zookeeper;
  }

  @Override
  public Map<String, Object> getStats() {
    if (stats == null) {
      return Collections.emptyMap();
    }
    Map<String, Object> res = new HashMap<>();
    res.put("queueLength", stats.getQueueLength());
    final Map<String, Object> statsMap = new HashMap<>();
    res.put("stats", statsMap);
    stats.getStats().forEach((op, stat) -> {
      final Map<String, Object> statMap = new HashMap<>();
      statMap.put("success", stat.success.get());
      statMap.put("errors", stat.errors.get());
      final List<Map<String, Object>> failed = new ArrayList<>(stat.failureDetails.size());
      statMap.put("failureDetails", failed);
      stat.failureDetails.forEach(failedOp -> {
        Map<String, Object> fo = new HashMap<>();
        fo.put("req", failedOp.req);
        fo.put("resp", failedOp.resp);
      });
      statsMap.put(op, statMap);
    });
    return res;
  }
}
