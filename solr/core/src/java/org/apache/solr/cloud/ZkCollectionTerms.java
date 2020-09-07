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

import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.core.CoreDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * Used to manage all ZkShardTerms of a collection
 */
class ZkCollectionTerms implements AutoCloseable {
  private final String collection;
  private final Map<String, ZkShardTerms> terms;
  private final SolrZkClient zkClient;
  private boolean closed;

  ZkCollectionTerms(String collection, SolrZkClient client) {
    this.collection = collection;
    this.terms = new HashMap<>();
    this.zkClient = client;
    ObjectReleaseTracker.track(this);
  }


  public ZkShardTerms getShard(String shardId) {
    synchronized (terms) {
      if (!terms.containsKey(shardId)) terms.put(shardId, new ZkShardTerms(collection, shardId, zkClient));
      return terms.get(shardId);
    }
  }

  public ZkShardTerms getShardOrNull(String shardId) {
    synchronized (terms) {
      if (!terms.containsKey(shardId)) return null;
      return terms.get(shardId);
    }
  }

  public void register(String shardId, String coreNodeName) {
    synchronized (terms)  {
      getShard(shardId).registerTerm(coreNodeName);
    }
  }

  public void remove(String shardId, CoreDescriptor coreDescriptor) {
    synchronized (terms) {
      ZkShardTerms zterms = getShardOrNull(shardId);
      if (zterms != null) {
        if (zterms.removeTerm(coreDescriptor)) {
          terms.remove(shardId).close();
        }
      }
    }
  }

  public void close() {
    synchronized (terms) {
      this.closed = true;
      terms.values().forEach(ZkShardTerms::close);
    }
    ObjectReleaseTracker.release(this);
  }

}
