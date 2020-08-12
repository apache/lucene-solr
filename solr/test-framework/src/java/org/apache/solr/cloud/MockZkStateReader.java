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

import java.util.Set;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollectionWatcher;
import org.apache.solr.common.cloud.ZkStateReader;

// does not yet mock zkclient at all
public class MockZkStateReader extends ZkStateReader {

  private Set<String> collections;

  public MockZkStateReader(ClusterState clusterState, Set<String> collections) {
    super(new MockSolrZkClient());
    this.clusterState = clusterState;
    this.collections = collections;
  }
  
  public Set<String> getAllCollections(){
    return collections;
  }

  @Override
  public void registerDocCollectionWatcher(String collection, DocCollectionWatcher stateWatcher) {
    // the doc collection will never be changed by this mock
    // so we just call onStateChanged once with the existing DocCollection object an return
    stateWatcher.onStateChanged(clusterState.getCollectionOrNull(collection));
  }
}
