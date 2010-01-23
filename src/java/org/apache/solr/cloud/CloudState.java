package org.apache.solr.cloud;

/**
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// effectively immutable
public class CloudState {
  protected static Logger log = LoggerFactory.getLogger(CloudState.class);
  
  private Map<String,Map<String,Slice>> collectionStates = new HashMap<String,Map<String,Slice>>();
  private Set<String> liveNodes = null;
  
  public CloudState(Set<String> liveNodes) {
    this.liveNodes = liveNodes;
  }
  
  // nocommit : only call before publishing
  void addSlices(String collection, Map<String,Slice> slices) {
    collectionStates.put(collection, slices);
  }
  
  // nocommit
  public Map<String,Slice> getSlices(String collection) {
    Map<String,Slice> collectionState = collectionStates.get(collection);
    if(collectionState == null) {
      log.error("Could not find cloud state for collection:" + collection);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Could not find cloud state for collection:" + collection);
    }
    return Collections.unmodifiableMap(collectionState);
  }
  
  public Set<String> getLiveNodes() {
    return Collections.unmodifiableSet(liveNodes);
  }
  
  public boolean liveNodesContain(String name) {
    return liveNodes.contains(name);
  }

}
