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
import java.util.List;
import java.util.Map;

// effectively immutable
public class CloudState {
  private Map<String,Slices> collectionStates = new HashMap<String,Slices>();
  private List<String> nodes = null;
  
  // nocommit
  public void addSlices(String collection, Slices slices) {
    collectionStates.put(collection, slices);
  }
  
  // nocommit
  public Slices getSlices(String collection) {
    return collectionStates.get(collection);
  }
  
  public List<String> getNodes() {
    return Collections.unmodifiableList(nodes);
  }
  
  // nocommit
  public void setNodes(List<String> nodes) {
    this.nodes = nodes;
  }
}
