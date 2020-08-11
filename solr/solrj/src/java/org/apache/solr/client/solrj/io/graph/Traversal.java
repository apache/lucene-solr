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

package org.apache.solr.client.solrj.io.graph;

import org.apache.solr.client.solrj.io.Tuple;
import java.util.*;

public class Traversal {

  private List<Map<String, Node>> graph = new ArrayList<>();
  private List<String> fields = new ArrayList<>();
  private List<String> collections = new ArrayList<>();
  private Set<Scatter> scatter = new HashSet<>();
  private Set<String> collectionSet = new HashSet<>();
  private boolean trackTraversal;
  private int depth;

  public void addLevel(Map<String, Node> level, String collection, String field) {
    graph.add(level);
    collections.add(collection);
    collectionSet.add(collection);
    fields.add(field);
    ++depth;
  }

  public int getDepth() {
    return depth;
  }

  public boolean getTrackTraversal() {
    return this.trackTraversal;
  }

  public boolean visited(String nodeId, String ancestorId, Tuple tuple) {
    for(Map<String, Node> level : graph) {
      Node node = level.get(nodeId);
      if(node != null) {
        node.add(depth+"^"+ancestorId, tuple);
        return true;
      }
    }
    return false;
  }

  public boolean isMultiCollection() {
    return collectionSet.size() > 1;
  }

  public List<Map<String, Node>> getGraph() {
    return graph;
  }

  public void setScatter(Set<Scatter> scatter) {
    this.scatter = scatter;
  }

  public Set<Scatter> getScatter() {
    return this.scatter;
  }

  public void setTrackTraversal(boolean trackTraversal) {
    this.trackTraversal = trackTraversal;
  }

  public List<String> getCollections() {
    return this.collections;
  }

  public List<String> getFields() {
    return this.fields;
  }

  public enum Scatter {
    BRANCHES,
    LEAVES;
  }

  @SuppressWarnings({"unchecked"})
  public Iterator<Tuple> iterator() {
    return new TraversalIterator(this, scatter);
  }
}