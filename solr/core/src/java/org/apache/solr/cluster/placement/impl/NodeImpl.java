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

package org.apache.solr.cluster.placement.impl;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.cluster.placement.Node;
import org.apache.solr.cluster.placement.PropertyValueSource;

class NodeImpl implements Node {
  public final String nodeName;

  /**
   * Transforms a collection of node names into a set of {@link Node} instances.
   */
  static Set<Node> getNodes(Collection<String> nodeNames) {
    return nodeNames.stream().map(NodeImpl::new).collect(Collectors.toSet());
  }

  NodeImpl(String nodeName) {
    this.nodeName = nodeName;
  }

  @Override
  public String getNodeName() {
    return nodeName;
  }

  /**
   * This class implements {@link PropertyValueSource} and will end up as a key in a Map for fetching {@link org.apache.solr.cluster.placement.PropertyKey}'s.
   * It is important to implement this method comparing node names given that new instances of {@link Node} are created
   * with names equal to existing instances (See {@link ReplicaImpl} constructor).
   *
   */
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) { return false; }
    NodeImpl other = (NodeImpl) obj;
    return Objects.equals(this.nodeName, other.nodeName);
  }

  public int hashCode() {
    return Objects.hashCode(nodeName);
  }
}
