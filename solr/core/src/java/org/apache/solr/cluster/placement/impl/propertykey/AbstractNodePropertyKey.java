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

package org.apache.solr.cluster.placement.impl.propertykey;

import java.util.Collection;

import org.apache.solr.cluster.placement.Node;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;

/**
 * Superclass for all {@link org.apache.solr.cluster.placement.PropertyKey} that target a {@link Node} and whose implementation
 * is based on using {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)}.
 */
abstract class AbstractNodePropertyKey implements NodeSnitchPropertyKey {
  private final Node node;
  private final String snitchTag;

  AbstractNodePropertyKey(Node node, String snitchTag) {
    this.node = node;
    this.snitchTag = snitchTag;
  }

  @Override
  public Node getPropertyValueSource() {
    return node;
  }

  @Override
  public String getNodeSnitchTag() {
    return snitchTag;
  }

}
