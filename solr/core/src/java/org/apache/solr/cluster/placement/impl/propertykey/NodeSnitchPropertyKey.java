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
import org.apache.solr.cluster.placement.PropertyKey;
import org.apache.solr.cluster.placement.PropertyValue;

/**
 * {@link org.apache.solr.cluster.placement.PropertyKey} that have a {@link org.apache.solr.cluster.placement.Node} as their
 * {@link org.apache.solr.cluster.placement.PropertyValueSource} and whose internal implementation for retrieving the value
 * uses {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)} implement this interface.
 */
public interface NodeSnitchPropertyKey extends PropertyKey {
  /**
   * @return the tag corresponding to this instance of {@link PropertyKey} that can be used to fetch the value from a
   * {@link org.apache.solr.cluster.placement.Node} using {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)}.
   * It is a design decision to do a 1:1 correspondence between {@link PropertyKey} and snitches so that each returned
   * {@link org.apache.solr.cluster.placement.PropertyValue} is complete (if it were to be assembled from multiple snitches
   * we'd have to deal with some snitches being returned and some not).
   */
  String getNodeSnitchTag();

  /**
   * Given the object returned by {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)}
   * for the tag {@link #getNodeSnitchTag()}, builds the appropriate {@link PropertyValue} representing that value.
   * @param nodeValue the value to convert. Is never {@code null}.
   */
  PropertyValue getPropertyValueFromNodeValue(Object nodeValue);

  @Override
  Node getPropertyValueSource();
}
