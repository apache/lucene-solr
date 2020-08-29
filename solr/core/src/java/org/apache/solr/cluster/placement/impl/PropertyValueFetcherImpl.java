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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cluster.placement.Node;
import org.apache.solr.cluster.placement.PropertyKey;
import org.apache.solr.cluster.placement.PropertyValue;
import org.apache.solr.cluster.placement.PropertyValueFetcher;
import org.apache.solr.cluster.placement.impl.propertykey.NodeSnitchPropertyKey;
import org.apache.solr.common.SolrException;

public class PropertyValueFetcherImpl implements PropertyValueFetcher {
  private final SolrCloudManager cloudManager;

  PropertyValueFetcherImpl(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
  }

  @Override
  public Map<PropertyKey, PropertyValue> fetchProperties(Set<PropertyKey> props) {
    // Convert the plugin view of what's to fetch into the underlying Solr representation
    Map<Node, Map<String, NodeSnitchPropertyKey>> nodeToTagsAndKeys = getTagsAndKeysByNode(props);
    // Fetch the data from the remote nodes
    Map<Node, Map<String, Object>> tagValues = getTagValuesForAllNodes(nodeToTagsAndKeys);
    // Convert fetched values to the plugin representation
    Map<PropertyKey, PropertyValue> keysToValues = convertToProperyValues(nodeToTagsAndKeys, tagValues);

    return keysToValues;
  }

  /**
   * Returns for each node from which {@link PropertyKey}'s need to be fetched, the corresponding tags to be used in
   * {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)} as well as which
   * {@link PropertyKey}'s they correspond to.
   */
  private Map<Node, Map<String, NodeSnitchPropertyKey>> getTagsAndKeysByNode(Set<PropertyKey> props) {
    // Map from a Node to a map from a tag name to the corresponding property key
    Map<Node, Map<String, NodeSnitchPropertyKey>> nodeToTagsAndKeys = new HashMap<>();

    for (PropertyKey propertyKey : props) {
      if (propertyKey instanceof NodeSnitchPropertyKey) {
        NodeSnitchPropertyKey nodeSnitchPropertyKey = (NodeSnitchPropertyKey) propertyKey;
        Node node = nodeSnitchPropertyKey.getPropertyValueSource();

        // We might want to limit acceptable nodes here. We could verify if they're in the set of live nodes or similar

        Map<String, NodeSnitchPropertyKey> tagsAndKeys = nodeToTagsAndKeys.get(node);
        if (tagsAndKeys == null) {
          tagsAndKeys = new HashMap<>();
          nodeToTagsAndKeys.put(node, tagsAndKeys);
        }

        String tag = nodeSnitchPropertyKey.getNodeSnitchTag();
        tagsAndKeys.put(tag, nodeSnitchPropertyKey);
      } else {
        // TODO support all keys...
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported property key " + propertyKey.getClass().getName());
      }
    }

    return nodeToTagsAndKeys;
  }

  /**
   * Gets the tag values from remote nodes for all nodes. Returns {@link Object}'s as returned by
   * {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)}.
   */
  private Map<Node, Map<String, Object>> getTagValuesForAllNodes(Map<Node, Map<String, NodeSnitchPropertyKey>> nodeToTagsAndKeys) {

    Map<Node, Map<String, Object>> nodeToTagValues = new HashMap<>(nodeToTagsAndKeys.size());

    // This implementation does a sequential fetch from all nodes. This will most likely have to be converted to
    // parallel fetching from all nodes concurrently.
    for (Map.Entry<Node, Map<String, NodeSnitchPropertyKey>> e : nodeToTagsAndKeys.entrySet()) {
      Node node = e.getKey();
      Map<String, Object> tagValues = cloudManager.getNodeStateProvider().getNodeValues(node.getNodeName(), e.getValue().keySet());
      if (tagValues != null) {
        nodeToTagValues.put(node, tagValues);
      }
    }

    return nodeToTagValues;
  }

  /**
   * Do the conversion of returned tag values (that are {@link Object}'s) into the strongly typed corresponding {@link PropertyValue}'s.
   * Returns a map with they keys being the original {@link PropertyKey}, for those properties that do have returned node values.
   */
  private Map<PropertyKey, PropertyValue> convertToProperyValues(Map<Node, Map<String, NodeSnitchPropertyKey>> nodeToTagsAndKeys, Map<Node, Map<String, Object>> nodeToTagValues) {
    Map<PropertyKey, PropertyValue> keysToValues = new HashMap<>();

    for (Map.Entry<Node, Map<String, Object>> e : nodeToTagValues.entrySet()) {
      // For each node in the results we have the map of returned tag node values (tagToNodeValue) as well as the map of requested
      // PropertyKey's (tagToPropertyKey).
      Node node = e.getKey();
      Map<String, Object> tagToNodeValue = e.getValue();
      Map<String, NodeSnitchPropertyKey> tagToPropertyKey = nodeToTagsAndKeys.get(node);

      // Iterating over the requested property keys we find the corresponding tag node value (if it exists) and use the
      // property key to cast that value into the correct type and return the corresponding PropertyValue.
      for (Map.Entry<String, NodeSnitchPropertyKey> tp : tagToPropertyKey.entrySet()) {
        NodeSnitchPropertyKey nodeSnitchPropertyKey = tp.getValue();
        Object nodeValue = tagToNodeValue.get(nodeSnitchPropertyKey.getNodeSnitchTag());
        if (nodeValue != null) {
          PropertyValue pv = nodeSnitchPropertyKey.getPropertyValueFromNodeValue(nodeValue);
          keysToValues.put(nodeSnitchPropertyKey, pv);
        }
      }
    }

    return keysToValues;
  }
}
