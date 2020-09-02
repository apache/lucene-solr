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

import com.google.common.collect.Maps;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cluster.placement.*;
import org.apache.solr.common.SolrException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class PropertyKeyFactoryImpl implements PropertyKeyFactory {
    @Override
    public PropertyKey.CoresCount createCoreCountKey(Node node) {
        return new AbstractNodePropertyKey.CoreCountImpl(node);
    }

    @Override
    public PropertyKey.FreeDisk createFreeDiskKey(Node node) {
        return new AbstractNodePropertyKey.FreeDiskImpl(node);
    }

    @Override
    public PropertyKey.TotalDisk createTotalDiskKey(Node node) {
        return new AbstractNodePropertyKey.TotalDiskImpl(node);
    }

    @Override
    public PropertyKey.DiskType createDiskTypeKey(Node node) {
        return new AbstractNodePropertyKey.DiskTypeImpl(node);
    }

    @Override
    public PropertyKey.Sysprop createSyspropKey(Node node, String syspropName) {
        return new AbstractNodePropertyKey.SyspropImpl(node, syspropName);
    }

    @Override
    public Map<Node, PropertyKey> createSyspropKeys(Set<Node> nodes, String syspropName) {
        Map<Node, PropertyKey> keys = Maps.newHashMap();
        for (Node n : nodes) {
            keys.put(n, createSyspropKey(n, syspropName));
        }
        return keys;
    }

    @Override
    public PropertyKey.Metric createMetricKey(Node nodeMetricSource, String metricName, NodeMetricRegistry registry) {
        return new AbstractNodePropertyKey.NodeMetricImpl(nodeMetricSource, metricName, registry);
    }

    @Override
    public PropertyKey.SystemLoad createSystemLoadKey(Node node) {
        return new AbstractNodePropertyKey.SystemLoadImpl(node);
    }

    @Override
    public PropertyKey.HeapUsage createHeapUsageKey(Node node) {
        return new AbstractNodePropertyKey.HeapUsageImpl(node);
    }

    @Override
    public PropertyKey.Metric createMetricKey(PropertyValueSource metricSource, String metricName) {
        return new NonNodeMetricKeyImpl(metricSource, metricName);
    }
}


class PlacementPlanFactoryImpl implements PlacementPlanFactory {
    @Override
    public PlacementPlan createPlacementPlanAddReplicas(AddReplicasPlacementRequest request, Set<ReplicaPlacement> replicaPlacements) {
        return new PlacementPlanAddReplicasImpl(request, replicaPlacements);
    }

    @Override
    public ReplicaPlacement createReplicaPlacement(String shardName, Node node, Replica.ReplicaType replicaType) {
        return new ReplicaPlacementImpl(shardName, node, replicaType);
    }
}

class PropertyValueFetcherImpl implements PropertyValueFetcher {
    private final SolrCloudManager cloudManager;

    PropertyValueFetcherImpl(SolrCloudManager cloudManager) {
        this.cloudManager = cloudManager;
    }

    @Override
    public void fetchProperties(Set<PropertyKey> props) {
        // Convert the plugin view of what's to fetch into the underlying Solr representation
        Map<Node, Map<String, AbstractNodePropertyKey>> nodeToTagsAndKeys = getTagsAndKeysByNode(props);
        // Fetch the data from the remote nodes
        Map<Node, Map<String, Object>> tagValues = getTagValuesForAllNodes(nodeToTagsAndKeys);
        // Set the fetched values on the keys (reset to empty keys for which no values)
        setFetchedValues(nodeToTagsAndKeys, tagValues);
    }

    /**
     * Returns for each node from which {@link PropertyKey}'s need to be fetched, the corresponding tags to be used in
     * {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)} as well as which
     * {@link PropertyKey}'s they correspond to.
     */
    private Map<Node, Map<String, AbstractNodePropertyKey>> getTagsAndKeysByNode(Set<PropertyKey> props) {
        // Map from a Node to a map from a tag name to the corresponding property key
        Map<Node, Map<String, AbstractNodePropertyKey>> nodeToTagsAndKeys = new HashMap<>();

        for (PropertyKey propertyKey : props) {
            if (propertyKey instanceof AbstractNodePropertyKey) {
                AbstractNodePropertyKey AbstractNodePropertyKey = (AbstractNodePropertyKey) propertyKey;
                Node node = AbstractNodePropertyKey.getPropertyValueSource();

                // We might want to limit acceptable nodes here. We could verify if they're in the set of live nodes or similar

                Map<String, AbstractNodePropertyKey> tagsAndKeys = nodeToTagsAndKeys.get(node);
                if (tagsAndKeys == null) {
                    tagsAndKeys = new HashMap<>();
                    nodeToTagsAndKeys.put(node, tagsAndKeys);
                }

                String tag = AbstractNodePropertyKey.getNodeSnitchTag();
                tagsAndKeys.put(tag, AbstractNodePropertyKey);
            } else {
                // TODO support all types of property keys...
                // Note that supporting non node property keys has a much larger impact than this method
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported property key " + propertyKey.getClass().getName());
            }
        }
        return nodeToTagsAndKeys;
    }

    /**
     * Gets the tag values from remote nodes for all nodes. Returns {@link Object}'s as returned by
     * {@link org.apache.solr.client.solrj.cloud.NodeStateProvider#getNodeValues(String, Collection)}.
     */
    private Map<Node, Map<String, Object>> getTagValuesForAllNodes(Map<Node, Map<String, AbstractNodePropertyKey>> nodeToTagsAndKeys) {

        Map<Node, Map<String, Object>> nodeToTagValues = new HashMap<>(nodeToTagsAndKeys.size());

        // This implementation does a sequential fetch from all nodes. This will most likely have to be converted to
        // parallel fetching from all nodes concurrently.
        for (Map.Entry<Node, Map<String, AbstractNodePropertyKey>> e : nodeToTagsAndKeys.entrySet()) {
            Node node = e.getKey();
            Map<String, Object> tagValues = cloudManager.getNodeStateProvider().getNodeValues(node.getName(), e.getValue().keySet());
            if (tagValues != null) {
                nodeToTagValues.put(node, tagValues);
            }
        }

        return nodeToTagValues;
    }

    /**
     * Set the returned tag values (that are {@link Object}'s) on the corresponding {@link AbstractNodePropertyKey}'s.
     */
    private void setFetchedValues(Map<Node, Map<String, AbstractNodePropertyKey>> nodeToTagsAndKeys, Map<Node, Map<String, Object>> nodeToTagValues) {
        for (Map.Entry<Node, Map<String, Object>> e : nodeToTagValues.entrySet()) {
            // For each node in the results we have the map of returned tag node values (tagToNodeValue) as well as the map of requested
            // PropertyKey's (tagToPropertyKey).
            Node node = e.getKey();
            Map<String, Object> tagToNodeValue = e.getValue();
            Map<String, AbstractNodePropertyKey> tagToPropertyKey = nodeToTagsAndKeys.get(node);

            // Iterating over the requested property keys we find the corresponding tag node value (if it exists) and set
            // the value on the property key. If no value found, we reset the property key so it appears empty (even though
            // it's initially empty, set to empty just in case it was fetched successfully a first time and now not).
            for (Map.Entry<String, AbstractNodePropertyKey> tp : tagToPropertyKey.entrySet()) {
                AbstractNodePropertyKey abstractNodePropertyKey = tp.getValue();
                Object nodeValue = tagToNodeValue.get(abstractNodePropertyKey.getNodeSnitchTag());
                if (nodeValue != null) {
                    abstractNodePropertyKey.setValue(nodeValue);
                } else {
                    abstractNodePropertyKey.setEmpty();
                }
            }
        }
    }
}
