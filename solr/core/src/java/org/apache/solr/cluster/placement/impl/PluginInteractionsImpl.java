package org.apache.solr.cluster.placement.impl;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cluster.placement.*;
import org.apache.solr.common.SolrException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class PropertyKeyFactoryImpl implements PropertyKeyFactory {
    @Override
    public CoreCountKeyImpl createCoreCountKey(Node node) {
        return new CoreCountKeyImpl(node);
    }

    @Override
    public FreeDiskKeyImpl createFreeDiskKey(Node node) {
        return new FreeDiskKeyImpl(node);
    }

    @Override
    public TotalDiskKeyImpl createTotalDiskKey(Node node) {
        return new TotalDiskKeyImpl(node);
    }

    @Override
    public DiskTypeKeyImpl createDiskTypeKey(Node node) {
        return new DiskTypeKeyImpl(node);
    }

    @Override
    public SyspropKeyImpl createSyspropKey(Node node, String syspropName) {
        return new SyspropKeyImpl(node, syspropName);
    }

    @Override
    public NonNodeMetricKeyImpl createMetricKey(PropertyValueSource metricSource, String metricName) {
        return new NonNodeMetricKeyImpl(metricSource, metricName);
    }

    @Override
    public NodeMetricKeyImpl createMetricKey(Node nodeMetricSource, String metricName, NodeMetricRegistry registry) {
        return new NodeMetricKeyImpl(nodeMetricSource, metricName, registry);
    }

    @Override
    public SystemLoadKeyImpl createSystemLoadKey(Node node) {
        return new SystemLoadKeyImpl(node);
    }

    @Override
    public HeapUsageKeyImpl createHeapUsageKey(Node node) {
        return new HeapUsageKeyImpl(node);
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
    public Map<PropertyKey, PropertyValue> fetchProperties(Set<PropertyKey> props) {
        // Convert the plugin view of what's to fetch into the underlying Solr representation
        Map<Node, Map<String, AbstractNodePropertyKey>> nodeToTagsAndKeys = getTagsAndKeysByNode(props);
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
    private Map<Node, Map<String, Object>> getTagValuesForAllNodes(Map<Node, Map<String, AbstractNodePropertyKey>> nodeToTagsAndKeys) {

        Map<Node, Map<String, Object>> nodeToTagValues = new HashMap<>(nodeToTagsAndKeys.size());

        // This implementation does a sequential fetch from all nodes. This will most likely have to be converted to
        // parallel fetching from all nodes concurrently.
        for (Map.Entry<Node, Map<String, AbstractNodePropertyKey>> e : nodeToTagsAndKeys.entrySet()) {
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
    private Map<PropertyKey, PropertyValue> convertToProperyValues(Map<Node, Map<String, AbstractNodePropertyKey>> nodeToTagsAndKeys, Map<Node, Map<String, Object>> nodeToTagValues) {
        Map<PropertyKey, PropertyValue> keysToValues = new HashMap<>();

        for (Map.Entry<Node, Map<String, Object>> e : nodeToTagValues.entrySet()) {
            // For each node in the results we have the map of returned tag node values (tagToNodeValue) as well as the map of requested
            // PropertyKey's (tagToPropertyKey).
            Node node = e.getKey();
            Map<String, Object> tagToNodeValue = e.getValue();
            Map<String, AbstractNodePropertyKey> tagToPropertyKey = nodeToTagsAndKeys.get(node);

            // Iterating over the requested property keys we find the corresponding tag node value (if it exists) and use the
            // property key to cast that value into the correct type and return the corresponding PropertyValue.
            for (Map.Entry<String, AbstractNodePropertyKey> tp : tagToPropertyKey.entrySet()) {
                AbstractNodePropertyKey AbstractNodePropertyKey = tp.getValue();
                Object nodeValue = tagToNodeValue.get(AbstractNodePropertyKey.getNodeSnitchTag());
                if (nodeValue != null) {
                    PropertyValue pv = AbstractNodePropertyKey.getPropertyValueFromNodeValue(nodeValue);
                    keysToValues.put(AbstractNodePropertyKey, pv);
                }
            }
        }

        return keysToValues;
    }
}
