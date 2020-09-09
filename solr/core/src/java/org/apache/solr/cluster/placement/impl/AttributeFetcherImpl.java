package org.apache.solr.cluster.placement.impl;

import com.google.common.collect.Maps;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.placement.Node;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.BiConsumer;

public class AttributeFetcherImpl implements AttributeFetcher {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    boolean requestedNodeCoreCount;
    boolean requestedNodeDiskType;
    boolean requestedNodeFreeDisk;
    boolean requestedNodeTotalDisk;
    boolean requestedNodeHeapUsage;
    boolean requestedNodeSystemLoadAverage;
    Set<String> requestedNodeSystemPropertiesSnitchTags = new HashSet<>();
    Set<String> requestedNodeMetricSnitchTags  = new HashSet<>();

    Set<Node> nodes = Collections.emptySet();

    private final SolrCloudManager cloudManager;

    AttributeFetcherImpl(SolrCloudManager cloudManager) {
        this.cloudManager = cloudManager;
    }

    @Override
    public AttributeFetcher requestNodeCoreCount() {
        requestedNodeCoreCount = true;
        return this;
    }

    @Override
    public AttributeFetcher requestNodeDiskType() {
        requestedNodeDiskType = true;
        return this;
    }

    @Override
    public AttributeFetcher requestNodeFreeDisk() {
        requestedNodeFreeDisk = true;
        return this;
    }

    @Override
    public AttributeFetcher requestNodeTotalDisk() {
        requestedNodeTotalDisk = true;
        return this;
    }

    @Override
    public AttributeFetcher requestNodeHeapUsage() {
        requestedNodeHeapUsage = true;
        return this;
    }

    @Override
    public AttributeFetcher requestNodeSystemLoadAverage() {
        requestedNodeSystemLoadAverage = true;
        return this;
    }

    @Override
    public AttributeFetcher requestNodeSystemProperty(String name) {
        requestedNodeSystemPropertiesSnitchTags.add(getSystemPropertySnitchTag(name));
        return this;
    }

    @Override
    public AttributeFetcher requestNodeEnvironmentVariable(String name) {
        throw new UnsupportedOperationException("Not yet implemented...");
    }

    @Override
    public AttributeFetcher requestNodeMetric(String metricName, NodeMetricRegistry registry) {
        requestedNodeMetricSnitchTags.add(getMetricSnitchTag(metricName, registry));
        return this;
    }

    @Override
    public AttributeFetcher fetchFrom(Set<Node> nodes) {
        this.nodes = nodes;
        return this;
    }

    @Override
    public AttributeFetcher requestMetric(String scope, String metricName) {
        throw new UnsupportedOperationException("Not yet implemented...");
    }

    @Override
    public AttributeValues fetchAttributes() {
        // TODO Code here only supports node related attributes for now

        // Maps in which attribute values will be added
        Map<Node, Integer> nodeToCoreCount = Maps.newHashMap();
        Map<Node, DiskHardwareType> nodeToDiskType = Maps.newHashMap();
        Map<Node, Long> nodeToFreeDisk = Maps.newHashMap();
        Map<Node, Long> nodeToTotalDisk = Maps.newHashMap();
        Map<Node, Double> nodeToHeapUsage = Maps.newHashMap();
        Map<Node, Double> nodeToSystemLoadAverage = Maps.newHashMap();
        Map<String, Map<Node, String>> syspropSnitchToNodeToValue = Maps.newHashMap();
        Map<String, Map<Node, Double>> metricSnitchToNodeToValue = Maps.newHashMap();

        // In order to match the returned values for the various snitches, we need to keep track of where each
        // received value goes. Given the target maps are of different types (the maps from Node to whatever defined
        // above) we instead pass a function taking two arguments, the node and the (non null) returned value,
        // that will cast the value into the appropriate type for the snitch tag and insert it into the appropriate map
        // with the node as the key.
        Map<String, BiConsumer<Node, Object>> allSnitchTagsToInsertion = Maps.newHashMap();
        if (requestedNodeCoreCount) {
            allSnitchTagsToInsertion.put(ImplicitSnitch.CORES, (node, value) -> nodeToCoreCount.put(node, ((Number) value).intValue()));
        }
        if (requestedNodeDiskType) {
            allSnitchTagsToInsertion.put(ImplicitSnitch.DISKTYPE, (node, value) -> {
                if ("rotational".equals(value)) {
                    nodeToDiskType.put(node, DiskHardwareType.ROTATIONAL);
                } else if ("ssd".equals(value)) {
                    nodeToDiskType.put(node, DiskHardwareType.SSD);
                }
                // unknown disk type: insert no value, returned optional will be empty
            });
        }
        if (requestedNodeFreeDisk) {
            allSnitchTagsToInsertion.put(SolrClientNodeStateProvider.Variable.FREEDISK.tagName,
                    (node, value) -> nodeToFreeDisk.put(node, ((Number) value).longValue()));
        }
        if (requestedNodeTotalDisk) {
            allSnitchTagsToInsertion.put(SolrClientNodeStateProvider.Variable.TOTALDISK.tagName,
                    (node, value) -> nodeToTotalDisk.put(node, ((Number) value).longValue()));
        }
        if (requestedNodeHeapUsage) {
            allSnitchTagsToInsertion.put(ImplicitSnitch.HEAPUSAGE,
                    (node, value) -> nodeToHeapUsage.put(node, ((Number) value).doubleValue()));
        }
        if (requestedNodeSystemLoadAverage) {
            allSnitchTagsToInsertion.put(ImplicitSnitch.SYSLOADAVG,
                    (node, value) -> nodeToSystemLoadAverage.put(node, ((Number) value).doubleValue()));
        }
        for (String sysPropSnitch : requestedNodeSystemPropertiesSnitchTags) {
            final Map<Node, String> sysPropMap = Maps.newHashMap();
            syspropSnitchToNodeToValue.put(sysPropSnitch, sysPropMap);
            allSnitchTagsToInsertion.put(sysPropSnitch, (node, value) -> sysPropMap.put(node, (String) value));
        }
        for (String metricSnitch : requestedNodeMetricSnitchTags) {
            final Map<Node, Double> metricMap = Maps.newHashMap();
            metricSnitchToNodeToValue.put(metricSnitch, metricMap);
            allSnitchTagsToInsertion.put(metricSnitch, (node, value) -> metricMap.put(node, (Double) value));
        }

        // Now that we know everything we need to fetch (and where to put it), just do it.
        for (Node node : nodes) {
            Map<String, Object> tagValues = cloudManager.getNodeStateProvider().getNodeValues(node.getName(), allSnitchTagsToInsertion.keySet());
            for (Map.Entry<String, Object> e : tagValues.entrySet()) {
                String tag = e.getKey();
                Object value = e.getValue(); // returned value from the node

                BiConsumer<Node, Object> inserter = allSnitchTagsToInsertion.get(tag);
                // If inserter is null it's a return of a tag that we didn't request
                if (inserter != null) {
                    inserter.accept(node, value);
                } else {
                    log.error("Received unsolicited snitch tag {} from node {}", tag, node);
                }
            }
        }

        return new AttributeValuesImpl(nodeToCoreCount,
                                       nodeToDiskType,
                                       nodeToFreeDisk,
                                       nodeToTotalDisk,
                                       nodeToHeapUsage,
                                       nodeToSystemLoadAverage,
                                       syspropSnitchToNodeToValue,
                                       metricSnitchToNodeToValue);
    }

    private static SolrInfoBean.Group getGroupFromMetricRegistry(NodeMetricRegistry registry) {
        switch (registry) {
            case SOLR_JVM:
                return SolrInfoBean.Group.jvm;
            case SOLR_NODE:
                return SolrInfoBean.Group.node;
            default:
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported registry value " + registry);
        }
    }

    static String getMetricSnitchTag(String metricName, NodeMetricRegistry registry) {
        return SolrClientNodeStateProvider.METRICS_PREFIX + SolrMetricManager.getRegistryName(getGroupFromMetricRegistry(registry), metricName);
    }

    static String getSystemPropertySnitchTag(String name) {
        return ImplicitSnitch.SYSPROP + name;
    }
}
