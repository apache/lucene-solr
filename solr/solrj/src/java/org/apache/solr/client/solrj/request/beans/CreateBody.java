package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

import java.util.List;
import java.util.Map;

public class CreateBody implements ReflectMapWriter {
    @JsonProperty(required = true)
    public String name;

    @JsonProperty
    public String config;

    @JsonProperty
    public RouterInfo router;

    @JsonProperty
    public Integer numShards;

    @JsonProperty
    public String shards;

    @JsonProperty
    public Integer replicationFactor;

    @JsonProperty
    public Integer nrtReplicas;

    @JsonProperty
    public Integer tlogReplicas;

    @JsonProperty
    public Integer pullReplicas;

    @JsonProperty
    public Integer maxShardsPerNode;

    @JsonProperty
    public List<String> nodeSet;

    @JsonProperty
    public Boolean shuffleNodes;

    @JsonProperty
    public Map<String, Object> properties;

    @JsonProperty
    public String async;

    @JsonProperty
    public Boolean waitForFinalState;

    @JsonProperty
    public Boolean perReplicaState;

    public static class RouterInfo implements ReflectMapWriter {
        @JsonProperty
        public String name;
        @JsonProperty
        public String field;
    }
}
