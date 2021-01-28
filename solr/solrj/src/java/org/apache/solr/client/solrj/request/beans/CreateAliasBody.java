package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

import java.util.List;
import java.util.Map;

public class CreateAliasBody implements ReflectMapWriter {
    @JsonProperty(required = true)
    public String name;

    @JsonProperty
    public List<String> collections;

    @JsonProperty
    public AliasRouter router;

    @JsonProperty
    public String tz;

    @JsonProperty("create-collection")
    public Map<String, Object> createCollectionParams;

    @JsonProperty
    public String async;

    public static class AliasRouter implements ReflectMapWriter {
        @JsonProperty(required = true)
        public String name;

        @JsonProperty
        public String field;

        @JsonProperty
        public String interval;

        @JsonProperty
        public Integer maxFutureMs;

        @JsonProperty
        public String preemptiveCreateMath;

        @JsonProperty
        public String autoDeleteAge;

        @JsonProperty
        public Integer maxCardinality;

        @JsonProperty
        public String mustMatch;

        @JsonProperty
        public List<Map<String, Object>> routerList;
    }
}


