package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

import java.util.Map;

public class SetAliasPropertyBody implements ReflectMapWriter {
    @JsonProperty(required = true)
    public String name;

    @JsonProperty
    public String async;

    @JsonProperty
    public Map<String, Object> properties;
}
