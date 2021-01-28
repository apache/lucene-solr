package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class DeleteAliasBody implements ReflectMapWriter {
    @JsonProperty(required = true)
    public String name;

    @JsonProperty
    public String async;
}
