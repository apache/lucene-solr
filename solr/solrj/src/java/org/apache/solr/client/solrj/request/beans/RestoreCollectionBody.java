package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

import java.util.List;
import java.util.Map;

/**
 * V2 API POJO for the /v2/collections 'restore-collection' command.
 *
 * Analogous to the request parameters for v1 /admin/collections?action=RESTORE API.
 */
public class RestoreCollectionBody implements ReflectMapWriter {

    @JsonProperty(required = true)
    public String collection;

    @JsonProperty(required = true)
    public String name;

    @JsonProperty
    public String location;

    @JsonProperty
    public String repository;

    @JsonProperty("create-collection")
    public Map<String, Object> createCollectionParams;

    @JsonProperty
    public String async;
}
