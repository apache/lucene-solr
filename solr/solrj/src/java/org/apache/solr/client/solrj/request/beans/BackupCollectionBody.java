package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

/**
 * V2 API POJO for the /v2/collections 'backup-collection' command.
 *
 * Analogous to the request parameters for v1 /admin/collections?action=BACKUP API.
 */
public class BackupCollectionBody implements ReflectMapWriter {
    @JsonProperty(required = true)
    public String collection;

    @JsonProperty(required = true)
    public String name;

    @JsonProperty
    public String location;

    @JsonProperty
    public String repository;

    @JsonProperty
    public Boolean followAliases;

    @JsonProperty
    public String indexBackup;

    @JsonProperty
    public String commitName;

    @JsonProperty
    public String async;
}
