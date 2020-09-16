package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class ClusterPropInfo implements ReflectMapWriter {

  @JsonProperty
  public String urlScheme;

  @JsonProperty
  public Integer maxCoresPerNode;
  @JsonProperty
  public String location;

  @JsonProperty
  public DefaultsInfo defaults;

  @JsonProperty
  public CollectionDefaults collectionDefaults;

  public static class CollectionDefaults implements ReflectMapWriter {
    @JsonProperty
    public Integer numShards;
    @JsonProperty
    public Integer tlogReplicas;
    @JsonProperty
    public Integer pullReplicas;
    @JsonProperty
    public Integer nrtReplicas;

  }

  public static class DefaultsInfo implements ReflectMapWriter {

    @JsonProperty
    public CollectionDefaults collection;

    @JsonProperty
    public CollectionDefaults cluster;

  }

  public static class ClusterInfo implements ReflectMapWriter {
    @JsonProperty
    public Boolean useLegacyReplicaAssignment;


    @JsonProperty
    public CollectionDefaults collection;

  }


}
