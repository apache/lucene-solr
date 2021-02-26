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

package org.apache.solr.client.solrj.request.beans;

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class ClusterPropPayload implements ReflectMapWriter {

  @JsonProperty
  public String urlScheme;

  @JsonProperty
  public Integer maxCoresPerNode;
  @JsonProperty
  public String location;

  @JsonProperty
  public Defaults defaults;

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

  public static class Defaults implements ReflectMapWriter {

    @JsonProperty
    public CollectionDefaults collection;

    @JsonProperty
    public Cluster cluster;

  }

  public static class Cluster implements ReflectMapWriter {
    @JsonProperty
    public Boolean useLegacyReplicaAssignment;


    @JsonProperty
    public CollectionDefaults collection;

  }


}
