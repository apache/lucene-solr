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
package org.apache.solr.common.cloud;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.noggit.JSONUtil;
import org.noggit.JSONWriter;

import static org.apache.solr.common.cloud.ZkStateReader.AUTO_ADD_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

/**
 * Models a Collection in zookeeper (but that Java name is obviously taken, hence "DocCollection")
 */
public class DocCollection extends ZkNodeProps {
  public static final String DOC_ROUTER = "router";
  public static final String SHARDS = "shards";
  public static final String STATE_FORMAT = "stateFormat";
  public static final String RULE = "rule";
  public static final String SNITCH = "snitch";
  private int znodeVersion = -1; // sentinel

  private final String name;
  private final Map<String, Slice> slices;
  private final Map<String, Slice> activeSlices;
  private final DocRouter router;
  private final String znode;

  private final Integer replicationFactor;
  private final Integer maxShardsPerNode;
  private final Boolean autoAddReplicas;


  public DocCollection(String name, Map<String, Slice> slices, Map<String, Object> props, DocRouter router) {
    this(name, slices, props, router, -1, ZkStateReader.CLUSTER_STATE);
  }

  /**
   * @param name  The name of the collection
   * @param slices The logical shards of the collection.  This is used directly and a copy is not made.
   * @param props  The properties of the slice.  This is used directly and a copy is not made.
   */
  public DocCollection(String name, Map<String, Slice> slices, Map<String, Object> props, DocRouter router, int zkVersion, String znode) {
    super(props==null ? props = new HashMap<String,Object>() : props);
    this.znodeVersion = zkVersion;
    this.name = name;

    this.slices = slices;
    this.activeSlices = new HashMap<>();
    this.replicationFactor = (Integer) verifyProp(props, REPLICATION_FACTOR);
    this.maxShardsPerNode = (Integer) verifyProp(props, MAX_SHARDS_PER_NODE);
    Boolean autoAddReplicas = (Boolean) verifyProp(props, AUTO_ADD_REPLICAS);
    this.autoAddReplicas = autoAddReplicas == null ? false : autoAddReplicas;
    verifyProp(props, RULE);
    verifyProp(props, SNITCH);
    Iterator<Map.Entry<String, Slice>> iter = slices.entrySet().iterator();

    while (iter.hasNext()) {
      Map.Entry<String, Slice> slice = iter.next();
      if (slice.getValue().getState() == Slice.State.ACTIVE)
        this.activeSlices.put(slice.getKey(), slice.getValue());
    }
    this.router = router;
    this.znode = znode == null? ZkStateReader.CLUSTER_STATE : znode;
    assert name != null && slices != null;
  }

  public static Object verifyProp(Map<String, Object> props, String propName) {
    Object o = props.get(propName);
    if (o == null) return null;
    switch (propName) {
      case MAX_SHARDS_PER_NODE:
      case REPLICATION_FACTOR:
        return Integer.parseInt(o.toString());
      case AUTO_ADD_REPLICAS:
        return Boolean.parseBoolean(o.toString());
      case "snitch":
      case "rule":
        return (List) o;
      default:
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unknown property " + propName);
    }

  }

  /**Use this to make an exact copy of DocCollection with a new set of Slices and every other property as is
   * @param slices the new set of Slices
   * @return the resulting DocCollection
   */
  public DocCollection copyWithSlices(Map<String, Slice> slices){
    return new DocCollection(getName(), slices, propMap, router, znodeVersion,znode);
  }

  /**
   * Return collection name.
   */
  public String getName() {
    return name;
  }

  public Slice getSlice(String sliceName) {
    return slices.get(sliceName);
  }

  /**
   * Gets the list of all slices for this collection.
   */
  public Collection<Slice> getSlices() {
    return slices.values();
  }


  /**
   * Return the list of active slices for this collection.
   */
  public Collection<Slice> getActiveSlices() {
    return activeSlices.values();
  }

  /**
   * Get the map of all slices (sliceName-&gt;Slice) for this collection.
   */
  public Map<String, Slice> getSlicesMap() {
    return slices;
  }

  /**
   * Get the map of active slices (sliceName-&gt;Slice) for this collection.
   */
  public Map<String, Slice> getActiveSlicesMap() {
    return activeSlices;
  }

  public int getZNodeVersion(){
    return znodeVersion;
  }

  public int getStateFormat() {
    return ZkStateReader.CLUSTER_STATE.equals(znode) ? 1 : 2;
  }
  /**
   * @return replication factor for this collection or null if no
   *         replication factor exists.
   */
  public Integer getReplicationFactor() {
    return replicationFactor;
  }
  
  public boolean getAutoAddReplicas() {
    return autoAddReplicas;
  }
  
  public int getMaxShardsPerNode() {
    if (maxShardsPerNode == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, MAX_SHARDS_PER_NODE + " is not in the cluster state.");
    }
    return maxShardsPerNode;
  }

  public String getZNode(){
    return znode;
  }


  public DocRouter getRouter() {
    return router;
  }

  @Override
  public String toString() {
    return "DocCollection("+name+")=" + JSONUtil.toJSON(this);
  }

  @Override
  public void write(JSONWriter jsonWriter) {
    LinkedHashMap<String, Object> all = new LinkedHashMap<>(slices.size() + 1);
    all.putAll(propMap);
    all.put(SHARDS, slices);
    jsonWriter.write(all);
  }

  public Replica getReplica(String coreNodeName) {
    for (Slice slice : slices.values()) {
      Replica replica = slice.getReplica(coreNodeName);
      if (replica != null) return replica;
    }
    return null;
  }
}
