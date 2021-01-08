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
package org.apache.solr.cloud;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Strings;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.common.util.PropertiesUtil;

/**
 * SolrCloud metadata attached to a {@link CoreDescriptor}.
 */
public class CloudDescriptor {

  private final CoreDescriptor cd; // back-reference

  private String shardId;
  private String collectionName;
  private String roles = null;
  private Integer numShards;
  private String nodeName = null;
  private Map<String,String> collectionParams = new HashMap<>();

  private volatile boolean isLeader = false;
  
  // set to true once a core has registered in zk
  // set to false on detecting a session expiration
  private volatile boolean hasRegistered = false;
  private volatile Replica.State lastPublished = Replica.State.ACTIVE;

  public static final String NUM_SHARDS = "numShards";
  
  public static final String REPLICA_TYPE = "replicaType";
  
  /**
   * The type of replica this core hosts
   */
  private final Replica.Type replicaType;

  public CloudDescriptor(CoreDescriptor cd, String coreName, Properties props) {
    this.cd = cd;
    this.shardId = props.getProperty(CoreDescriptor.CORE_SHARD, null);
    if (Strings.isNullOrEmpty(shardId))
      this.shardId = null;
    // If no collection name is specified, we default to the core name
    this.collectionName = props.getProperty(CoreDescriptor.CORE_COLLECTION, coreName);
    this.roles = props.getProperty(CoreDescriptor.CORE_ROLES, null);
    this.nodeName = props.getProperty(CoreDescriptor.CORE_NODE_NAME);
    if (Strings.isNullOrEmpty(nodeName))
      this.nodeName = null;
    this.numShards = PropertiesUtil.toInteger(props.getProperty(CloudDescriptor.NUM_SHARDS), null);
    String replicaTypeStr = props.getProperty(CloudDescriptor.REPLICA_TYPE);
    if (Strings.isNullOrEmpty(replicaTypeStr)) {
      this.replicaType = Replica.Type.NRT;
    } else {
      this.replicaType = Replica.Type.valueOf(replicaTypeStr);
    }
    for (String propName : props.stringPropertyNames()) {
      if (propName.startsWith(ZkController.COLLECTION_PARAM_PREFIX)) {
        collectionParams.put(propName.substring(ZkController.COLLECTION_PARAM_PREFIX.length()), props.getProperty(propName));
      }
    }
  }
  
  public boolean requiresTransactionLog() {
    return this.replicaType != Replica.Type.PULL;
  }
  
  public Replica.State getLastPublished() {
    return lastPublished;
  }

  public void setLastPublished(Replica.State state) {
    lastPublished = state;
  }

  public boolean isLeader() {
    return isLeader;
  }
  
  public void setLeader(boolean isLeader) {
    this.isLeader = isLeader;
  }
  
  public boolean hasRegistered() {
    return hasRegistered;
  }
  
  public void setHasRegistered(boolean hasRegistered) {
    this.hasRegistered = hasRegistered;
  }

  public void setShardId(String shardId) {
    this.shardId = shardId;
  }
  
  public String getShardId() {
    return shardId;
  }
  
  public String getCollectionName() {
    return collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  public String getRoles(){
    return roles;
  }
  
  public void setRoles(String roles){
    this.roles = roles;
  }
  
  /** Optional parameters that can change how a core is created. */
  public Map<String, String> getParams() {
    return collectionParams;
  }

  // setting only matters on core creation
  public Integer getNumShards() {
    return numShards;
  }
  
  public void setNumShards(int numShards) {
    this.numShards = numShards;
  }
  
  public String getCoreNodeName() {
    return nodeName;
  }

  public void setCoreNodeName(String nodeName) {
    this.nodeName = nodeName;
    if(nodeName==null) cd.getPersistableStandardProperties().remove(CoreDescriptor.CORE_NODE_NAME);
    else cd.getPersistableStandardProperties().setProperty(CoreDescriptor.CORE_NODE_NAME, nodeName);
  }

  public void reload(CloudDescriptor reloadFrom) {
    if (reloadFrom == null) return;

    setShardId(StringUtils.isEmpty(reloadFrom.getShardId()) ? getShardId() : reloadFrom.getShardId());
    setCollectionName(StringUtils.isEmpty(reloadFrom.getCollectionName()) ? getCollectionName() : reloadFrom.getCollectionName());
    setRoles(StringUtils.isEmpty(reloadFrom.getRoles()) ? getRoles() : reloadFrom.getRoles());
    if (reloadFrom.getNumShards() != null) {
      setNumShards(reloadFrom.getNumShards());
    }
    setCoreNodeName(StringUtils.isEmpty(reloadFrom.getCoreNodeName()) ? getCoreNodeName() : reloadFrom.getCoreNodeName());
    setLeader(reloadFrom.isLeader);
    setHasRegistered(reloadFrom.hasRegistered);
    setLastPublished(reloadFrom.getLastPublished());

    for (Map.Entry<String, String> ent : reloadFrom.getParams().entrySet()) {
      collectionParams.put(ent.getKey(), ent.getValue());
    }
  }

  public Replica.Type getReplicaType() {
    return replicaType;
  }
}
