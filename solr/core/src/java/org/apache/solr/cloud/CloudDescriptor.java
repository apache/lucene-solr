package org.apache.solr.cloud;

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

import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.util.PropertiesUtil;

import java.util.Properties;

public class CloudDescriptor {

  private final CoreDescriptor cd;
  private String shardId;
  private String collectionName;
  private SolrParams params;
  private String roles = null;
  private Integer numShards;
  private String nodeName = null;

  /* shardRange and shardState are used once-only during sub shard creation for shard splits
   * Use the values from {@link Slice} instead */
  volatile String shardRange = null;
  volatile String shardState = Slice.ACTIVE;
  volatile String shardParent = null;

  volatile boolean isLeader = false;
  volatile String lastPublished = ZkStateReader.ACTIVE;

  public static final String NUM_SHARDS = "numShards";

  public CloudDescriptor(String coreName, Properties props, CoreDescriptor cd) {
    this.cd = cd;
    this.shardId = props.getProperty(CoreDescriptor.CORE_SHARD, null);
    // If no collection name is specified, we default to the core name
    this.collectionName = props.getProperty(CoreDescriptor.CORE_COLLECTION, coreName);
    this.roles = props.getProperty(CoreDescriptor.CORE_ROLES, null);
    this.nodeName = props.getProperty(CoreDescriptor.CORE_NODE_NAME);
    this.numShards = PropertiesUtil.toInteger(props.getProperty(CloudDescriptor.NUM_SHARDS), null);
  }
  
  public String getLastPublished() {
    return lastPublished;
  }

  public boolean isLeader() {
    return isLeader;
  }
  
  public void setLeader(boolean isLeader) {
    this.isLeader = isLeader;
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
  public SolrParams getParams() {
    return params;
  }

  public void setParams(SolrParams params) {
    this.params = params;
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
}
