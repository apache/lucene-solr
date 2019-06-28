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
package org.apache.solr.store.blob.metadata;

import org.apache.solr.store.shared.metadata.SharedShardMetadataController;

/*
 * Class containing information needed to complete a push or a pull to a shared store in 
 * a Shared Collection.
 */
public class PushPullData {
  
  protected String collectionName;
  protected String shardName;
  protected String coreName;
  
  /*
   * Identifier unique for a Shared Collection's shard data as stored on the shared store
   */
  protected String sharedStoreName;
  
  /**
   * Unique value of the metadataSuffix for the last persisted shard index in the shared store.
   * If this value is equal to {@link SharedShardMetadataController#METADATA_NODE_DEFAULT_VALUE} then 
   * the shard index this instance is associated with has not yet been successfully persisted
   */
  protected String lastReadMetadataSuffix;
  
  /*
   * Unique value of the metadataSuffix that will be appended to the core metadata file name if this 
   * PushPullData instance is association with a push operation to the shared store.
   */
  protected String newMetadataSuffix;
  
  /*
   * Value originating from a ZooKeeper node used to handle conditionally and safely update the 
   * core.metadata file written to the shared store.
   */
  protected int version;
  
  public PushPullData() {}
  
  public PushPullData(String collectionName, String shardName, String coreName, String sharedStoreName, String lastReadMetadataSuffix,
      String newMetadataSuffix, int version) {
    this.collectionName = collectionName;
    this.shardName = shardName;
    this.coreName = coreName;
    this.sharedStoreName = sharedStoreName;
    this.lastReadMetadataSuffix = lastReadMetadataSuffix;
    this.newMetadataSuffix = newMetadataSuffix;
    this.version = version;
  }

  public String getCoreName() {
    return coreName;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public String getShardName() {
    return shardName;
  }
  
  public String getSharedStoreName() {
    return sharedStoreName;
  }

  public String getLastReadMetadataSuffix() {
    return lastReadMetadataSuffix;
  }
  
  public String getNewMetadataSuffix() {
    return newMetadataSuffix;
  }
  
  public int getZkVersion() {
    return version;
  }
  
  @Override
  public String toString() {
    return "collectionName=" + collectionName + " shardName=" + shardName + " coreName=" + 
        sharedStoreName + " coreName=" + coreName + " lastReadMetadataSuffix=" + lastReadMetadataSuffix +
        " newMetadataSuffix=" + newMetadataSuffix + " lastReadZkVersion=" + version;
  }

  public static class Builder {
    
    private PushPullData data = new PushPullData();
    
    public Builder setCollectionName(String collectionName) {
      data.collectionName = collectionName;
      return this;
    }

    public Builder setShardName(String shardName) {
      data.shardName = shardName;
      return this;
    }

    public Builder setCoreName(String coreName) {
      data.coreName = coreName;
      return this;
    }
    
    public Builder setSharedStoreName(String sharedStoreName) {
      data.sharedStoreName = sharedStoreName;
      return this;
    }
    
    public Builder setLastReadMetadataSuffix(String lastReadMetadataSuffix) {
      data.lastReadMetadataSuffix = lastReadMetadataSuffix;
      return this;
    }
    
    public Builder setNewMetadataSuffix(String newMetadataSuffix) {
      data.newMetadataSuffix = newMetadataSuffix;
      return this;
    }
    
    public Builder setZkVersion(int version) {
      data.version = version;
      return this;
    }
    
    public PushPullData build() {
      return new PushPullData(data.collectionName, data.shardName, data.coreName, 
          data.sharedStoreName, data.lastReadMetadataSuffix, data.newMetadataSuffix, data.version);
    }    
  }

}
