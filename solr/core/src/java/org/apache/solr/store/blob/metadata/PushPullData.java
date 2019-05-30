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
  
  public PushPullData() {}
  
  public PushPullData(String collectionName, String shardName, String coreName, String sharedStoreName) {
    this.collectionName = collectionName;
    this.shardName = shardName;
    this.coreName = collectionName;
    this.sharedStoreName = sharedStoreName;
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

  public static class Builder {
    
    private PushPullData data = new PushPullData();
    
    public void setCollectionName(String collectionName) {
      data.collectionName = collectionName;
    }

    public void setShardName(String shardName) {
      data.shardName = shardName;
    }

    public void setCoreName(String coreName) {
      data.coreName = coreName;
    }
    
    public void setSharedStoreName(String sharedStoreName) {
      data.sharedStoreName = sharedStoreName;
    }
    
    public PushPullData build() {
      return new PushPullData(data.collectionName, data.shardName, data.coreName, 
          data.sharedStoreName);
    }    
  }

}
