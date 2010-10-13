package org.apache.solr.cloud;

import org.apache.solr.common.params.SolrParams;


/**
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

public class CloudDescriptor {
  private String shardId;
  private String collectionName;
  private SolrParams params;

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

  /** Optional parameters that can change how a core is created. */
  public SolrParams getParams() {
    return params;
  }

  public void setParams(SolrParams params) {
    this.params = params;
  }
}
