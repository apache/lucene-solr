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
package org.apache.solr.handler.component;

import org.apache.lucene.search.FieldDoc;
import org.apache.solr.common.util.NamedList;

public class ShardDoc extends FieldDoc {
  public String shard;
  public String shardAddress;  // TODO
  
  public int orderInShard;
    // the position of this doc within the shard... this can be used
    // to short-circuit comparisons if the shard is equal, and can
    // also be used to break ties within the same shard.

  public Object id;
    // this is currently the uniqueKeyField but
    // may be replaced with internal docid in a future release.

  @SuppressWarnings({"rawtypes"})
  public NamedList sortFieldValues;
  // sort field values for *all* docs in a particular shard.
  // this doc's values are in position orderInShard

  // TODO: store the SolrDocument here?
  // Store the order in the merged list for lookup when getting stored fields?
  // (other components need this ordering to store data in order, like highlighting)
  // but we shouldn't expose uniqueKey (have a map by it) until the stored-field
  // retrieval stage.

  public int positionInResponse;
  // the ordinal position in the merged response arraylist  

  public ShardDoc(float score, Object[] fields, Object uniqueId, String shard) {
      super(-1, score, fields);
      this.id = uniqueId;
      this.shard = shard;
  }

  public ShardDoc() {
    super(-1, Float.NaN);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ShardDoc shardDoc = (ShardDoc) o;

    if (id != null ? !id.equals(shardDoc.id) : shardDoc.id != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }

  @Override
  public String toString(){
    return "id="+id
            +" ,score="+score
            +" ,shard="+shard
            +" ,orderInShard="+orderInShard
            +" ,positionInResponse="+positionInResponse
            +" ,sortFieldValues="+sortFieldValues;
  }
}