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
package org.apache.solr.hadoop;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce partitioner that partitions the Mapper output such that each
 * SolrInputDocument gets sent to the SolrCloud shard that it would have been
 * sent to if the document were ingested via the standard SolrCloud Near Real
 * Time (NRT) API.
 * 
 * In other words, this class implements the same partitioning semantics as the
 * standard SolrCloud NRT API. This enables to mix batch updates from MapReduce
 * ingestion with updates from standard NRT ingestion on the same SolrCloud
 * cluster, using identical unique document keys.
 */
public class SolrCloudPartitioner extends Partitioner<Text, SolrInputDocumentWritable> implements Configurable {
  
  private Configuration conf;
  private DocCollection docCollection;
  private Map<String, Integer> shardNumbers;
  private int shards = 0;
  private final SolrParams emptySolrParams = new MapSolrParams(Collections.EMPTY_MAP);
  
  public static final String SHARDS = SolrCloudPartitioner.class.getName() + ".shards";
  public static final String ZKHOST = SolrCloudPartitioner.class.getName() + ".zkHost";
  public static final String COLLECTION = SolrCloudPartitioner.class.getName() + ".collection";
  
  private static final Logger LOG = LoggerFactory.getLogger(SolrCloudPartitioner.class);
  
  public SolrCloudPartitioner() {}

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;    
    this.shards = conf.getInt(SHARDS, -1);
    if (shards <= 0) {
      throw new IllegalArgumentException("Illegal shards: " + shards);
    }
    String zkHost = conf.get(ZKHOST);
    if (zkHost == null) {
      throw new IllegalArgumentException("zkHost must not be null");
    }
    String collection = conf.get(COLLECTION);
    if (collection == null) {
      throw new IllegalArgumentException("collection must not be null");
    }    
    LOG.info("Using SolrCloud zkHost: {}, collection: {}", zkHost, collection);
    docCollection = new ZooKeeperInspector().extractDocCollection(zkHost, collection);
    if (docCollection == null) {
      throw new IllegalArgumentException("docCollection must not be null");
    }
    if (docCollection.getSlicesMap().size() != shards) {
      throw new IllegalArgumentException("Incompatible shards: + " + shards + " for docCollection: " + docCollection);
    }    
    List<Slice> slices = new ZooKeeperInspector().getSortedSlices(docCollection.getSlices());
    if (slices.size() != shards) {
      throw new IllegalStateException("Incompatible sorted shards: + " + shards + " for docCollection: " + docCollection);
    }    
    shardNumbers = new HashMap(10 * slices.size()); // sparse for performance
    for (int i = 0; i < slices.size(); i++) {
      shardNumbers.put(slices.get(i).getName(), i);
    }
    LOG.debug("Using SolrCloud docCollection: {}", docCollection);
    DocRouter docRouter = docCollection.getRouter();
    if (docRouter == null) {
      throw new IllegalArgumentException("docRouter must not be null");
    }
    LOG.info("Using SolrCloud docRouterClass: {}", docRouter.getClass());    
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
  
  @Override
  public int getPartition(Text key, SolrInputDocumentWritable value, int numPartitions) {
    DocRouter docRouter = docCollection.getRouter();
    SolrInputDocument doc = value.getSolrInputDocument();
    String keyStr = key.toString();
    
    // TODO: scalability: replace linear search in HashBasedRouter.hashToSlice() with binary search on sorted hash ranges
    Slice slice = docRouter.getTargetSlice(keyStr, doc, emptySolrParams, docCollection); 
    
//    LOG.info("slice: {}", slice);
    if (slice == null) {
      throw new IllegalStateException("No matching slice found! The slice seems unavailable. docRouterClass: "
          + docRouter.getClass().getName());
    }
    int rootShard = shardNumbers.get(slice.getName());
    if (rootShard < 0 || rootShard >= shards) {
      throw new IllegalStateException("Illegal shard number " + rootShard + " for slice: " + slice + ", docCollection: "
          + docCollection);
    }      

    // map doc to micro shard aka leaf shard, akin to HashBasedRouter.sliceHash()
    // taking into account mtree merge algorithm
    assert numPartitions % shards == 0; // Also note that numPartitions is equal to the number of reducers
    int hashCode = Hash.murmurhash3_x86_32(keyStr, 0, keyStr.length(), 0); 
    int offset = (hashCode & Integer.MAX_VALUE) % (numPartitions / shards);
    int microShard = (rootShard * (numPartitions / shards)) + offset;
//    LOG.info("Subpartitions rootShard: {}, offset: {}", rootShard, offset);
//    LOG.info("Partitioned to p: {} for numPartitions: {}, shards: {}, key: {}, value: {}", microShard, numPartitions, shards, key, value);
    
    assert microShard >= 0 && microShard < numPartitions;
    return microShard;
  }

}
