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

package org.apache.solr.store.blob.util;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.store.blob.client.BlobCoreMetadata;
import org.apache.zookeeper.KeeperException;

/**
 * Utility class for BlobStore components
 */
public class BlobStoreUtils {

  /** 
   * The only blob that has a constant name for a core is the metadata. Basically the Blob store's equivalent for a core
   * of the highest segments_N file for a Solr server. 
   */
  public static final String CORE_METADATA_BLOB_FILENAME = "core.metadata";
  
  public static String buildBlobStoreMetadataName(String suffix) {
    return CORE_METADATA_BLOB_FILENAME + "." + suffix;
  }

  /**
   * Generates a metadataSuffix value that gets appended to the name of {@link BlobCoreMetadata}
   * that are pushed to blob store
   */
  public static String generateMetadataSuffix() {
    return UUID.randomUUID().toString();
  }

  /**
   * Returns current time in milliseconds for use in measuring elapsed time.
   * Cannot be combined with currentTimeMillis - currentTimeMillis will return ms relative to epoch
   * while this method returns ms relative to some arbitrary time
   */
  public static long getCurrentTimeMs() {
    return TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
  }

  /**
   * Returns the list of core properties that are needed to create a missing core corresponding
   * to provided {@code replica} of the {@code collection}. 
   */
  public static Map<String, String> getSharedCoreProperties(ZkStateReader zkStateReader, DocCollection collection, Replica replica) throws KeeperException {
    // "numShards" is another property that is found in core descriptors. But it is only set on the cores created at 
    // collection creation time. It is not part of cores created by addition of replicas/shards or shard splits.
    // Once set, it is not even kept in sync with latest number of shards. That initial value does not seem to have any 
    // purpose beyond collection creation nor does its persistence as core property. Therefore we do not put it in any 
    // of missing cores we create.

    Map<String, String> params = new HashMap<>();
    params.put(CoreDescriptor.CORE_COLLECTION, collection.getName());
    params.put(CoreDescriptor.CORE_NODE_NAME, replica.getName());
    params.put(CoreDescriptor.CORE_SHARD, collection.getShardId(replica.getNodeName(), replica.getCoreName()));
    params.put(CloudDescriptor.REPLICA_TYPE, Replica.Type.SHARED.name());
    String configName = zkStateReader.readConfigName(collection.getName());
    params.put(CollectionAdminParams.COLL_CONF, configName);
    return params;
  }
}
