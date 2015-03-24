package org.apache.solr.logging;

import java.util.Map;

import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.MDC;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;

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

public class MDCUtils {
  public static void cleanupMDC(Map previousMDCContext) {
    if (previousMDCContext != null)
      MDC.setContextMap(previousMDCContext);
  }

  public static void setMDC (String collection, String shard, String replica, String core) {
    setCollection(collection);
    setShard(shard);
    setReplica(replica);
    setCore(core);
  }

  public static void setCollection(String collection) {
    if (collection != null)
      MDC.put(COLLECTION_PROP, collection);
  }

  public static void setShard(String shard) {
    if (shard != null)
      MDC.put(SHARD_ID_PROP, shard);
  }

  public static void setReplica(String replica) {
    if (replica != null)
      MDC.put(REPLICA_PROP, replica);
  }

  public static void setCore(String core) {
    if (core != null)
      MDC.put(CORE_NAME_PROP, core);
  }

  public static void clearMDC() {
    MDC.remove(COLLECTION_PROP);
    MDC.remove(CORE_NAME_PROP);
    MDC.remove(REPLICA_PROP);
    MDC.remove(SHARD_ID_PROP);
  }
}
