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
package org.apache.solr.logging;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.StringUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.slf4j.MDC;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;

/**
 * Set's per thread context info for logging. Nested calls will use the top level parent for all context. The first
 * caller always owns the context until it calls {@link #clear()}. Always call {@link #setCore(SolrCore)} or
 * {@link #setCoreDescriptor(CoreContainer, CoreDescriptor)} and then {@link #clear()} in a finally block.
 */
public class MDCLoggingContext {
  public static final String TRACE_ID = "trace_id";
  // When a thread sets context and finds that the context is already set, we should noop and ignore the finally clear
  private static ThreadLocal<Integer> CALL_DEPTH = ThreadLocal.withInitial(() -> 0);

  public static void setCollection(String collection) {
    if (collection != null) {
      MDC.put(COLLECTION_PROP, "c:" + collection);
    } else {
      MDC.remove(COLLECTION_PROP);
    }
  }

  public static void setTracerId(String traceId) {
    if (!StringUtils.isEmpty(traceId)) {
      MDC.put(TRACE_ID, "t:" + traceId);
    } else {
      MDC.remove(TRACE_ID);
    }
  }
  
  public static void setShard(String shard) {
    if (shard != null) {
      MDC.put(SHARD_ID_PROP, "s:" + shard);
    } else {
      MDC.remove(SHARD_ID_PROP);
    }
  }
  
  public static void setReplica(String replica) {
    if (replica != null) {
      MDC.put(REPLICA_PROP, "r:" + replica);
    } else {
      MDC.remove(REPLICA_PROP);
    }
  }
  
  public static void setCoreName(String core) {
    if (core != null) {
      MDC.put(CORE_NAME_PROP, "x:" + core);
    } else {
      MDC.remove(CORE_NAME_PROP);
    }
  }
  
  public static void setNode(CoreContainer cc) {
    if (cc != null) {
      ZkController zk = cc.getZkController();
      if (zk != null) {
        setNode(zk.getNodeName());
      }
    }
  }
  
  // we allow the host to be set like this because it is the same for any thread
  // in the thread pool - we can't do this with the per core properties!
  public static void setNode(String node) {
    int used = CALL_DEPTH.get();
    if (used == 0) {
      setNodeName(node);
    }
  }
  
  private static void setNodeName(String node) {
    if (node != null) {
      MDC.put(NODE_NAME_PROP, "n:" + node);
    } else {
      MDC.remove(NODE_NAME_PROP);
    }
  }

  /**
   * Sets multiple information from the params.
   * REMEMBER TO CALL {@link #clear()} in a finally!
   */
  public static void setCore(SolrCore core) {
    CoreContainer coreContainer = core == null ? null : core.getCoreContainer();
    CoreDescriptor coreDescriptor = core == null ? null : core.getCoreDescriptor();
    setCoreDescriptor(coreContainer, coreDescriptor);
  }

  /**
   * Sets multiple information from the params.
   * REMEMBER TO CALL {@link #clear()} in a finally!
   */
  public static void setCoreDescriptor(CoreContainer coreContainer, CoreDescriptor cd) {
    setNode(coreContainer);

    int callDepth = CALL_DEPTH.get();
    CALL_DEPTH.set(callDepth + 1);
    if (callDepth > 0) {
      return;
    }

    if (cd != null) {

      assert cd.getName() != null;
      setCoreName(cd.getName());
      
      CloudDescriptor ccd = cd.getCloudDescriptor();
      if (ccd != null) {
        setCollection(ccd.getCollectionName());
        setShard(ccd.getShardId());
        setReplica(ccd.getCoreNodeName());
      }
    }
  }

  /**
   * Call this after {@link #setCore(SolrCore)} or {@link #setCoreDescriptor(CoreContainer, CoreDescriptor)} in a
   * finally.
   */
  public static void clear() {
    int used = CALL_DEPTH.get();
    if (used <= 1) {
      CALL_DEPTH.set(0);
      MDC.remove(COLLECTION_PROP);
      MDC.remove(CORE_NAME_PROP);
      MDC.remove(REPLICA_PROP);
      MDC.remove(SHARD_ID_PROP);
    } else {
      CALL_DEPTH.set(used - 1);
    }
  }
  
  private static void removeAll() {
    MDC.remove(COLLECTION_PROP);
    MDC.remove(CORE_NAME_PROP);
    MDC.remove(REPLICA_PROP);
    MDC.remove(SHARD_ID_PROP);
    MDC.remove(NODE_NAME_PROP);
    MDC.remove(TRACE_ID);
  }

  /** Resets to a cleared state.  Used in-between requests into Solr. */
  public static void reset() {
    CALL_DEPTH.set(0);
    removeAll();
  }
}
