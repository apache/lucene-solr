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
package org.apache.solr.handler;

import java.nio.charset.Charset;
import java.util.Locale;

public class CdcrParams {

  /**
   * The definition of a replica configuration *
   */
  public static final String REPLICA_PARAM = "replica";

  /**
   * The source collection of a replica *
   */
  public static final String SOURCE_COLLECTION_PARAM = "source";

  /**
   * The target collection of a replica *
   */
  public static final String TARGET_COLLECTION_PARAM = "target";

  /**
   * The Zookeeper host of the target cluster hosting the replica *
   */
  public static final String ZK_HOST_PARAM = "zkHost";

  /**
   * The definition of the {@link org.apache.solr.handler.CdcrReplicatorScheduler} configuration *
   */
  public static final String REPLICATOR_PARAM = "replicator";

  /**
   * The thread pool size of the replicator *
   */
  public static final String THREAD_POOL_SIZE_PARAM = "threadPoolSize";

  /**
   * The time schedule (in ms) of the replicator *
   */
  public static final String SCHEDULE_PARAM = "schedule";

  /**
   * The batch size of the replicator *
   */
  public static final String BATCH_SIZE_PARAM = "batchSize";

  /**
   * The definition of the {@link org.apache.solr.handler.CdcrUpdateLogSynchronizer} configuration *
   */
  public static final String UPDATE_LOG_SYNCHRONIZER_PARAM = "updateLogSynchronizer";

  /**
   * The definition of the {@link org.apache.solr.handler.CdcrBufferManager} configuration *
   */
  public static final String BUFFER_PARAM = "buffer";

  /**
   * The default state at startup of the buffer *
   */
  public static final String DEFAULT_STATE_PARAM = "defaultState";

  /**
   * The latest update checkpoint on a target cluster *
   */
  public final static String CHECKPOINT = "checkpoint";

  /**
   * The last processed version on a source cluster *
   */
  public final static String LAST_PROCESSED_VERSION = "lastProcessedVersion";

  /**
   * A list of replica queues on a source cluster *
   */
  public final static String QUEUES = "queues";

  /**
   * The size of a replica queue on a source cluster *
   */
  public final static String QUEUE_SIZE = "queueSize";

  /**
   * The timestamp of the last processed operation in a replica queue *
   */
  public final static String LAST_TIMESTAMP = "lastTimestamp";

  /**
   * A list of qps statistics per collection *
   */
  public final static String OPERATIONS_PER_SECOND = "operationsPerSecond";

  /**
   * Overall counter *
   */
  public final static String COUNTER_ALL = "all";

  /**
   * Counter for Adds *
   */
  public final static String COUNTER_ADDS = "adds";

  /**
   * Counter for Deletes *
   */
  public final static String COUNTER_DELETES = "deletes";

  /**
   * Counter for Bootstrap operations *
   */
  public final static String COUNTER_BOOTSTRAP = "bootstraps";

  /**
   * A list of errors per target collection *
   */
  public final static String ERRORS = "errors";

  /**
   * Counter for consecutive errors encountered by a replicator thread *
   */
  public final static String CONSECUTIVE_ERRORS = "consecutiveErrors";

  /**
   * A list of the last errors encountered by a replicator thread *
   */
  public final static String LAST = "last";

  /**
   * Total size of transaction logs *
   */
  public final static String TLOG_TOTAL_SIZE = "tlogTotalSize";

  /**
   * Total count of transaction logs *
   */
  public final static String TLOG_TOTAL_COUNT = "tlogTotalCount";

  /**
   * The state of the update log synchronizer *
   */
  public final static String UPDATE_LOG_SYNCHRONIZER = "updateLogSynchronizer";

  /**
   * The actions supported by the CDCR API
   */
  public enum CdcrAction {
    START,
    STOP,
    STATUS,
    COLLECTIONCHECKPOINT,
    SHARDCHECKPOINT,
    ENABLEBUFFER,
    DISABLEBUFFER,
    LASTPROCESSEDVERSION,
    QUEUES,
    OPS,
    ERRORS,
    BOOTSTRAP,
    BOOTSTRAP_STATUS,
    CANCEL_BOOTSTRAP;

    public static CdcrAction get(String p) {
      if (p != null) {
        try {
          return CdcrAction.valueOf(p.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
        }
      }
      return null;
    }

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }

  }

  /**
   * The possible states of the CDCR process
   */
  public enum ProcessState {
    STARTED,
    STOPPED;

    public static ProcessState get(byte[] state) {
      if (state != null) {
        try {
          return ProcessState.valueOf(new String(state, Charset.forName("UTF-8")).toUpperCase(Locale.ROOT));
        } catch (Exception e) {
        }
      }
      return null;
    }

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }

    public byte[] getBytes() {
      return toLower().getBytes(Charset.forName("UTF-8"));
    }

    public static String getParam() {
      return "process";
    }

  }

  /**
   * The possible states of the CDCR buffer
   */
  public enum BufferState {
    ENABLED,
    DISABLED;

    public static BufferState get(byte[] state) {
      if (state != null) {
        try {
          return BufferState.valueOf(new String(state, Charset.forName("UTF-8")).toUpperCase(Locale.ROOT));
        } catch (Exception e) {
        }
      }
      return null;
    }

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }

    public byte[] getBytes() {
      return toLower().getBytes(Charset.forName("UTF-8"));
    }

    public static String getParam() {
      return "buffer";
    }

  }
}

