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
package org.apache.solr.security;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for asynchronous audit logging. Extend this class for queued logging events.
 * This interface may change in next release and is marked experimental
 * @since 8.1.0
 * @lucene.experimental
 */
public abstract class AsyncAuditLoggerPlugin extends AuditLoggerPlugin implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PARAM_BLOCKASYNC = "blockAsync";
  private static final String PARAM_QUEUE_SIZE = "queueSize";
  private static final String PARAM_NUM_THREADS = "numThreads";
  private static final int DEFAULT_QUEUE_SIZE = 4096;
  private static final int DEFAULT_NUM_THREADS = 1;
  private BlockingQueue<AuditEvent> queue;
  private boolean blockAsync;
  private int blockingQueueSize;
  

  /**
   * Enqueues an {@link AuditEvent} to a queue and returns immediately.
   * A background thread will pull events from this queue and call {@link #auditCallback(AuditEvent)}
   * @param event the audit event
   */
  public final void audit(AuditEvent event) {
    if (blockAsync) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting to insert AuditEvent into blocking queue");
        Thread.currentThread().interrupt();
      }
    } else {
      if (!queue.offer(event)) {
        log.warn("Audit log async queue is full (size={}), not blocking since {}", blockingQueueSize, PARAM_BLOCKASYNC + "==false");
      }
    }
  }

  /**
   * Audits an event. The event should be a {@link AuditEvent} to be able to pull context info.
   * This method will be called by the audit background thread as it pulls events from the
   * queue. This is where the actual logging work shall be done.
   * @param event the audit event
   */
  public abstract void auditCallback(AuditEvent event);

  /**
   * Initialize the plugin from security.json.
   * This method removes parameters from config object after consuming, so subclasses can check for config errors.
   * @param pluginConfig the config for the plugin
   */
  public void init(Map<String, Object> pluginConfig) {
    blockAsync = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_BLOCKASYNC, false)));
    blockingQueueSize = Integer.parseInt(String.valueOf(pluginConfig.getOrDefault(PARAM_QUEUE_SIZE, DEFAULT_QUEUE_SIZE)));
    int numThreads = Integer.parseInt(String.valueOf(pluginConfig.getOrDefault(PARAM_NUM_THREADS, DEFAULT_NUM_THREADS)));;
    pluginConfig.remove(PARAM_BLOCKASYNC);
    pluginConfig.remove(PARAM_QUEUE_SIZE);
    pluginConfig.remove(PARAM_NUM_THREADS);
    queue = new ArrayBlockingQueue<>(blockingQueueSize);
    ExecutorService executorService = ExecutorUtil.newMDCAwareFixedThreadPool(numThreads, new SolrjNamedThreadFactory("audit"));
    executorService.submit(this);
  }

  /**
   * Pick next event from async queue and call {@link #auditCallback(AuditEvent)}
   */
  @Override
  public void run() {
    while (true) {
      try {
        auditCallback(queue.take());
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for next audit log event");
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        log.warn("Exception when attempting to audit log asynchronously", ex);
      }
    }
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registryName, String tag, String scope) {
    super.initializeMetrics(manager, registryName, tag, scope);
    manager.registerGauge(this, registryName, () -> blockingQueueSize,"queueSizeMax", true, "queueSizeMax", getCategory().toString());
    manager.registerGauge(this, registryName, () -> blockingQueueSize - queue.remainingCapacity(),"queueSize", true, "queueSize", getCategory().toString());
    metricNames.add("queueSize");
  }
}
