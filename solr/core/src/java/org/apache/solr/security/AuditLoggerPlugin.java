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

import java.io.Closeable;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Audit logger plugins
 */
public abstract class AuditLoggerPlugin implements Closeable, Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String PARAM_BLOCKASYNC = "blockAsync";
  private static final String PARAM_QUEUE_SIZE = "queueSize";
  protected AuditEventFormatter formatter;
  private BlockingQueue<AuditEvent> queue;
  private boolean blockAsync;
  private int blockingQueueSize;

  /**
   * Audits an event. The event should be a {@link AuditEvent} to be able to pull context info.
   * @param event
   */
  public final void auditAsync(AuditEvent event) {
    if (blockAsync) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting to insert AuditEvent into blocking queue");
      }
    } else {
      if (!queue.offer(event)) {
        log.warn("Audit log async queue is full, not blocking since " + PARAM_BLOCKASYNC + "==false");
      }
    }
  }

  /**
   * Audits an event. The event should be a {@link AuditEvent} to be able to pull context info.
   * If an event was submitted asynchronously with {@link #auditAsync(AuditEvent)} then this
   * method will be called by the framework by the background thread.
   * @param event
   */
  public abstract void audit(AuditEvent event);

  /**
   * Initialize the plugin from security.json.
   * @param pluginConfig the config for the plugin
   */
  public void init(Map<String, Object> pluginConfig) {
    blockAsync = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_BLOCKASYNC, false)));
    blockingQueueSize = Integer.parseInt(String.valueOf(pluginConfig.getOrDefault(PARAM_QUEUE_SIZE, 1024)));
    queue = new ArrayBlockingQueue<>(blockingQueueSize);
    formatter = new JSONAuditEventFormatter();
    ExecutorService executorService = Executors.newSingleThreadExecutor(new SolrjNamedThreadFactory("audit"));
    executorService.submit(this);
  }
  
  public void setFormatter(AuditEventFormatter formatter) {
    this.formatter = formatter;
  }
  
  /**
   * Interface for formatting the event
   */
  public interface AuditEventFormatter {
    String formatEvent(AuditEvent event);
  }

  /**
   * Event formatter that returns event as JSON string
   */
  public static class JSONAuditEventFormatter implements AuditEventFormatter {
    /**
     * Formats an audit event as a JSON string
     */
    @Override
    public String formatEvent(AuditEvent event) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
      mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
      try {
        StringWriter sw = new StringWriter();
        mapper.writeValue(sw, event);
        return sw.toString();
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error converting Event to JSON", e);
      }
    }
  }

  /**
   * Pick next event from async queue and call {@link #audit(AuditEvent)}
   */
  @Override
  public void run() {
    try {
      audit(queue.take());
    } catch (InterruptedException e) {
      log.warn("Interrupted while waiting for next audit log event");
    }
  }
}
