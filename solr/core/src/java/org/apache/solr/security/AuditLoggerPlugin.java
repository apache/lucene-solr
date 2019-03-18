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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.security.AuditEvent.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Audit logger plugins.
 * This interface may change in next release and is marked experimental
 * @since 8.1.0
 * @lucene.experimental
 */
public abstract class AuditLoggerPlugin implements Closeable, Runnable, SolrInfoBean, SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_EVENT_TYPES = "eventTypes";
  
  private static final String PARAM_ASYNC = "async";
  private static final String PARAM_BLOCKASYNC = "blockAsync";
  private static final String PARAM_QUEUE_SIZE = "queueSize";
  private static final String PARAM_NUM_THREADS = "numThreads";
  private static final int DEFAULT_QUEUE_SIZE = 4096;
  private static final int DEFAULT_NUM_THREADS = 2;

  BlockingQueue<AuditEvent> queue;
  boolean async;
  boolean blockAsync;
  int blockingQueueSize;

  protected AuditEventFormatter formatter;
  MetricRegistry registry;
  Set<String> metricNames = ConcurrentHashMap.newKeySet();
  
  protected String registryName;
  protected SolrMetricManager metricManager;
  protected Meter numErrors = new Meter();
  protected Meter numLogged = new Meter();
  protected Timer requestTimes = new Timer();
  protected Counter totalTime = new Counter();

  // Event types to be logged by default
  protected List<String> eventTypes = Arrays.asList(
      EventType.COMPLETED.name(), 
      EventType.ERROR.name(),
      EventType.REJECTED.name(),
      EventType.UNAUTHORIZED.name(),
      EventType.ANONYMOUS_REJECTED.name());
  private ExecutorService executorService;
  private boolean closed;

  /**
   * Initialize the plugin from security.json.
   * This method removes parameters from config object after consuming, so subclasses can check for config errors.
   * @param pluginConfig the config for the plugin
   */
  public void init(Map<String, Object> pluginConfig) {
    formatter = new JSONAuditEventFormatter();
    if (pluginConfig.containsKey(PARAM_EVENT_TYPES)) {
      eventTypes = (List<String>) pluginConfig.get(PARAM_EVENT_TYPES);
    }
    pluginConfig.remove(PARAM_EVENT_TYPES);
    
    async = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_ASYNC, true)));
    blockAsync = Boolean.parseBoolean(String.valueOf(pluginConfig.getOrDefault(PARAM_BLOCKASYNC, false)));
    blockingQueueSize = async ? Integer.parseInt(String.valueOf(pluginConfig.getOrDefault(PARAM_QUEUE_SIZE, DEFAULT_QUEUE_SIZE))) : 1;
    int numThreads = async ? Integer.parseInt(String.valueOf(pluginConfig.getOrDefault(PARAM_NUM_THREADS, DEFAULT_NUM_THREADS))) : 1;
    pluginConfig.remove(PARAM_ASYNC);
    pluginConfig.remove(PARAM_BLOCKASYNC);
    pluginConfig.remove(PARAM_QUEUE_SIZE);
    pluginConfig.remove(PARAM_NUM_THREADS);
    queue = new ArrayBlockingQueue<>(blockingQueueSize);
    if (async) {
      executorService = ExecutorUtil.newMDCAwareFixedThreadPool(numThreads, new SolrjNamedThreadFactory("audit"));
      executorService.submit(this);
    }
    pluginConfig.remove("class");
    log.debug("AuditLogger initialized in {} mode with event types {}", async?"async":"syncronous", eventTypes);
  }

  /**
   * Audits an event. The event should be a {@link AuditEvent} to be able to pull context info.
   * @param event the audit event
   */
  public abstract void audit(AuditEvent event);

  /**
   * Called by the framework, and takes care of metrics  
   */
  public final void doAudit(AuditEvent event) {
    if (async) {
      auditAsync(event);
    } else {
      Timer.Context timer = requestTimes.time();
      numLogged.mark();
      try {
        audit(event);
      } catch(Exception e) {
        numErrors.mark();
        throw e;
      } finally {
        totalTime.inc(timer.stop());
      }
    }
  }

  /**
   * Enqueues an {@link AuditEvent} to a queue and returns immediately.
   * A background thread will pull events from this queue and call {@link #audit(AuditEvent)}
   * @param event the audit event
   */
  public final void auditAsync(AuditEvent event) {
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
   * Pick next event from async queue and call {@link #audit(AuditEvent)}
   */
  @Override
  public void run() {
    while (!closed && !Thread.currentThread().isInterrupted()) {
      try {
        AuditEvent event = queue.poll(1000, TimeUnit.MILLISECONDS);
        if (event == null) continue;
        Timer.Context timer = requestTimes.time();
        audit(event);
        numLogged.mark();
        totalTime.inc(timer.stop());
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for next audit log event");
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        log.warn("Exception when attempting to audit log asynchronously", ex);
        numErrors.mark();
      }
    }
  }
  
  /**
   * Checks whether this event type should be logged based on "eventTypes" config parameter.
   *
   * @param eventType the event type to consider
   * @return true if this event type should be logged 
   */
  public boolean shouldLog(EventType eventType) {
    boolean shouldLog = eventTypes.contains(eventType.name()); 
    if (!shouldLog) {
      log.debug("Event type {} is not configured for audit logging", eventType.name());
    }
    return shouldLog;
  }
  
  public void setFormatter(AuditEventFormatter formatter) {
    this.formatter = formatter;
  }
  
  @Override
  public void initializeMetrics(SolrMetricManager manager, String registryName, String tag, final String scope) {
    String className = this.getClass().getSimpleName();
    log.debug("Initializing metrics for {}", className);
    this.metricManager = manager;
    this.registryName = registryName;
    // Metrics
    registry = manager.registry(registryName);
    numErrors = manager.meter(this, registryName, "errors", getCategory().toString(), scope, className);
    numLogged = manager.meter(this, registryName, "count", getCategory().toString(), scope, className);
    requestTimes = manager.timer(this, registryName, "requestTimes", getCategory().toString(), scope, className);
    totalTime = manager.counter(this, registryName, "totalTime", getCategory().toString(), scope, className);
    manager.registerGauge(this, registryName, () -> blockingQueueSize,"queueCapacity", true, "queueCapacity", getCategory().toString(), scope, className);
    manager.registerGauge(this, registryName, () -> blockingQueueSize - queue.remainingCapacity(),"queueSize", true, "queueSize", getCategory().toString(), scope, className);
    manager.registerGauge(this, registryName, () -> async,"async", true, "async", getCategory().toString(), scope, className);
    metricNames.addAll(Arrays.asList("errors", "logged", "requestTimes", "totalTime", "queueCapacity", "queueSize", "async"));
  }
  
  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return "Auditlogger Plugin " + this.getClass().getName();
  }

  @Override
  public Category getCategory() {
    return Category.SECURITY;
  }
  
  @Override
  public Set<String> getMetricNames() {
    return metricNames;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return registry;
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
      mapper.setSerializationInclusion(Include.NON_NULL);
      try {
        StringWriter sw = new StringWriter();
        mapper.writeValue(sw, event);
        return sw.toString();
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error converting Event to JSON", e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (executorService != null) {
      int timeSlept = 0;
      while (!queue.isEmpty() && timeSlept < 30) {
        try {
          log.info("Async auditlogger queue still has {} elements, sleeping to let it drain...", queue.size());
          Thread.sleep(1000);
          timeSlept ++;
        } catch (InterruptedException e) {}
      }
      closed = true;
      log.info("Shutting down async Auditlogger background thread(s)");
      executorService.shutdownNow();
    }
  }
}
