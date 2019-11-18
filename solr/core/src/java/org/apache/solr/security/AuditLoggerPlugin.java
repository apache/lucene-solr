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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.security.AuditEvent.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Audit logger plugins.
 * This interface may change in next release and is marked experimental
 * @since 8.1.0
 * @lucene.experimental
 */
public abstract class AuditLoggerPlugin implements Closeable, Runnable, SolrInfoBean {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_EVENT_TYPES = "eventTypes";
  static final String PARAM_ASYNC = "async";
  static final String PARAM_BLOCKASYNC = "blockAsync";
  static final String PARAM_QUEUE_SIZE = "queueSize";
  static final String PARAM_NUM_THREADS = "numThreads";
  static final String PARAM_MUTE_RULES = "muteRules";
  private static final int DEFAULT_QUEUE_SIZE = 4096;
  private static final int DEFAULT_NUM_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);

  BlockingQueue<AuditEvent> queue;
  AtomicInteger auditsInFlight = new AtomicInteger(0);
  boolean async;
  boolean blockAsync;
  int blockingQueueSize;

  protected AuditEventFormatter formatter;
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private ExecutorService executorService;
  private boolean closed;
  private MuteRules muteRules;

  protected SolrMetricsContext solrMetricsContext;
  protected Meter numErrors = new Meter();
  protected Meter numLost = new Meter();
  protected Meter numLogged = new Meter();
  protected Timer requestTimes = new Timer();
  protected Timer queuedTime = new Timer();
  protected Counter totalTime = new Counter();

  // Event types to be logged by default
  protected List<String> eventTypes = Arrays.asList(
      EventType.COMPLETED.name(), 
      EventType.ERROR.name(),
      EventType.REJECTED.name(),
      EventType.UNAUTHORIZED.name(),
      EventType.ANONYMOUS_REJECTED.name());

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
    muteRules = new MuteRules(pluginConfig.remove(PARAM_MUTE_RULES));
    pluginConfig.remove(PARAM_ASYNC);
    pluginConfig.remove(PARAM_BLOCKASYNC);
    pluginConfig.remove(PARAM_QUEUE_SIZE);
    pluginConfig.remove(PARAM_NUM_THREADS);
    if (async) {
      queue = new ArrayBlockingQueue<>(blockingQueueSize);
      executorService = ExecutorUtil.newMDCAwareFixedThreadPool(numThreads, new SolrjNamedThreadFactory("audit"));
      executorService.submit(this);
    }
    pluginConfig.remove("class");
    log.debug("AuditLogger initialized in {} mode with event types {}", async?"async":"syncronous", eventTypes);
  }

  /**
   * This is the method that each Audit plugin has to implement to do the actual logging.
   * @param event the audit event
   */
  protected abstract void audit(AuditEvent event);

  /**
   * Called by the framework, and takes care of metrics tracking and to dispatch
   * to either synchronous or async logging.
   */
  public final void doAudit(AuditEvent event) {
    if (shouldMute(event)) {
      log.debug("Event muted due to mute rule(s)");
      return;
    }
    if (async) {
      auditAsync(event);
    } else {
      Timer.Context timer = requestTimes.time();
      numLogged.mark();
      try {
        audit(event);
      } catch(Exception e) {
        numErrors.mark();
        log.error("Exception when attempting to audit log", e);
      } finally {
        totalTime.inc(timer.stop());
      }
    }
  }

  /**
   * Returns true if any of the configured mute rules matches. The inner lists are ORed, while rules inside
   * inner lists are ANDed
   * @param event the audit event
   */
  protected boolean shouldMute(AuditEvent event) {
    return muteRules.shouldMute(event);
  }

  /**
   * Enqueues an {@link AuditEvent} to a queue and returns immediately.
   * A background thread will pull events from this queue and call {@link #audit(AuditEvent)}
   * @param event the audit event
   */
  protected final void auditAsync(AuditEvent event) {
    assert(async);
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
        numLost.mark();
      }
    }
  }

  /**
   * Pick next event from async queue and call {@link #audit(AuditEvent)}
   */
  @Override
  public void run() {
    assert(async);
    while (!closed && !Thread.currentThread().isInterrupted()) {
      try {
        AuditEvent event = queue.poll(1000, TimeUnit.MILLISECONDS);
        auditsInFlight.incrementAndGet();
        if (event == null) continue;
        if (event.getDate() != null) {
          queuedTime.update(new Date().getTime() - event.getDate().getTime(), TimeUnit.MILLISECONDS);
        }
        Timer.Context timer = requestTimes.time();
        audit(event);
        numLogged.mark();
        totalTime.inc(timer.stop());
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for next audit log event");
        Thread.currentThread().interrupt();
      } catch (Exception ex) {
        log.error("Exception when attempting to audit log asynchronously", ex);
        numErrors.mark();
      } finally {
        auditsInFlight.decrementAndGet();
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
  public void initializeMetrics(SolrMetricsContext parentContext, final String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    String className = this.getClass().getSimpleName();
    log.debug("Initializing metrics for {}", className);
    numErrors = solrMetricsContext.meter("errors", getCategory().toString(), scope, className);
    numLost = solrMetricsContext.meter("lost", getCategory().toString(), scope, className);
    numLogged = solrMetricsContext.meter("count", getCategory().toString(), scope, className);
    requestTimes = solrMetricsContext.timer("requestTimes", getCategory().toString(), scope, className);
    totalTime = solrMetricsContext.counter("totalTime", getCategory().toString(), scope, className);
    if (async) {
      solrMetricsContext.gauge(() -> blockingQueueSize, true, "queueCapacity", getCategory().toString(), scope, className);
      solrMetricsContext.gauge(() -> blockingQueueSize - queue.remainingCapacity(), true, "queueSize", getCategory().toString(), scope, className);
      queuedTime = solrMetricsContext.timer("queuedTime", getCategory().toString(), scope, className);
    }
    solrMetricsContext.gauge(() -> async, true, "async", getCategory().toString(), scope, className);
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
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
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

  /**
   * Waits 30s for async queue to drain, then closes executor threads.
   * Subclasses should either call <code>super.close()</code> or {@link #waitForQueueToDrain(int)}
   * <b>before</b> shutting itself down to make sure they can complete logging events in the queue. 
   */
  @Override
  public void close() throws IOException {
    if (async && executorService != null) {
      waitForQueueToDrain(30);
      closed = true;
      log.info("Shutting down async Auditlogger background thread(s)");
      executorService.shutdownNow();
      try {
        SolrInfoBean.super.close();
      } catch (Exception e) {
        throw new IOException("Exception closing", e);
      }
    }
  }

  /**
   * Blocks until the async event queue is drained
   * @param timeoutSeconds number of seconds to wait for queue to drain
   */
  protected void waitForQueueToDrain(int timeoutSeconds) {
    if (async && executorService != null) {
      int timeSlept = 0;
      while ((!queue.isEmpty() || auditsInFlight.get() > 0) && timeSlept < timeoutSeconds) {
        try {
          log.info("Async auditlogger queue still has {} elements and {} audits in-flight, sleeping to drain...", queue.size(), auditsInFlight.get());
          Thread.sleep(1000);
          timeSlept ++;
        } catch (InterruptedException ignored) {}
      }
    }
  }

  /**
   * Set of rules for when audit logging should be muted.
   */
  private class MuteRules {
    private List<List<MuteRule>> rules;

    MuteRules(Object o) {
      rules = new ArrayList<>();
      if (o != null) {
        if (o instanceof List) {
          ((List)o).forEach(l -> {
            if (l instanceof String) {
              rules.add(Collections.singletonList(parseRule(l)));
            } else if (l instanceof List) {
              List<MuteRule> rl = new ArrayList<>();
              ((List) l).forEach(r -> rl.add(parseRule(r)));
              rules.add(rl);
            }
          });
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The " + PARAM_MUTE_RULES + " configuration must be a list");
        }
      }
    }

    private MuteRule parseRule(Object ruleObj) {
      try {
        String rule = (String) ruleObj;
        if (rule.startsWith("type:")) {
          AuditEvent.RequestType muteType = AuditEvent.RequestType.valueOf(rule.substring("type:".length()));
          return event -> event.getRequestType() != null && event.getRequestType().equals(muteType);          
        }
        if (rule.startsWith("collection:")) {
          return event -> event.getCollections() != null && event.getCollections().contains(rule.substring("collection:".length()));
        }
        if (rule.startsWith("user:")) {
          return event -> event.getUsername() != null && event.getUsername().equals(rule.substring("user:".length()));
        }
        if (rule.startsWith("path:")) {
          return event -> event.getResource() != null && event.getResource().startsWith(rule.substring("path:".length()));
        }
        if (rule.startsWith("ip:")) {
          return event -> event.getClientIp() != null && event.getClientIp().equals(rule.substring("ip:".length()));
        }
        if (rule.startsWith("param:")) {
          String[] kv = rule.substring("param:".length()).split("=");
          if (kv.length == 2) {
            return event -> event.getSolrParams() != null && kv[1].equals(event.getSolrParamAsString(kv[0]));
          } else {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "The 'param' muteRule must be of format 'param:key=value', got " + rule);
          }
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unkonwn mute rule " + rule);
      } catch (ClassCastException | IllegalArgumentException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "There was a problem parsing muteRules. Must be a list of valid rule strings", e);
      }
    }

    /**
     * Returns true if any of the configured mute rules matches. The inner lists are ORed, while rules inside
     * inner lists are ANDed
     */
    boolean shouldMute(AuditEvent event) {
      if (rules == null) return false;
      return rules.stream().anyMatch(rl -> rl.stream().allMatch(r -> r.shouldMute(event)));
    }
  }

  public interface MuteRule {
    boolean shouldMute(AuditEvent event);
  }
}
