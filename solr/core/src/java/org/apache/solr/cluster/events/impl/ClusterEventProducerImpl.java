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
package org.apache.solr.cluster.events.impl;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.ParseException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.cluster.events.NodeDownEvent;
import org.apache.solr.cluster.events.NodeUpEvent;
import org.apache.solr.cluster.events.Schedule;
import org.apache.solr.cluster.events.ScheduledEvent;
import org.apache.solr.cluster.events.ScheduledEventListener;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.LiveNodesListener;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ClusterEventProducer}.
 * <h3>Implementation notes</h3>
 * <p>For each cluster event relevant listeners are always invoked sequentially
 * (not in parallel) and in arbitrary order. This means that if any listener blocks the
 * processing other listeners may be invoked much later or not at all.</p>
 * <p>Scheduled events are triggered at most with {@link #SCHEDULE_INTERVAL_SEC} interval. See also above note
 * on the sequential processing. If the total time of execution exceeds any schedule
 * interval such events will be silently missed and they will be invoked only when the
 * the next event will be generated.</p>
 */
public class ClusterEventProducerImpl implements ClusterEventProducer, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int SCHEDULE_INTERVAL_SEC = 10;

  private final Map<ClusterEvent.EventType, Set<ClusterEventListener>> listeners = new HashMap<>();
  private final Map<String, CompiledSchedule> schedules = new ConcurrentHashMap<>();
  private final CoreContainer cc;
  private final LiveNodesListener liveNodesListener;
  private final ZkController zkController;
  private final ScheduledExecutorService scheduler;

  private final Set<ClusterEvent.EventType> supportedEvents =
      new HashSet<>() {{
        add(ClusterEvent.EventType.NODE_DOWN);
        add(ClusterEvent.EventType.NODE_UP);
        add(ClusterEvent.EventType.SCHEDULED);
      }};

  private volatile boolean isClosed = false;

  private class CompiledSchedule {
    final String name;
    final TimeZone timeZone;
    final Instant startTime;
    final String interval;
    final DateMathParser dateMathParser;

    Instant lastRunAt;

    CompiledSchedule(Schedule schedule) throws Exception {
      this.name = schedule.getName();
      this.timeZone = TimeZoneUtils.getTimeZone(schedule.getTimeZone());
      this.startTime = parseStartTime(new Date(), schedule.getStartTime(), timeZone);
      this.lastRunAt = startTime;
      this.interval = schedule.getInterval();
      this.dateMathParser = new DateMathParser(timeZone);
    }

    private Instant parseStartTime(Date now, String startTimeStr, TimeZone timeZone) throws Exception {
      try {
        // try parsing startTime as an ISO-8601 date time string
        return DateMathParser.parseMath(now, startTimeStr).toInstant();
      } catch (SolrException e) {
        if (e.code() != SolrException.ErrorCode.BAD_REQUEST.code) {
          throw new Exception("startTime: error parsing value '" + startTimeStr + "': " + e.toString());
        }
      }
      DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE).appendPattern("['T'[HH[:mm[:ss]]]]")
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .toFormatter(Locale.ROOT).withZone(timeZone.toZoneId());
      try {
        return Instant.from(dateTimeFormatter.parse(startTimeStr));
      } catch (Exception e) {
        throw new Exception("startTime: error parsing startTime '" + startTimeStr + "': " + e.toString());
      }
    }

    boolean shouldRun() {
      dateMathParser.setNow(new Date(lastRunAt.toEpochMilli()));
      Instant nextRunTime;
      try {
        Date next = dateMathParser.parseMath(interval);
        nextRunTime = next.toInstant();
      } catch (ParseException e) {
        log.warn("Invalid math expression, skipping: " + e);
        return false;
      }
      if (Instant.now().isAfter(nextRunTime)) {
        return true;
      } else {
        return false;
      }
    }

    void setLastRunAt(Instant lastRunAt) {
      this.lastRunAt = lastRunAt;
    }
  }

  public ClusterEventProducerImpl(CoreContainer coreContainer) {
    this.cc = coreContainer;
    this.zkController = this.cc.getZkController();
    if (zkController == null) {
      liveNodesListener = null;
      scheduler = null;
      return;
    }

    // register liveNodesListener
    liveNodesListener = (oldNodes, newNodes) -> {
      // already closed but still registered
      if (isClosed) {
        // remove the listener
        return true;
      }
      // spurious event, ignore but keep listening
      if (oldNodes.equals(newNodes)) {
        return false;
      }
      oldNodes.forEach(oldNode -> {
        if (!newNodes.contains(oldNode)) {
          fireEvent(new NodeDownEvent() {
            final Instant timestamp = Instant.now();
            @Override
            public Instant getTimestamp() {
              return timestamp;
            }

            @Override
            public String getNodeName() {
              return oldNode;
            }
          });
        }
      });
      newNodes.forEach(newNode -> {
        if (!oldNodes.contains(newNode)) {
          fireEvent(new NodeUpEvent() {
            final Instant timestamp = Instant.now();
            @Override
            public Instant getTimestamp() {
              return timestamp;
            }
            @Override
            public String getNodeName() {
              return newNode;
            }
          });
        }
      });
      return false;
    };

    // XXX register collection state listener?
    // XXX not sure how to efficiently monitor for REPLICA_DOWN events

    // create scheduler
    scheduler = Executors.newSingleThreadScheduledExecutor(new SolrNamedThreadFactory("cluster-event-scheduler"));
    scheduler.schedule(() -> maybeFireScheduledEvent(), SCHEDULE_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  private void ensureNotClosed() {
    if (isClosed) {
      throw new RuntimeException("ClusterEventProducerImpl already closed");
    }
  }

  private void maybeFireScheduledEvent() {
    ensureNotClosed();
    Set<ClusterEventListener> scheduledListeners = listeners.getOrDefault(ClusterEvent.EventType.SCHEDULED, Collections.emptySet());
    if (scheduledListeners.isEmpty()) {
      return;
    }
    scheduledListeners.forEach(listener -> {
      ScheduledEventListener scheduledEventListener = (ScheduledEventListener) listener;
      Schedule schedule = scheduledEventListener.getSchedule();
      CompiledSchedule compiledSchedule = schedules.get(schedule.getName());
      if (compiledSchedule == null) { // ???
        return;
      }
      if (compiledSchedule.shouldRun()) {
        Instant now = Instant.now();
        ClusterEvent event = new ScheduledEvent() {
          @Override
          public Schedule getSchedule() {
            return schedule;
          }

          @Override
          public Instant getTimestamp() {
            return now;
          }
        };
        listener.onEvent(event);
        compiledSchedule.setLastRunAt(now);
      }
    });
  }


  @Override
  public void registerListener(ClusterEventListener listener) throws Exception {
    ensureNotClosed();
    try {
      listener.getEventTypes().forEach(type -> {
        if (!supportedEvents.contains(type)) {
          throw new RuntimeException("event type " + type + " not supported yet");
        }
      });
    } catch (Throwable e) {
      throw new Exception(e);
    }
    CompiledSchedule compiledSchedule = null;
    if (listener.getEventTypes().contains(ClusterEvent.EventType.SCHEDULED)) {
      if (!(listener instanceof ScheduledEventListener)) {
        throw new Exception("listener " + listener + " wants to process " +
            ClusterEvent.EventType.SCHEDULED + " events but is not a " +
            ScheduledEventListener.class.getSimpleName());
      } else {
        compiledSchedule = new CompiledSchedule(((ScheduledEventListener) listener).getSchedule());
      }
    }
    ClusterEventProducer.super.registerListener(listener);
    if (compiledSchedule != null) {
      schedules.put(compiledSchedule.name, compiledSchedule);
    }
  }

  @Override
  public void unregisterListener(ClusterEventListener listener) {
    if (listener.getEventTypes().contains(ClusterEvent.EventType.SCHEDULED)) {
      schedules.remove(((ScheduledEventListener) listener).getSchedule().getName());
    }
    ClusterEventProducer.super.unregisterListener(listener);
  }

  @Override
  public void close() throws IOException {
    isClosed = true;
    if (liveNodesListener != null) {
      zkController.zkStateReader.removeLiveNodesListener(liveNodesListener);
    }
    if (scheduler != null) {
      scheduler.shutdownNow();
      try {
        scheduler.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for the scheduler to shut down, ignoring");
      }
    }
    schedules.clear();
    listeners.clear();
  }

  @Override
  public Map<ClusterEvent.EventType, Set<ClusterEventListener>> getEventListeners() {
    ensureNotClosed();
    return listeners;
  }
}
