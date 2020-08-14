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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.update.UpdateLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The state of the replication with a target cluster.
 */
class CdcrReplicatorState {

  private final String targetCollection;
  private final String zkHost;
  private final CloudSolrClient targetClient;

  private CdcrUpdateLog.CdcrLogReader logReader;

  private long consecutiveErrors = 0;
  private final Map<ErrorType, Long> errorCounters = new HashMap<>();
  private final FixedQueue<ErrorQueueEntry> errorsQueue = new FixedQueue<>(100); // keep the last 100 errors

  private BenchmarkTimer benchmarkTimer;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AtomicBoolean bootstrapInProgress = new AtomicBoolean(false);
  private final AtomicInteger numBootstraps = new AtomicInteger();

  CdcrReplicatorState(final String targetCollection, final String zkHost, final CloudSolrClient targetClient) {
    this.targetCollection = targetCollection;
    this.targetClient = targetClient;
    this.zkHost = zkHost;
    this.benchmarkTimer = new BenchmarkTimer();
  }

  /**
   * Initialise the replicator state with a {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader}
   * that is positioned at the last target cluster checkpoint.
   */
  void init(final CdcrUpdateLog.CdcrLogReader logReader) {
    this.logReader = logReader;
  }

  void closeLogReader() {
    if (logReader != null) {
      logReader.close();
      logReader = null;
    }
  }

  CdcrUpdateLog.CdcrLogReader getLogReader() {
    return logReader;
  }

  String getTargetCollection() {
    return targetCollection;
  }

  String getZkHost() {
    return zkHost;
  }

  CloudSolrClient getClient() {
    return targetClient;
  }

  void shutdown() {
    try {
      targetClient.close();
    } catch (IOException ioe) {
      log.warn("Caught exception trying to close server: ", ioe);
    }
    this.closeLogReader();
  }

  void reportError(ErrorType error) {
    if (!errorCounters.containsKey(error)) {
      errorCounters.put(error, 0l);
    }
    errorCounters.put(error, errorCounters.get(error) + 1);
    errorsQueue.add(new ErrorQueueEntry(error, new Date()));
    consecutiveErrors++;
  }

  void resetConsecutiveErrors() {
    consecutiveErrors = 0;
  }

  /**
   * Returns the number of consecutive errors encountered while trying to forward updates to the target.
   */
  long getConsecutiveErrors() {
    return consecutiveErrors;
  }

  /**
   * Gets the number of errors of a particular type.
   */
  long getErrorCount(ErrorType type) {
    if (errorCounters.containsKey(type)) {
      return errorCounters.get(type);
    } else {
      return 0;
    }
  }

  /**
   * Gets the last errors ordered by timestamp (most recent first)
   */
  List<String[]> getLastErrors() {
    List<String[]> lastErrors = new ArrayList<>();
    synchronized (errorsQueue) {
      Iterator<ErrorQueueEntry> it = errorsQueue.iterator();
      while (it.hasNext()) {
        ErrorQueueEntry entry = it.next();
        lastErrors.add(new String[]{entry.timestamp.toInstant().toString(), entry.type.toLower()});
      }
    }
    return lastErrors;
  }

  /**
   * Return the timestamp of the last processed operations
   */
  String getTimestampOfLastProcessedOperation() {
    if (logReader != null && logReader.getLastVersion() != -1) {
      // Shift back to the right by 20 bits the version number - See VersionInfo#getNewClock
      return Instant.ofEpochMilli(logReader.getLastVersion() >> 20).toString();
    }
    return "";
  }

  /**
   * Gets the benchmark timer.
   */
  BenchmarkTimer getBenchmarkTimer() {
    return this.benchmarkTimer;
  }

  /**
   * @return true if a bootstrap operation is in progress, false otherwise
   */
  boolean isBootstrapInProgress() {
    return bootstrapInProgress.get();
  }

  void setBootstrapInProgress(boolean inProgress) {
    if (bootstrapInProgress.compareAndSet(true, false)) {
      numBootstraps.incrementAndGet();
    }
    bootstrapInProgress.set(inProgress);
  }

  public int getNumBootstraps() {
    return numBootstraps.get();
  }

  enum ErrorType {
    INTERNAL,
    BAD_REQUEST;

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }

  }

  static class BenchmarkTimer {

    private long startTime;
    private long runTime = 0;
    private Map<Integer, Long> opCounters = new HashMap<>();

    /**
     * Start recording time.
     */
    void start() {
      startTime = System.nanoTime();
    }

    /**
     * Stop recording time.
     */
    void stop() {
      runTime += System.nanoTime() - startTime;
      startTime = -1;
    }

    void incrementCounter(final int operationType) {
      switch (operationType) {
        case UpdateLog.ADD:
        case UpdateLog.DELETE:
        case UpdateLog.DELETE_BY_QUERY: {
          if (!opCounters.containsKey(operationType)) {
            opCounters.put(operationType, 0l);
          }
          opCounters.put(operationType, opCounters.get(operationType) + 1);
          return;
        }

        default:
      }
    }

    long getRunTime() {
      long totalRunTime = runTime;
      if (startTime != -1) { // we are currently recording the time
        totalRunTime += System.nanoTime() - startTime;
      }
      return totalRunTime;
    }

    double getOperationsPerSecond() {
      long total = 0;
      for (long counter : opCounters.values()) {
        total += counter;
      }
      double elapsedTimeInSeconds = ((double) this.getRunTime() / 1E9);
      return total / elapsedTimeInSeconds;
    }

    double getAddsPerSecond() {
      long total = opCounters.get(UpdateLog.ADD) != null ? opCounters.get(UpdateLog.ADD) : 0;
      double elapsedTimeInSeconds = ((double) this.getRunTime() / 1E9);
      return total / elapsedTimeInSeconds;
    }

    double getDeletesPerSecond() {
      long total = opCounters.get(UpdateLog.DELETE) != null ? opCounters.get(UpdateLog.DELETE) : 0;
      total += opCounters.get(UpdateLog.DELETE_BY_QUERY) != null ? opCounters.get(UpdateLog.DELETE_BY_QUERY) : 0;
      double elapsedTimeInSeconds = ((double) this.getRunTime() / 1E9);
      return total / elapsedTimeInSeconds;
    }

  }

  private static class ErrorQueueEntry {

    private ErrorType type;
    private Date timestamp;

    private ErrorQueueEntry(ErrorType type, Date timestamp) {
      this.type = type;
      this.timestamp = timestamp;
    }
  }

  private static class FixedQueue<E> extends LinkedList<E> {

    private int maxSize;

    public FixedQueue(int maxSize) {
      this.maxSize = maxSize;
    }

    @Override
    public synchronized boolean add(E e) {
      super.addFirst(e);
      if (size() > maxSize) {
        removeLast();
      }
      return true;
    }
  }

}

