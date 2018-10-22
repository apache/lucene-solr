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
package org.apache.solr.cloud;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Timer;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.cloud.ZkNodeProps;

/**
 * Used to hold statistics about some SolrCloud operations.
 *
 * This is experimental API and subject to change.
 */
public class Stats {
  static final int MAX_STORED_FAILURES = 10;

  final Map<String, Stat> stats = new ConcurrentHashMap<>();
  private volatile int queueLength;

  public Map<String, Stat> getStats() {
    return stats;
  }

  public int getSuccessCount(String operation) {
    Stat stat = stats.get(operation.toLowerCase(Locale.ROOT));
    return stat == null ? 0 : stat.success.get();
  }

  public int getErrorCount(String operation)  {
    Stat stat = stats.get(operation.toLowerCase(Locale.ROOT));
    return stat == null ? 0 : stat.errors.get();
  }

  public void success(String operation) {
    String op = operation.toLowerCase(Locale.ROOT);
    Stat stat = stats.get(op);
    if (stat == null) {
      stat = new Stat();
      stats.put(op, stat);
    }
    stat.success.incrementAndGet();
  }

  public void error(String operation) {
    String op = operation.toLowerCase(Locale.ROOT);
    Stat stat = stats.get(op);
    if (stat == null) {
      stat = new Stat();
      stats.put(op, stat);
    }
    stat.errors.incrementAndGet();
  }

  public Timer.Context time(String operation) {
    String op = operation.toLowerCase(Locale.ROOT);
    Stat stat = stats.get(op);
    if (stat == null) {
      stat = new Stat();
      stats.put(op, stat);
    }
    return stat.requestTime.time();
  }

  public void storeFailureDetails(String operation, ZkNodeProps request, SolrResponse resp) {
    String op = operation.toLowerCase(Locale.ROOT);
    Stat stat = stats.get(op);
    if (stat == null) {
      stat = new Stat();
      stats.put(op, stat);
    }
    LinkedList<FailedOp> failedOps = stat.failureDetails;
    synchronized (failedOps)  {
      if (failedOps.size() >= MAX_STORED_FAILURES)  {
        failedOps.removeFirst();
      }
      failedOps.addLast(new FailedOp(request, resp));
    }
  }

  public List<FailedOp> getFailureDetails(String operation) {
    Stat stat = stats.get(operation.toLowerCase(Locale.ROOT));
    if (stat == null || stat.failureDetails.isEmpty()) return null;
    LinkedList<FailedOp> failedOps = stat.failureDetails;
    synchronized (failedOps)  {
      ArrayList<FailedOp> ret = new ArrayList<>(failedOps);
      return ret;
    }
  }

  public int getQueueLength() {
    return queueLength;
  }

  public void setQueueLength(int queueLength) {
    this.queueLength = queueLength;
  }

  public void clear() {
    stats.clear();
  }

  public static class Stat  {
    public final AtomicInteger success;
    public final AtomicInteger errors;
    public final Timer requestTime;
    public final LinkedList<FailedOp> failureDetails;

    public Stat() {
      this.success = new AtomicInteger();
      this.errors = new AtomicInteger();
      this.requestTime = new Timer();
      this.failureDetails = new LinkedList<>();
    }
  }

  public static class FailedOp  {
    public final ZkNodeProps req;
    public final SolrResponse resp;

    public FailedOp(ZkNodeProps req, SolrResponse resp) {
      this.req = req;
      this.resp = resp;
    }
  }
}
