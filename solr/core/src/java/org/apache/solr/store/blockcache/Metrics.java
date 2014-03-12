package org.apache.solr.store.blockcache;

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

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;

/**
 * @lucene.experimental
 */
public class Metrics implements Updater {
  
  public static class MethodCall {
    public AtomicLong invokes = new AtomicLong();
    public AtomicLong times = new AtomicLong();
  }

  public AtomicLong blockCacheHit = new AtomicLong(0);
  public AtomicLong blockCacheMiss = new AtomicLong(0);
  public AtomicLong blockCacheEviction = new AtomicLong(0);
  public AtomicLong blockCacheSize = new AtomicLong(0);
  public AtomicLong rowReads = new AtomicLong(0);
  public AtomicLong rowWrites = new AtomicLong(0);
  public AtomicLong recordReads = new AtomicLong(0);
  public AtomicLong recordWrites = new AtomicLong(0);
  public AtomicLong queriesExternal = new AtomicLong(0);
  public AtomicLong queriesInternal = new AtomicLong(0);
  public AtomicLong shardBuffercacheAllocate1024 = new AtomicLong(0);
  public AtomicLong shardBuffercacheAllocate8192 = new AtomicLong(0);
  public AtomicLong shardBuffercacheAllocateOther = new AtomicLong(0);
  public AtomicLong shardBuffercacheLost = new AtomicLong(0);
  public Map<String,MethodCall> methodCalls = new ConcurrentHashMap<>();
  
  public AtomicLong tableCount = new AtomicLong(0);
  public AtomicLong rowCount = new AtomicLong(0);
  public AtomicLong recordCount = new AtomicLong(0);
  public AtomicLong indexCount = new AtomicLong(0);
  public AtomicLong indexMemoryUsage = new AtomicLong(0);
  public AtomicLong segmentCount = new AtomicLong(0);

  private MetricsRecord metricsRecord;
  private long previous = System.nanoTime();

  public static void main(String[] args) throws InterruptedException {
    Configuration conf = new Configuration();
    Metrics metrics = new Metrics(conf);
    MethodCall methodCall = new MethodCall();
    metrics.methodCalls.put("test",methodCall);
    for (int i = 0; i < 100; i++) {
      metrics.blockCacheHit.incrementAndGet();
      metrics.blockCacheMiss.incrementAndGet();
      methodCall.invokes.incrementAndGet();
      methodCall.times.addAndGet(56000000);
      Thread.sleep(500);
    }
  }

  public Metrics(Configuration conf) {
    JvmMetrics.init("blockcache", Long.toString(System.currentTimeMillis()));
    MetricsContext metricsContext = MetricsUtil.getContext("blockcache");
    metricsRecord = MetricsUtil.createRecord(metricsContext, "metrics");
    metricsContext.registerUpdater(this);
  }

  @Override
  public void doUpdates(MetricsContext context) {
    synchronized (this) {
      long now = System.nanoTime();
      float seconds = (now - previous) / 1000000000.0f;
      metricsRecord.setMetric("blockcache.hit", getPerSecond(blockCacheHit.getAndSet(0), seconds));
      metricsRecord.setMetric("blockcache.miss", getPerSecond(blockCacheMiss.getAndSet(0), seconds));
      metricsRecord.setMetric("blockcache.eviction", getPerSecond(blockCacheEviction.getAndSet(0), seconds));
      metricsRecord.setMetric("blockcache.size", blockCacheSize.get());
      metricsRecord.setMetric("row.reads", getPerSecond(rowReads.getAndSet(0), seconds));
      metricsRecord.setMetric("row.writes", getPerSecond(rowWrites.getAndSet(0), seconds));
      metricsRecord.setMetric("record.reads", getPerSecond(recordReads.getAndSet(0), seconds));
      metricsRecord.setMetric("record.writes", getPerSecond(recordWrites.getAndSet(0), seconds));
      metricsRecord.setMetric("query.external", getPerSecond(queriesExternal.getAndSet(0), seconds));
      metricsRecord.setMetric("query.internal", getPerSecond(queriesInternal.getAndSet(0), seconds));
      for (Entry<String,MethodCall> entry : methodCalls.entrySet()) {
        String key = entry.getKey();
        MethodCall value = entry.getValue();
        long invokes = value.invokes.getAndSet(0);
        long times = value.times.getAndSet(0);
        
        float avgTimes = (times / (float) invokes) / 1000000000.0f;
        metricsRecord.setMetric("methodcalls." + key + ".count", getPerSecond(invokes, seconds));
        metricsRecord.setMetric("methodcalls." + key + ".time", avgTimes);
      }
      metricsRecord.setMetric("tables", tableCount.get());
      metricsRecord.setMetric("rows", rowCount.get());
      metricsRecord.setMetric("records", recordCount.get());
      metricsRecord.setMetric("index.count", indexCount.get());
      metricsRecord.setMetric("index.memoryusage", indexMemoryUsage.get());
      metricsRecord.setMetric("index.segments", segmentCount.get());
      previous = now;
    }
    metricsRecord.update();
  }

  private float getPerSecond(long value, float seconds) {
    return (float) (value / seconds);
  }

}
