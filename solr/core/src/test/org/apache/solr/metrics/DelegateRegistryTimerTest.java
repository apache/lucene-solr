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

package org.apache.solr.metrics;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DelegateRegistryTimerTest {

  MetricRegistry.MetricSupplier<Timer> timerSupplier = new MetricSuppliers.DefaultTimerSupplier(null);

  @Test
  public void update() {
    DelegateRegistryTimer delegateRegistryTimer = new DelegateRegistryTimer(Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    delegateRegistryTimer.update(Duration.ofSeconds(10));
    assertEquals(1, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(10000000000.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), 0.0);
    assertEquals(10000000000.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(10000000000L, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMax());
    assertEquals(10000000000.0, delegateRegistryTimer.getSnapshot().getMean(), 0.0);
    assertEquals(10000000000.0, delegateRegistryTimer.getSnapshot().getMedian(), 0.0);
    assertEquals(10000000000L, delegateRegistryTimer.getSnapshot().getMax());
    assertEquals(1, delegateRegistryTimer.getDelegateTimer().getCount());
    assertEquals(10000000000.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(10000000000.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(10000000000L, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMax());

    delegateRegistryTimer.update(Duration.ofSeconds(20));
    delegateRegistryTimer.update(Duration.ofSeconds(30));
    assertEquals(3, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(20000000000.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), 0.0);
    assertEquals(20000000000.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(30000000000L, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMax());
    assertEquals(20000000000.0, delegateRegistryTimer.getSnapshot().getMean(), 0.0);
    assertEquals(20000000000.0, delegateRegistryTimer.getSnapshot().getMedian(), 0.0);
    assertEquals(30000000000L, delegateRegistryTimer.getSnapshot().getMax());
    assertEquals(3, delegateRegistryTimer.getDelegateTimer().getCount());
    assertEquals(20000000000.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(20000000000.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(30000000000L, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMax());
  }

  @Test
  public void testUpdate() {
    DelegateRegistryTimer delegateRegistryTimer = new DelegateRegistryTimer(Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    delegateRegistryTimer.update(10, TimeUnit.SECONDS);
    assertEquals(1, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(10000000000.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), 0.0);
    assertEquals(10000000000.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(10000000000L, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMax());
    assertEquals(10000000000.0, delegateRegistryTimer.getSnapshot().getMean(), 0.0);
    assertEquals(10000000000.0, delegateRegistryTimer.getSnapshot().getMedian(), 0.0);
    assertEquals(10000000000L, delegateRegistryTimer.getSnapshot().getMax());
    assertEquals(1, delegateRegistryTimer.getDelegateTimer().getCount());
    assertEquals(10000000000.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(10000000000.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(10000000000L, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMax());

    delegateRegistryTimer.update(20, TimeUnit.SECONDS);
    delegateRegistryTimer.update(30, TimeUnit.SECONDS);
    assertEquals(3, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(20000000000.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), 0.0);
    assertEquals(20000000000.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(30000000000L, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMax());
    assertEquals(20000000000.0, delegateRegistryTimer.getSnapshot().getMean(), 0.0);
    assertEquals(20000000000.0, delegateRegistryTimer.getSnapshot().getMedian(), 0.0);
    assertEquals(30000000000L, delegateRegistryTimer.getSnapshot().getMax());
    assertEquals(3, delegateRegistryTimer.getDelegateTimer().getCount());
    assertEquals(20000000000.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(20000000000.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(30000000000L, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMax());
  }

  @Test
  public void timeContext() throws InterruptedException {
    DelegateRegistryTimer delegateRegistryTimer = new DelegateRegistryTimer(Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    Timer.Context time = delegateRegistryTimer.time();
    Thread.sleep(100);
    time.close();
    assertTrue(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean() > 100000);
    assertTrue(delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean() > 100000);
    assertEquals(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), delegateRegistryTimer.getSnapshot().getMean(), 0.0);
  }

  @Test
  public void timeSupplier() {
    DelegateRegistryTimer delegateRegistryTimer = new DelegateRegistryTimer(Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    Long supplierResult = delegateRegistryTimer.timeSupplier(() -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Assert.fail("Thread was interrupted while sleeping");
      }
      return 1L;
    });
    assertEquals(Long.valueOf(1L), supplierResult);
    assertTrue(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean() > 100000);
    assertTrue(delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean() > 100000);
    assertEquals(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), delegateRegistryTimer.getSnapshot().getMean(), 0.0);
  }

  @Test
  public void testTimeCallable() throws Exception {
    DelegateRegistryTimer delegateRegistryTimer = new DelegateRegistryTimer(Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    Long callableResult = delegateRegistryTimer.time(() -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Assert.fail("Thread was interrupted while sleeping");
      }
      return 1L;
    });
    assertEquals(Long.valueOf(1L), callableResult);
    assertTrue(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean() > 100000);
    assertTrue(delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean() > 100000);
    assertEquals(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), delegateRegistryTimer.getSnapshot().getMean(), 0.0);
  }

  @Test
  public void testTimeRunnable() {
    DelegateRegistryTimer delegateRegistryTimer = new DelegateRegistryTimer(Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    delegateRegistryTimer.time(() -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Assert.fail("Thread was interrupted while sleeping");
      }
    });
    assertTrue(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean() > 100000);
    assertTrue(delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean() > 100000);
    assertEquals(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), delegateRegistryTimer.getSnapshot().getMean(), 0.0);
  }
}