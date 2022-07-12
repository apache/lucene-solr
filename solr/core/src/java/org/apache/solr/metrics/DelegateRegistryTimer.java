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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class DelegateRegistryTimer extends Timer {

  private final Timer primaryTimer;
  private final Timer delegateTimer;
  private final Clock clock;

  public DelegateRegistryTimer(Clock clock, Timer primaryTimer, Timer delegateTimer) {
    this.primaryTimer = primaryTimer;
    this.delegateTimer = delegateTimer;
    this.clock = clock;
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    primaryTimer.update(duration, unit);
    delegateTimer.update(duration, unit);
  }

  @Override
  public void update(Duration duration) {
    primaryTimer.update(duration);
    delegateTimer.update(duration);
  }

  @Override
  public <T> T time(Callable<T> event) throws Exception {
    final long startTime = clock.getTick();
    try {
      return event.call();
    } finally {
      update(clock.getTick() - startTime, TimeUnit.NANOSECONDS);
    }
  }

  @Override
  public <T> T timeSupplier(Supplier<T> event) {
    final long startTime = clock.getTick();
    try {
      return event.get();
    } finally {
      update(clock.getTick() - startTime, TimeUnit.NANOSECONDS);
    }
  }

  @Override
  public void time(Runnable event) {
    final long startTime = clock.getTick();
    try {
      event.run();
    } finally {
      update(clock.getTick() - startTime, TimeUnit.NANOSECONDS);
    }
  }

  @Override
  public Context time() {
    return super.time();
  }

  @Override
  public long getCount() {
    return primaryTimer.getCount();
  }

  @Override
  public double getFifteenMinuteRate() {
    return primaryTimer.getFifteenMinuteRate();
  }

  @Override
  public double getFiveMinuteRate() {
    return primaryTimer.getFiveMinuteRate();
  }

  @Override
  public double getMeanRate() {
    return primaryTimer.getMeanRate();
  }

  @Override
  public double getOneMinuteRate() {
    return primaryTimer.getOneMinuteRate();
  }

  @Override
  public Snapshot getSnapshot() {
    return primaryTimer.getSnapshot();
  }

  public Timer getPrimaryTimer() {
    return primaryTimer;
  }

  public Timer getDelegateTimer() {
    return delegateTimer;
  }
}
