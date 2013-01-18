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

/*
 * Forked from https://github.com/codahale/metrics
 */

package org.apache.solr.util.stats;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A meter metric which measures mean throughput and one-, five-, and fifteen-minute
 * exponentially-weighted moving average throughputs.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">EMA</a>
 */
public class Meter {

  private static final long TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5);

  private final EWMA m1Rate = EWMA.oneMinuteEWMA();
  private final EWMA m5Rate = EWMA.fiveMinuteEWMA();
  private final EWMA m15Rate = EWMA.fifteenMinuteEWMA();

  private final AtomicLong count = new AtomicLong();
  private final long startTime;
  private final AtomicLong lastTick;
  private final TimeUnit rateUnit;
  private final String eventType;
  private final Clock clock;

  /**
   * Creates a new {@link Meter}.
   *
   * @param eventType  the plural name of the event the meter is measuring (e.g., {@code
   *                   "requests"})
   * @param rateUnit   the rate unit of the new meter
   * @param clock      the clock to use for the meter ticks
   */
  Meter(String eventType, TimeUnit rateUnit, Clock clock) {
    this.rateUnit = rateUnit;
    this.eventType = eventType;
    this.clock = clock;
    this.startTime = this.clock.getTick();
    this.lastTick = new AtomicLong(startTime);
  }

  public TimeUnit getRateUnit() {
    return rateUnit;
  }

  public String getEventType() {
    return eventType;
  }

  /**
   * Updates the moving averages.
   */
  void tick() {
    m1Rate.tick();
    m5Rate.tick();
    m15Rate.tick();
  }

  /**
   * Mark the occurrence of an event.
   */
  public void mark() {
    mark(1);
  }

  /**
   * Mark the occurrence of a given number of events.
   *
   * @param n the number of events
   */
  public void mark(long n) {
    tickIfNecessary();
    count.addAndGet(n);
    m1Rate.update(n);
    m5Rate.update(n);
    m15Rate.update(n);
  }

  private void tickIfNecessary() {
    final long oldTick = lastTick.get();
    final long newTick = clock.getTick();
    final long age = newTick - oldTick;
    if (age > TICK_INTERVAL && lastTick.compareAndSet(oldTick, newTick)) {
      final long requiredTicks = age / TICK_INTERVAL;
      for (long i = 0; i < requiredTicks; i++) {
        tick();
      }
    }
  }

  public long getCount() {
    return count.get();
  }

  public double getFifteenMinuteRate() {
    tickIfNecessary();
    return m15Rate.getRate(rateUnit);
  }

  public double getFiveMinuteRate() {
    tickIfNecessary();
    return m5Rate.getRate(rateUnit);
  }

  public double getMeanRate() {
    if (getCount() == 0) {
      return 0.0;
    } else {
      final long elapsed = (clock.getTick() - startTime);
      return convertNsRate(getCount() / (double) elapsed);
    }
  }

  public double getOneMinuteRate() {
    tickIfNecessary();
    return m1Rate.getRate(rateUnit);
  }

  private double convertNsRate(double ratePerNs) {
    return ratePerNs * (double) rateUnit.toNanos(1);
  }
}
