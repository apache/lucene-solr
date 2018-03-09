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
package org.apache.solr.common.util;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source of time.
 * <p>NOTE: depending on implementation returned values may not be related in any way to the
 * current Epoch or calendar time, and they may even be negative - but the API guarantees that
 * they are always monotonically increasing.</p>
 */
public abstract class TimeSource {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Implementation that uses {@link System#currentTimeMillis()}.
   * This implementation's {@link #getTime()} returns the same values as
   * {@link #getEpochTime()}.
   */
  public static final class CurrentTimeSource extends TimeSource {

    @Override
    @SuppressForbidden(reason = "Needed to provide timestamps based on currentTimeMillis.")
    public long getTime() {
      return TimeUnit.NANOSECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public long getEpochTime() {
      return getTime();
    }

    @Override
    public void sleep(long ms) throws InterruptedException {
      Thread.sleep(ms);
    }

    @Override
    public long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit) {
      return toUnit.convert(value, fromUnit);
    }
  }

  /**
   * Implementation that uses {@link System#nanoTime()}.
   * Epoch time is initialized using {@link CurrentTimeSource}, and then
   * calculated as the elapsed number of nanoseconds as measured by this
   * implementation.
   */
  public static final class NanoTimeSource extends TimeSource {
    private final long epochStart;
    private final long nanoStart;

    public NanoTimeSource() {
      epochStart = CURRENT_TIME.getTime();
      nanoStart = System.nanoTime();
    }

    @Override
    public long getTime() {
      return System.nanoTime();
    }

    @Override
    public long getEpochTime() {
      return epochStart + getTime() - nanoStart;
    }

    @Override
    public void sleep(long ms) throws InterruptedException {
      Thread.sleep(ms);
    }

    @Override
    public long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit) {
      return toUnit.convert(value, fromUnit);
    }
  }

  /** Implementation that uses {@link #NANO_TIME} accelerated by a double multiplier. */
  public static final class SimTimeSource extends TimeSource {

    final double multiplier;
    final long nanoStart;
    final long epochStart;

    /**
     * Create a simulated time source that runs faster than real time by a multiplier.
     * @param multiplier must be greater than 0.0
     */
    public SimTimeSource(double multiplier) {
      this.multiplier = multiplier;
      epochStart = CURRENT_TIME.getTime();
      nanoStart = NANO_TIME.getTime();
    }

    @Override
    public long getTime() {
      return nanoStart + Math.round((double)(NANO_TIME.getTime() - nanoStart) * multiplier);
    }

    @Override
    public long getEpochTime() {
      return epochStart + getTime() - nanoStart;
    }

    @Override
    public void sleep(long ms) throws InterruptedException {
      ms = Math.round((double)ms / multiplier);
      Thread.sleep(ms);
    }

    @Override
    public long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit) {
      long nano = Math.round((double)TimeUnit.NANOSECONDS.convert(value, fromUnit) / multiplier);
      return toUnit.convert(nano, TimeUnit.NANOSECONDS);
    }
  }

  /** This instance uses {@link CurrentTimeSource} for generating timestamps. */
  public static final TimeSource CURRENT_TIME = new CurrentTimeSource();

  /** This instance uses {@link NanoTimeSource} for generating timestamps. */
  public static final TimeSource NANO_TIME = new NanoTimeSource();

  private static Map<String, SimTimeSource> simTimeSources = new ConcurrentHashMap<>();

  /**
   * Obtain an instance of time source.
   * @param type supported types: <code>currentTime</code>, <code>nanoTime</code> and accelerated
   *             time with a double factor in the form of <code>simTime:FACTOR</code>, eg.
   *             <code>simTime:2.5</code>
   * @return one of the supported types
   */
  public static TimeSource get(String type) {
    if (type == null) {
      return NANO_TIME;
    } else if (type.equals("currentTime")) {
      return CURRENT_TIME;
    } else if (type.equals("nanoTime")) {
      return NANO_TIME;
    } else if (type.startsWith("simTime")) {
      return simTimeSources.computeIfAbsent(type, t -> {
        String[] parts = t.split(":");
        double mul = 1.0;
        if (parts.length != 2) {
          log.warn("Invalid simTime specification, assuming multiplier==1.0: '" + type + "'");
        } else {
          try {
            mul = Double.parseDouble(parts[1]);
          } catch (Exception e) {
            log.warn("Invalid simTime specification, assuming multiplier==1.0: '" + type + "'");
          }
        }
        return new SimTimeSource(mul);
      });
    } else {
      throw new UnsupportedOperationException("Unsupported time source type '" + type + "'.");
    }
  }

  /**
   * Return a time value, in nanosecond units. Depending on implementation this value may or
   * may not be related to Epoch time.
   */
  public abstract long getTime();

  /**
   * Return Epoch time. Implementations that are not natively based on epoch time may
   * return values that are consistently off by a (small) fixed number of milliseconds from
   * the actual epoch time.
   */
  public abstract long getEpochTime();

  public abstract void sleep(long ms) throws InterruptedException;

  public abstract long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit);
}
