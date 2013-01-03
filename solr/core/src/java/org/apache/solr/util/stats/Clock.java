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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * An abstraction for how time passes. It is passed to {@link Timer} to track timing.
 */
public abstract class Clock {
  /**
   * Returns the current time tick.
   *
   * @return time tick in nanoseconds
   */
  public abstract long getTick();

  /**
   * Returns the current time in milliseconds.
   *
   * @return time in milliseconds
   */
  public long getTime() {
    return System.currentTimeMillis();
  }

  private static final Clock DEFAULT = new UserTimeClock();

  /**
   * The default clock to use.
   *
   * @return the default {@link Clock} instance
   *
   * @see UserTimeClock
   */
  public static Clock defaultClock() {
    return DEFAULT;
  }


  /**
   * A clock implementation which returns the current time in epoch nanoseconds.
   */
  public static class UserTimeClock extends Clock {
    @Override
    public long getTick() {
      return System.nanoTime();
    }
  }

  /**
   * A clock implementation which returns the current thread's CPU time.
   */
  public static class CpuTimeClock extends Clock {
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    @Override
    public long getTick() {
      return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }
  }
}
