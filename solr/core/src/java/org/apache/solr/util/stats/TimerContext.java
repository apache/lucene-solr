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

/**
 * A timing context.
 *
 * @see Timer#time()
 */
public class TimerContext {
  private final Timer timer;
  private final Clock clock;
  private final long startTime;

  /**
   * Creates a new {@link TimerContext} with the current time as its starting value and with the
   * given {@link Timer}.
   *
   * @param timer the {@link Timer} to report the elapsed time to
   */
  TimerContext(Timer timer, Clock clock) {
    this.timer = timer;
    this.clock = clock;
    this.startTime = clock.getTick();
  }

  /**
   * Stops recording the elapsed time, updates the timer and returns the elapsed time
   */
  public long stop() {
    final long elapsedNanos = clock.getTick() - startTime;
    timer.update(elapsedNanos, TimeUnit.NANOSECONDS);
    return elapsedNanos;
  }
}
