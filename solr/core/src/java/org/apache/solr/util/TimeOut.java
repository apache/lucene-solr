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
package org.apache.solr.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.solr.common.util.TimeSource;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TimeOut {

  private final long timeoutAt, startTime;
  private final TimeSource timeSource;

  public TimeOut(long interval, TimeUnit unit, TimeSource timeSource) {
    this.timeSource = timeSource;
    startTime = timeSource.getTimeNs();
    this.timeoutAt = startTime + NANOSECONDS.convert(interval, unit);
  }

  public boolean hasTimedOut() {
    return timeSource.getTimeNs() > timeoutAt;
  }

  public void sleep(long ms) throws InterruptedException {
    timeSource.sleep(ms);
  }

  public long timeLeft(TimeUnit unit) {
    return unit.convert(timeoutAt - timeSource.getTimeNs(), NANOSECONDS);
  }

  public long timeElapsed(TimeUnit unit) {
    return unit.convert(timeSource.getTimeNs() - startTime, NANOSECONDS);
  }

  public void waitFor(String messageOnTimeOut, Supplier<Boolean> supplier)
      throws InterruptedException, TimeoutException {
    while (!supplier.get() && hasTimedOut()) {
      Thread.sleep(500);
    }
    if (hasTimedOut()) throw new TimeoutException(messageOnTimeOut);
  }
}
