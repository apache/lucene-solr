package org.apache.solr.cloud;

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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// this class may be accessed by multiple threads, but only one at a time
public class ActionThrottle {
  private static Logger log = LoggerFactory.getLogger(ActionThrottle.class);
  
  private volatile long lastActionStartedAt;
  private volatile long minMsBetweenActions;

  private final String name;
  
  public ActionThrottle(String name, long minMsBetweenActions) {
    this.name = name;
    this.minMsBetweenActions = minMsBetweenActions;
  }
  
  public void markAttemptingAction() {
    lastActionStartedAt = System.nanoTime();
  }
  
  public void minimumWaitBetweenActions() {
    if (lastActionStartedAt == 0) {
      return;
    }
    long diff = System.nanoTime() - lastActionStartedAt;
    int diffMs = (int) TimeUnit.MILLISECONDS.convert(diff, TimeUnit.NANOSECONDS);
    long minNsBetweenActions = TimeUnit.NANOSECONDS.convert(minMsBetweenActions, TimeUnit.MILLISECONDS);
    log.info("The last {} attempt started {}ms ago.", name, diffMs);
    int sleep = 0;
    
    if (diffMs > 0 && diff < minNsBetweenActions) {
      sleep = (int) TimeUnit.MILLISECONDS.convert(minNsBetweenActions - diff, TimeUnit.NANOSECONDS);
    } else if (diffMs == 0) {
      sleep = (int) minMsBetweenActions;
    }
    
    if (sleep > 0) {
      log.info("Throttling {} attempts - waiting for {}ms", name, sleep);
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
