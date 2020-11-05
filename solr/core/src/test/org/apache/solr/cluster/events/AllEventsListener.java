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

package org.apache.solr.cluster.events;

import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AllEventsListener implements ClusterEventListener {
  CountDownLatch eventLatch = new CountDownLatch(1);
  ClusterEvent.EventType expectedType;
  Map<ClusterEvent.EventType, List<ClusterEvent>> events = new HashMap<>();

  @Override
  public void onEvent(ClusterEvent event) {
    events.computeIfAbsent(event.getType(), type -> new ArrayList<>()).add(event);
    if (event.getType() == expectedType) {
      eventLatch.countDown();
    }
  }

  public void setExpectedType(ClusterEvent.EventType expectedType) {
    this.expectedType = expectedType;
    eventLatch = new CountDownLatch(1);
  }

  public void waitForExpectedEvent(int timeoutSeconds) throws InterruptedException {
    boolean await = eventLatch.await(timeoutSeconds, TimeUnit.SECONDS);
    if (!await) {
      Assert.fail("Timed out waiting for expected event " + expectedType);
    }
  }

  public void close() throws IOException {

  }
}
