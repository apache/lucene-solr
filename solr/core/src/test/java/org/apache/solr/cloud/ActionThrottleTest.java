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
package org.apache.solr.cloud;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.TimeSource;
import org.junit.Test;

public class ActionThrottleTest extends SolrTestCaseJ4 {

  static class TestNanoTimeSource extends TimeSource {

    private List<Long> returnValues;
    private int index = 0;

    public TestNanoTimeSource(List<Long> returnValues) {
      this.returnValues = returnValues;
    }

    @Override
    public long getTimeNs() {
      return returnValues.get(index++);
    }

    @Override
    public long getEpochTimeNs() {
      return getTimeNs();
    }

    @Override
    public long[] getTimeAndEpochNs() {
      long time = getTimeNs();
      return new long[]{time, time};
    }

    @Override
    public void sleep(long ms) throws InterruptedException {
      TimeSource.NANO_TIME.sleep(ms);
    }

    @Override
    public long convertDelay(TimeUnit fromUnit, long value, TimeUnit toUnit) {
      throw new UnsupportedOperationException();
    }

  }

  // use the same time source as ActionThrottle
  private static final TimeSource timeSource = TimeSource.NANO_TIME;

  @Test
  public void testBasics() throws Exception {

    ActionThrottle at = new ActionThrottle("test", 1000);
    long start = timeSource.getTimeNs();

    at.minimumWaitBetweenActions();

    // should be no wait
    assertTrue(TimeUnit.MILLISECONDS.convert(timeSource.getTimeNs() - start, TimeUnit.NANOSECONDS) < 1000);
    at.markAttemptingAction();

    if (random().nextBoolean()) Thread.sleep(100);

    at.minimumWaitBetweenActions();

    long elaspsedTime = TimeUnit.MILLISECONDS.convert(timeSource.getTimeNs() - start, TimeUnit.NANOSECONDS);

    assertTrue(elaspsedTime + "ms", elaspsedTime >= 995);

    start = timeSource.getTimeNs();

    at.markAttemptingAction();
    at.minimumWaitBetweenActions();

    Thread.sleep(random().nextInt(1000));

    elaspsedTime = TimeUnit.MILLISECONDS.convert(timeSource.getTimeNs() - start, TimeUnit.NANOSECONDS);

    assertTrue(elaspsedTime + "ms", elaspsedTime >= 995);
  }
  
  @Test
  public void testAZeroNanoTimeReturnInWait() throws Exception {

    ActionThrottle at = new ActionThrottle("test", 1000, new TestNanoTimeSource(Arrays.asList(new Long[]{0L, 10L})));
    long start = timeSource.getTimeNs();
    
    at.markAttemptingAction();
    
    at.minimumWaitBetweenActions();
    
    long elaspsedTime = TimeUnit.MILLISECONDS.convert(timeSource.getTimeNs() - start, TimeUnit.NANOSECONDS);
    
    assertTrue(elaspsedTime + "ms", elaspsedTime >= 995);

  }

  public void testCreateNewThrottleWithLastValue() throws Exception {
    ActionThrottle throttle = new ActionThrottle("xyz", 1000, new TestNanoTimeSource(Arrays.asList(new Long[]{10L, 20L})));
    throttle.markAttemptingAction();
    assertEquals((Long)10L, throttle.getLastActionStartedAt());
    throttle = new ActionThrottle("new_xyz", 1000, throttle.getLastActionStartedAt());
    assertEquals((Long)10L, throttle.getLastActionStartedAt());
  }
}
