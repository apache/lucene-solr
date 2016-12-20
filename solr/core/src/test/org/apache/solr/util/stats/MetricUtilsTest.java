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

package org.apache.solr.util.stats;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.Test;

public class MetricUtilsTest extends SolrTestCaseJ4 {

  @Test
  public void testSolrTimerGetSnapshot() {
    // create a timer with up to 100 data points
    final Timer timer = new Timer();
    final int iterations = random().nextInt(100);
    for (int i = 0; i < iterations; ++i) {
      timer.update(random().nextInt(), TimeUnit.NANOSECONDS);
    }
    // obtain timer metrics
    final NamedList<Object> lst = new SimpleOrderedMap<>();
    MetricUtils.addMetrics(lst, timer);
    // check that expected metrics were obtained
    assertEquals(lst.size(), 9);
    final Snapshot snapshot = timer.getSnapshot();
    // cannot test avgRequestsPerMinute directly because mean rate changes as time increases!
    // assertEquals(lst.get("avgRequestsPerSecond"), timer.getMeanRate());
    assertEquals(lst.get("5minRateRequestsPerSecond"), timer.getFiveMinuteRate());
    assertEquals(lst.get("15minRateRequestsPerSecond"), timer.getFifteenMinuteRate());
    assertEquals(lst.get("avgTimePerRequest"), MetricUtils.nsToMs(snapshot.getMean()));
    assertEquals(lst.get("medianRequestTime"), MetricUtils.nsToMs(snapshot.getMedian()));
    assertEquals(lst.get("75thPcRequestTime"), MetricUtils.nsToMs(snapshot.get75thPercentile()));
    assertEquals(lst.get("95thPcRequestTime"), MetricUtils.nsToMs(snapshot.get95thPercentile()));
    assertEquals(lst.get("99thPcRequestTime"), MetricUtils.nsToMs(snapshot.get99thPercentile()));
    assertEquals(lst.get("999thPcRequestTime"), MetricUtils.nsToMs(snapshot.get999thPercentile()));
  }

}

