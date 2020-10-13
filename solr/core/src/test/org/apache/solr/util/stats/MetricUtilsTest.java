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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.AggregateMetric;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.Test;

public class MetricUtilsTest extends SolrTestCaseJ4 {

  @Test
  @SuppressWarnings({"unchecked"})
  public void testSolrTimerGetSnapshot() {
    // create a timer with up to 100 data points
    final Timer timer = new Timer();
    final int iterations = random().nextInt(100);
    for (int i = 0; i < iterations; ++i) {
      timer.update(Math.abs(random().nextInt()) + 1, TimeUnit.NANOSECONDS);
    }
    // obtain timer metrics
    Map<String,Object> map = new HashMap<>();
    MetricUtils.convertTimer("", timer, MetricUtils.ALL_PROPERTIES, false, false, ".", (k, v) -> {
      ((MapWriter) v).toMap(map);
    });
    @SuppressWarnings({"rawtypes"})
    NamedList lst = new NamedList(map);
    // check that expected metrics were obtained
    assertEquals(14, lst.size());
    final Snapshot snapshot = timer.getSnapshot();
    // cannot test avgRequestsPerMinute directly because mean rate changes as time increases!
    // assertEquals(lst.get("avgRequestsPerSecond"), timer.getMeanRate());
    assertEquals(timer.getFiveMinuteRate(), lst.get("5minRate"));
    assertEquals(timer.getFifteenMinuteRate(), lst.get("15minRate"));
    assertEquals(MetricUtils.nsToMs(snapshot.getMean()), lst.get("mean_ms"));
    assertEquals(MetricUtils.nsToMs(snapshot.getMedian()), lst.get("median_ms"));
    assertEquals(MetricUtils.nsToMs(snapshot.get75thPercentile()), lst.get("p75_ms"));
    assertEquals(MetricUtils.nsToMs(snapshot.get95thPercentile()), lst.get("p95_ms"));
    assertEquals(MetricUtils.nsToMs(snapshot.get99thPercentile()), lst.get("p99_ms"));
    assertEquals(MetricUtils.nsToMs(snapshot.get999thPercentile()), lst.get("p999_ms"));
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testMetrics() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("counter");
    counter.inc();
    Timer timer = registry.timer("timer");
    Timer.Context ctx = timer.time();
    Thread.sleep(150);
    ctx.stop();
    Meter meter = registry.meter("meter");
    meter.mark();
    Histogram histogram = registry.histogram("histogram");
    histogram.update(10);

    // SOLR-14252: check that negative values are supported correctly
    // NB a-d represent the same metric from multiple nodes
    AggregateMetric am1 = new AggregateMetric();
    registry.register("aggregate1", am1);
    am1.set("a", -10);
    am1.set("b", 1);
    am1.set("b", -2);
    am1.set("c", -3);
    am1.set("d", -5);

    // SOLR-14252: check that aggregation of non-Number metrics don't trigger NullPointerException
    AggregateMetric am2 = new AggregateMetric();
    registry.register("aggregate2", am2);
    am2.set("a", false);
    am2.set("b", true);

    Gauge<String> gauge = () -> "foobar";
    registry.register("gauge", gauge);
    Gauge<Long> error = () -> {throw new InternalError("Memory Pool not found error");};
    registry.register("memory.expected.error", error);

    MetricsMap metricsMapWithMap = new MetricsMap((detailed, map) -> {
      map.put("foo", "bar");
    });
    registry.register("mapWithMap", metricsMapWithMap);
    MetricsMap metricsMap = new MetricsMap(map -> {
      map.putNoEx("foo", "bar");
    });
    registry.register("map", metricsMap);

    SolrMetricManager.GaugeWrapper<Map<String,Object>> gaugeWrapper = new SolrMetricManager.GaugeWrapper<>(metricsMap, "foo-tag");
    registry.register("wrappedGauge", gaugeWrapper);

    MetricUtils.toMaps(registry, Collections.singletonList(MetricFilter.ALL), MetricFilter.ALL,
        MetricUtils.ALL_PROPERTIES, false, false, false, false, (k, o) -> {
      @SuppressWarnings({"rawtypes"})
      Map<String, Object> v = new HashMap<>();
      if (o != null) {
        ((MapWriter) o).toMap(v);
      }
      if (k.startsWith("counter")) {
        assertEquals(1L, v.get("count"));
      } else if (k.startsWith("gauge")) {
        assertEquals("foobar", v.get("value"));
      } else if (k.startsWith("timer")) {
        assertEquals(1L, v.get("count"));
        assertTrue(((Number)v.get("min_ms")).intValue() > 100);
      } else if (k.startsWith("meter")) {
        assertEquals(1L, v.get("count"));
      } else if (k.startsWith("histogram")) {
        assertEquals(1L, v.get("count"));
      } else if (k.startsWith("aggregate1")) {
        assertEquals(4, v.get("count"));
        Map<String, Object> values = (Map<String, Object>)v.get("values");
        assertNotNull(values);
        assertEquals(4, values.size());
        Map<String, Object> update = (Map<String, Object>)values.get("a");
        assertEquals(-10, update.get("value"));
        assertEquals(1, update.get("updateCount"));
        update = (Map<String, Object>)values.get("b");
        assertEquals(-2, update.get("value"));
        assertEquals(2, update.get("updateCount"));
        assertEquals(-10D, v.get("min"));
        assertEquals(-2D, v.get("max"));
        assertEquals(-5D, v.get("mean"));
      } else if (k.startsWith("aggregate2")) {
        // SOLR-14252: non-Number metric aggregations should return 0 rather than throwing NPE
        assertEquals(2, v.get("count"));
        assertEquals(0D, v.get("min"));
        assertEquals(0D, v.get("max"));
        assertEquals(0D, v.get("mean"));
      } else if (k.startsWith("memory.expected.error")) {
        assertTrue(v.isEmpty());
      } else if (k.startsWith("map") || k.startsWith("wrapped")) {
        assertNotNull(v.toString(), v.get("value"));
        assertTrue(v.toString(), v.get("value") instanceof Map);
        assertEquals(v.toString(), "bar", ((Map) v.get("value")).get("foo"));
      }
    });
    // test compact format
    MetricUtils.toMaps(registry, Collections.singletonList(MetricFilter.ALL), MetricFilter.ALL,
        MetricUtils.ALL_PROPERTIES, false, false, true, false, (k, o) -> {
          if (k.startsWith("counter")) {
            assertTrue(o instanceof Long);
            assertEquals(1L, o);
          } else if (k.startsWith("gauge")) {
            assertTrue(o instanceof String);
            assertEquals("foobar", o);
          } else if (k.startsWith("timer")) {
            assertTrue(o instanceof MapWriter);
            Map<String, Object> v = new HashMap<>();
            ((MapWriter) o).toMap(v);
            assertEquals(1L, v.get("count"));
            assertTrue(((Number)v.get("min_ms")).intValue() > 100);
          } else if (k.startsWith("meter")) {
            assertTrue(o instanceof MapWriter);
            Map<String, Object> v = new HashMap<>();
            ((MapWriter) o).toMap(v);
            assertEquals(1L, v.get("count"));
          } else if (k.startsWith("histogram")) {
            assertTrue(o instanceof MapWriter);
            Map<String, Object> v = new HashMap<>();
            ((MapWriter) o).toMap(v);
            assertEquals(1L, v.get("count"));
          } else if (k.startsWith("aggregate1")) {
            assertTrue(o instanceof MapWriter);
            Map<String, Object> v = new HashMap<>();
            ((MapWriter) o).toMap(v);
            assertEquals(4, v.get("count"));
            Map<String, Object> values = (Map<String, Object>)v.get("values");
            assertNotNull(values);
            assertEquals(4, values.size());
            Map<String, Object> update = (Map<String, Object>)values.get("a");
            assertEquals(-10, update.get("value"));
            assertEquals(1, update.get("updateCount"));
            update = (Map<String, Object>)values.get("b");
            assertEquals(-2, update.get("value"));
            assertEquals(2, update.get("updateCount"));
          } else if (k.startsWith("aggregate2")) {
            assertTrue(o instanceof MapWriter);
            Map<String, Object> v = new HashMap<>();
            ((MapWriter) o).toMap(v);
            assertEquals(2, v.get("count"));
            Map<String, Object> values = (Map<String, Object>)v.get("values");
            assertNotNull(values);
            assertEquals(2, values.size());
            Map<String, Object> update = (Map<String, Object>)values.get("a");
            assertEquals(false, update.get("value"));
            assertEquals(1, update.get("updateCount"));
            update = (Map<String, Object>)values.get("b");
            assertEquals(true, update.get("value"));
            assertEquals(1, update.get("updateCount"));
          } else if (k.startsWith("memory.expected.error")) {
            assertNull(o);
          } else if (k.startsWith("map") || k.startsWith("wrapped")) {
            assertTrue(o instanceof MapWriter);
            MapWriter writer = (MapWriter) o;
            assertEquals(1, writer._size());
            assertEquals("bar", writer._get("foo", null));
          } else {
            assertTrue(o instanceof MapWriter);
            Map<String, Object> v = new HashMap<>();
            ((MapWriter) o).toMap(v);
            assertEquals(1L, v.get("count"));
          }
        });

  }

}

