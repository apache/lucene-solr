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

package org.apache.solr.prometheus.collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.prometheus.client.Collector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricSamplesTest {

  private Collector.MetricFamilySamples.Sample sample(String name, Double value) {
    return new Collector.MetricFamilySamples.Sample(name, Collections.emptyList(), Collections.emptyList(), value);
  }

  private Collector.MetricFamilySamples samples(String metricName, Collector.Type type, Collector.MetricFamilySamples.Sample...samples) {
    return new Collector.MetricFamilySamples(
        metricName,
        type,
        "help",
        new ArrayList<>(Arrays.asList(samples))
    );
  }

  private void validateMetricSamples(
      List<Collector.MetricFamilySamples> allMetrics,
      String metricName,
      List<Double> expectedValues) {

    Collector.MetricFamilySamples test1 = allMetrics.stream()
        .filter(s -> s.name.equals(metricName))
        .findFirst()
        .orElseThrow(() -> new RuntimeException(String.format(Locale.ROOT, "Unable to find item %s", metricName)));

    assertTrue(Iterables.elementsEqual(expectedValues, test1.samples.stream().map(s -> s.value).collect(Collectors.toList())));
  }

  @Test
  public void asList() {
    MetricSamples samples = new MetricSamples(Maps.newHashMap(ImmutableMap.<String, Collector.MetricFamilySamples>builder()
        .put("test1", samples("test1", Collector.Type.GAUGE, sample("test1", 1.0), sample("test1", 2.0)))
        .put("test2", samples("test2", Collector.Type.GAUGE, sample("test2", 1.0)))
        .build()));

    List<Collector.MetricFamilySamples> output = samples.asList();

    assertEquals(2, output.size());

    validateMetricSamples(output, "test1", Arrays.asList(1.0, 2.0));
    validateMetricSamples(output, "test2", Collections.singletonList(1.0));
  }

  @Test
  public void addAll() {
    MetricSamples lhs = new MetricSamples(Maps.newHashMap(ImmutableMap.<String, Collector.MetricFamilySamples>builder()
        .put("same", samples("same", Collector.Type.GAUGE, sample("same", 1.0), sample("same", 2.0)))
        .put("diff1", samples("diff1", Collector.Type.GAUGE, sample("diff1", 1.0)))
        .build()));

    MetricSamples rhs = new MetricSamples(Maps.newHashMap(ImmutableMap.<String, Collector.MetricFamilySamples>builder()
        .put("same", samples("test1", Collector.Type.GAUGE, sample("test1", 3.0), sample("test1", 4.0)))
        .put("diff2", samples("diff2", Collector.Type.GAUGE, sample("diff2", 1.0)))
        .build()));

    lhs.addAll(rhs);

    List<Collector.MetricFamilySamples> output = lhs.asList();

    validateMetricSamples(output, "same", Arrays.asList(1.0, 2.0, 3.0, 4.0));
    validateMetricSamples(output, "diff1", Collections.singletonList(1.0));
    validateMetricSamples(output, "diff2", Collections.singletonList(1.0));
  }

}