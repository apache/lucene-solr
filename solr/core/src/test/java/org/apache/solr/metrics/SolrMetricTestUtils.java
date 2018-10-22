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
package org.apache.solr.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.codahale.metrics.Counter;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.core.SolrInfoBean;

public final class SolrMetricTestUtils {

  private static final int                    MAX_ITERATIONS = 100;
  private static final SolrInfoBean.Category CATEGORIES[]   = SolrInfoBean.Category.values();

  public static String getRandomScope(Random random) {
    return getRandomScope(random, random.nextBoolean());
  }

  public static String getRandomScope(Random random, boolean shouldDefineScope) {
    return shouldDefineScope ? TestUtil.randomSimpleString(random, 5, 10) : null; // must be simple string for JMX publishing
  }

  public static SolrInfoBean.Category getRandomCategory(Random random) {
    return getRandomCategory(random, random.nextBoolean());
  }

  public static SolrInfoBean.Category getRandomCategory(Random random, boolean shouldDefineCategory) {
    return shouldDefineCategory ? CATEGORIES[TestUtil.nextInt(random, 0, CATEGORIES.length - 1)] : null;
  }

  public static Map<String, Counter> getRandomMetrics(Random random) {
    return getRandomMetrics(random, random.nextBoolean());
  }

  public static Map<String, Counter> getRandomMetrics(Random random, boolean shouldDefineMetrics) {
    return shouldDefineMetrics ? getRandomMetricsWithReplacements(random, new HashMap<>()) : null;
  }

  public static final String SUFFIX = "_testing";

  public static Map<String, Counter> getRandomMetricsWithReplacements(Random random, Map<String, Counter> existing) {
    HashMap<String, Counter> metrics = new HashMap<>();
    ArrayList<String> existingKeys = new ArrayList<>(existing.keySet());

    int numMetrics = TestUtil.nextInt(random, 1, MAX_ITERATIONS);
    for (int i = 0; i < numMetrics; ++i) {
      boolean shouldReplaceMetric = !existing.isEmpty() && random.nextBoolean();
      String name = shouldReplaceMetric
          ? existingKeys.get(TestUtil.nextInt(random, 0, existingKeys.size() - 1))
          : TestUtil.randomSimpleString(random, 5, 10) + SUFFIX; // must be simple string for JMX publishing

      Counter counter = new Counter();
      counter.inc(random.nextLong());
      metrics.put(name, counter);
    }

    return metrics;
  }

  public static SolrMetricProducer getProducerOf(SolrMetricManager metricManager, SolrInfoBean.Category category, String scope, Map<String, Counter> metrics) {
    return new SolrMetricProducer() {
      @Override
      public void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
        if (category == null) {
          throw new IllegalArgumentException("null category");
        }
        if (metrics == null || metrics.isEmpty()) {
          return;
        }
        for (Map.Entry<String, Counter> entry : metrics.entrySet()) {
          manager.counter(null, registry, entry.getKey(), category.toString(), scope);
        }
      }

      @Override
      public String toString() {
        return "SolrMetricProducer.of{" +
            "\ncategory=" + category +
            "\nscope=" + scope +
            "\nmetrics=" + metrics +
            "\n}";
      }
    };
  }
}
