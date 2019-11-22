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
package org.apache.solr.core;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;

class MockInfoBean implements SolrInfoBean, SolrMetricProducer {
  Set<String> metricNames = ConcurrentHashMap.newKeySet();
  SolrMetricsContext solrMetricsContext;

  @Override
  public String getName() {
    return "mock";
  }

  @Override
  public Category getCategory() {
    return Category.OTHER;
  }

  @Override
  public String getDescription() {
    return "mock";
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    MetricsMap metricsMap = new MetricsMap((detailed, map) -> {
      map.put("Integer", 123);
      map.put("Double",567.534);
      map.put("Long", 32352463l);
      map.put("Short", (short) 32768);
      map.put("Byte", (byte) 254);
      map.put("Float", 3.456f);
      map.put("String","testing");
      map.put("Object", new Object());
    });
    solrMetricsContext.gauge(metricsMap, true, getClass().getSimpleName(), getCategory().toString(), scope);
  }
}
