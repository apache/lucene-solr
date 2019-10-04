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
import java.util.List;

import com.codahale.metrics.MetricFilter;

/**
 * A {@link SolrMetricReporter} that supports (prefix) filters.
 */
public abstract class FilteringSolrMetricReporter extends SolrMetricReporter {

  protected List<String> filters = new ArrayList<>();

  public FilteringSolrMetricReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
  }

  public void setFilter(List<String> filters) {
    if (filters == null || filters.isEmpty()) {
      return;
    }
    this.filters.addAll(filters);
  }

  public void setFilter(String filter) {
    if (filter != null && !filter.isEmpty()) {
      this.filters.add(filter);
    }
  }

  /**
   * Report only metrics with names matching any of the prefix filters.
   * If the filters list is empty then all names will match.
   */
  protected MetricFilter newMetricFilter() {
    final MetricFilter filter;
    if (!filters.isEmpty()) {
      filter = new SolrMetricManager.PrefixFilter(filters);
    } else {
      filter = MetricFilter.ALL;
    }
    return filter;
  }

}
