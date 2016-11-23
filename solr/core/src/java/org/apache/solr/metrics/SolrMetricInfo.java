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

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.core.SolrInfoMBean;

/**
 * Wraps meta-data for a metric.
 */
public final class SolrMetricInfo {
  public final String name;
  public final String scope;
  public final SolrInfoMBean.Category category;

  /**
   * Creates a new instance of {@link SolrMetricInfo}.
   *
   * @param name     the name of the metric (e.g. `Requests`)
   * @param scope    the scope of the metric (e.g. `/admin/ping`)
   * @param category the category of the metric (e.g. `QUERYHANDLERS`)
   */
  public SolrMetricInfo(String name, String scope, SolrInfoMBean.Category category) {
    this.name = name;
    this.scope = scope;
    this.category = category;
  }

  /**
   * Returns the metric name defined by this object.
   * For example, if the name is `Requests`, scope is `/admin/ping`,
   * and category is `QUERYHANDLERS`, then the metric name is
   * `QUERYHANDLERS./admin/ping.Requests`.
   *
   * @return the metric name defined by this object
   */
  public String getMetricName() {
    return MetricRegistry.name(category.toString(), scope, name);
  }
}
