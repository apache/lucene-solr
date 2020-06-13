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
package org.apache.solr.highlight;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;

/**
 * 
 * @since solr 1.3
 */
public abstract class HighlightingPluginBase implements SolrInfoBean, SolrMetricProducer
{
  protected Counter numRequests;
  protected SolrParams defaults;
  protected Set<String> metricNames = ConcurrentHashMap.newKeySet(1);
  protected MetricRegistry registry;
  protected SolrMetricManager metricManager;
  protected String registryName;

  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    if( args != null ) {
      Object o = args.get("defaults");
      if (o != null && o instanceof NamedList ) {
        defaults = ((NamedList) o).toSolrParams();
      }
    }
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public abstract String getDescription();

  @Override
  public Category getCategory()
  {
    return Category.HIGHLIGHTER;
  }

  @Override
  public Set<String> getMetricNames() {
    return metricNames;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return registry;
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registryName, String tag, String scope) {
    this.registryName = registryName;
    this.metricManager = manager;
    registry = manager.registry(registryName);
    numRequests = manager.counter(this, registryName, "requests", getCategory().toString(), scope);
  }
}


