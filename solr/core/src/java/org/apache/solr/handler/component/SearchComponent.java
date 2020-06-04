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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.search.facet.FacetModule;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * TODO!
 * 
 *
 * @since solr 1.3
 */
public abstract class SearchComponent implements SolrInfoBean, NamedListInitializedPlugin
{
  /**
   * The name given to this component in solrconfig.xml file
   */
  private String name = this.getClass().getName();

  protected Set<String> metricNames = ConcurrentHashMap.newKeySet();
  protected MetricRegistry registry;

  /**
   * Prepare the response.  Guaranteed to be called before any SearchComponent {@link #process(org.apache.solr.handler.component.ResponseBuilder)} method.
   * Called for every incoming request.
   *
   * The place to do initialization that is request dependent.
   * @param rb The {@link org.apache.solr.handler.component.ResponseBuilder}
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void prepare(ResponseBuilder rb) throws IOException;

  /**
   * Process the request for this component 
   * @param rb The {@link ResponseBuilder}
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void process(ResponseBuilder rb) throws IOException;

  /**
   * Process for a distributed search.
   * @return the next stage for this component
   */
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    return ResponseBuilder.STAGE_DONE;
  }

  /** Called after another component adds a request */
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
  }

  /** Called after all responses for a single request were received */
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
  }

  /** Called after all responses have been received for this stage.
   * Useful when different requests are sent to each shard.
   */
  public void finishStage(ResponseBuilder rb) {
  }
  
  /**
   * Sets the name of the SearchComponent. The name of the component is usually
   * the name defined for it in the configuration.
   */
  public void setName(String name) {
    this.name = name;
  }


  //////////////////////// NamedListInitializedPlugin methods //////////////////////
  @Override
  public void init( @SuppressWarnings({"rawtypes"})NamedList args )
  {
    // By default do nothing
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getName() {
    return name;
  }

  @Override
  public abstract String getDescription();

  @Override
  public Category getCategory() {
    return Category.OTHER;
  }

  @Override
  public Set<String> getMetricNames() {
    return metricNames;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return registry;
  }

  public static final Map<String, Class<? extends SearchComponent>> standard_components;


  static {
    HashMap<String, Class<? extends SearchComponent>> map = new HashMap<>();
    map.put(HighlightComponent.COMPONENT_NAME, HighlightComponent.class);
    map.put(QueryComponent.COMPONENT_NAME, QueryComponent.class);
    map.put(FacetComponent.COMPONENT_NAME, FacetComponent.class);
    map.put(FacetModule.COMPONENT_NAME, FacetModule.class);
    map.put(MoreLikeThisComponent.COMPONENT_NAME, MoreLikeThisComponent.class);
    map.put(StatsComponent.COMPONENT_NAME, StatsComponent.class);
    map.put(DebugComponent.COMPONENT_NAME, DebugComponent.class);
    map.put(RealTimeGetComponent.COMPONENT_NAME, RealTimeGetComponent.class);
    map.put(ExpandComponent.COMPONENT_NAME, ExpandComponent.class);
    map.put(TermsComponent.COMPONENT_NAME, TermsComponent.class);

    standard_components = Collections.unmodifiableMap(map);
  }

}
