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

package org.apache.solr.handler.admin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.stats.MetricUtils;

/**
 * Request handler to return metrics
 */
public class MetricsHandler extends RequestHandlerBase implements PermissionNameProvider {
  final CoreContainer container;
  final SolrMetricManager metricManager;

  public MetricsHandler() {
    this.container = null;
    this.metricManager = null;
  }

  public MetricsHandler(CoreContainer container) {
    this.container = container;
    this.metricManager = this.container.getMetricManager();
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.METRICS_READ_PERM;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (container == null) {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "Core container instance not initialized");
    }

    List<MetricType> metricTypes = parseMetricTypes(req);
    List<MetricFilter> metricFilters = metricTypes.stream().map(MetricType::asMetricFilter).collect(Collectors.toList());
    List<Group> requestedGroups = parseGroups(req);

    NamedList response = new NamedList();
    for (Group group : requestedGroups) {
      String registryName = SolrMetricManager.getRegistryName(group);
      if (group == Group.core) {
        // this requires special handling because of the way we create registry name for a core (deeply nested)
        container.getAllCoreNames().forEach(s -> {
          String coreRegistryName;
          try (SolrCore core = container.getCore(s)) {
            coreRegistryName = core.getCoreMetricManager().getRegistryName();
          }
          MetricRegistry registry = metricManager.registry(coreRegistryName);
          response.add(coreRegistryName, MetricUtils.toNamedList(registry, metricFilters));
        });
      } else {
        MetricRegistry registry = metricManager.registry(registryName);
        response.add(registryName, MetricUtils.toNamedList(registry, metricFilters));
      }
    }
    rsp.getValues().add("metrics", response);
  }

  private List<Group> parseGroups(SolrQueryRequest req) {
    String[] groupStr = req.getParams().getParams("group");
    List<String> groups = Collections.emptyList();
    if (groupStr != null && groupStr.length > 0) {
      groups = new ArrayList<>();
      for (String g : groupStr) {
        groups.addAll(StrUtils.splitSmart(g, ','));
      }
    }

    List<Group> requestedGroups = Arrays.asList(Group.values()); // by default we return all groups
    try {
      if (groups.size() > 0 && !groups.contains("all")) {
        requestedGroups = groups.stream().map(String::trim).map(Group::valueOf).collect(Collectors.toList());
      }
    } catch (IllegalArgumentException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid group in: " + groups + " specified. Must be one of (all, jvm, jetty, http, node, core)", e);
    }
    return requestedGroups;
  }

  private List<MetricType> parseMetricTypes(SolrQueryRequest req) {
    String[] typeStr = req.getParams().getParams("type");
    List<String> types = Collections.emptyList();
    if (typeStr != null && typeStr.length > 0)  {
      types = new ArrayList<>();
      for (String type : typeStr) {
        types.addAll(StrUtils.splitSmart(type, ','));
      }
    }

    List<MetricType> metricTypes = Collections.singletonList(MetricType.all); // include all metrics by default
    try {
      if (types.size() > 0) {
        metricTypes = types.stream().map(String::trim).map(MetricType::valueOf).collect(Collectors.toList());
      }
    } catch (IllegalArgumentException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid metric type in: " + types + " specified. Must be one of (all, meter, timer, histogram, counter, gauge)", e);
    }
    return metricTypes;
  }

  @Override
  public String getDescription() {
    return "A handler to return all the metrics gathered by Solr";
  }

  enum MetricType {
    histogram(Histogram.class),
    meter(Meter.class),
    timer(Timer.class),
    counter(Counter.class),
    gauge(Gauge.class),
    all(null);

    private final Class klass;

    MetricType(Class klass) {
      this.klass = klass;
    }

    public MetricFilter asMetricFilter() {
      return (name, metric) -> klass == null || klass.isInstance(metric);
    }
  }
}
