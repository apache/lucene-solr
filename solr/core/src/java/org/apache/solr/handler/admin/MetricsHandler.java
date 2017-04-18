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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
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

  public static final String COMPACT_PARAM = "compact";
  public static final String PREFIX_PARAM = "prefix";
  public static final String REGEX_PARAM = "regex";
  public static final String REGISTRY_PARAM = "registry";
  public static final String GROUP_PARAM = "group";
  public static final String TYPE_PARAM = "type";

  public static final String ALL = "all";

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

    boolean compact = req.getParams().getBool(COMPACT_PARAM, false);
    MetricFilter mustMatchFilter = parseMustMatchFilter(req);
    List<MetricType> metricTypes = parseMetricTypes(req);
    List<MetricFilter> metricFilters = metricTypes.stream().map(MetricType::asMetricFilter).collect(Collectors.toList());
    Set<String> requestedRegistries = parseRegistries(req);

    NamedList response = new SimpleOrderedMap();
    for (String registryName : requestedRegistries) {
      MetricRegistry registry = metricManager.registry(registryName);
      response.add(registryName, MetricUtils.toNamedList(registry, metricFilters, mustMatchFilter, false,
          false, compact, null));
    }
    rsp.getValues().add("metrics", response);
  }

  private MetricFilter parseMustMatchFilter(SolrQueryRequest req) {
    String[] prefixes = req.getParams().getParams(PREFIX_PARAM);
    MetricFilter prefixFilter = null;
    if (prefixes != null && prefixes.length > 0) {
      Set<String> prefixSet = new HashSet<>();
      for (String prefix : prefixes) {
        prefixSet.addAll(StrUtils.splitSmart(prefix, ','));
      }
      prefixFilter = new SolrMetricManager.PrefixFilter((String[])prefixSet.toArray(new String[prefixSet.size()]));
    }
    String[] regexes = req.getParams().getParams(REGEX_PARAM);
    MetricFilter regexFilter = null;
    if (regexes != null && regexes.length > 0) {
      regexFilter = new SolrMetricManager.RegexFilter(regexes);
    }
    MetricFilter mustMatchFilter;
    if (prefixFilter == null && regexFilter == null) {
      mustMatchFilter = MetricFilter.ALL;
    } else {
      mustMatchFilter = new SolrMetricManager.OrFilter(prefixFilter, regexFilter);
    }
    return mustMatchFilter;
  }

  private Set<String> parseRegistries(SolrQueryRequest req) {
    String[] groupStr = req.getParams().getParams(GROUP_PARAM);
    String[] registryStr = req.getParams().getParams(REGISTRY_PARAM);
    if ((groupStr == null || groupStr.length == 0) && (registryStr == null || registryStr.length == 0)) {
      // return all registries
      return container.getMetricManager().registryNames();
    }
    boolean allRegistries = false;
    Set<String> initialPrefixes = Collections.emptySet();
    if (groupStr != null && groupStr.length > 0) {
      initialPrefixes = new HashSet<>();
      for (String g : groupStr) {
        List<String> split = StrUtils.splitSmart(g, ',');
        for (String s : split) {
          if (s.trim().equals(ALL)) {
            allRegistries = true;
            break;
          }
          initialPrefixes.add(SolrMetricManager.overridableRegistryName(s.trim()));
        }
        if (allRegistries) {
          return container.getMetricManager().registryNames();
        }
      }
    }

    if (registryStr != null && registryStr.length > 0) {
      if (initialPrefixes.isEmpty()) {
        initialPrefixes = new HashSet<>();
      }
      for (String r : registryStr) {
        List<String> split = StrUtils.splitSmart(r, ',');
        for (String s : split) {
          if (s.trim().equals(ALL)) {
            allRegistries = true;
            break;
          }
          initialPrefixes.add(SolrMetricManager.overridableRegistryName(s.trim()));
        }
        if (allRegistries) {
          return container.getMetricManager().registryNames();
        }
      }
    }
    Set<String> validRegistries = new HashSet<>();
    for (String r : container.getMetricManager().registryNames()) {
      for (String prefix : initialPrefixes) {
        if (r.startsWith(prefix)) {
          validRegistries.add(r);
          break;
        }
      }
    }
    return validRegistries;
  }

  private List<MetricType> parseMetricTypes(SolrQueryRequest req) {
    String[] typeStr = req.getParams().getParams(TYPE_PARAM);
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
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid metric type in: " + types +
          " specified. Must be one of " + MetricType.SUPPORTED_TYPES_MSG, e);
    }
    return metricTypes;
  }

  @Override
  public String getDescription() {
    return "A handler to return all the metrics gathered by Solr";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  enum MetricType {
    histogram(Histogram.class),
    meter(Meter.class),
    timer(Timer.class),
    counter(Counter.class),
    gauge(Gauge.class),
    all(null);

    public static final String SUPPORTED_TYPES_MSG = EnumSet.allOf(MetricType.class).toString();

    private final Class klass;

    MetricType(Class klass) {
      this.klass = klass;
    }

    public MetricFilter asMetricFilter() {
      return (name, metric) -> klass == null || klass.isInstance(metric);
    }
  }
}
