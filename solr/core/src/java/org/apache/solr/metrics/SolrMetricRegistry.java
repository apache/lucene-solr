package org.apache.solr.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SolrMetricRegistry extends MetricRegistry {

  private ConcurrentHashMap<String,Metric> map;

  protected ConcurrentMap<String,Metric> buildMap() {
    // some hold as many 500+
    this.map = new ConcurrentHashMap<>(712);
    return this.map;
  }

  public void registerAll(String prefix, MetricSet metrics) throws IllegalArgumentException {
    metrics.getMetrics().forEach((s, metric) -> {
      if (metric instanceof MetricSet) {
        registerAll(name(prefix, s), (MetricSet) metric);
      } else {
        register(name(prefix, s), metric);
      }
    });
  }

  public ConcurrentHashMap<String,Metric> getMap() {
    return map;
  }
}
