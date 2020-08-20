package org.apache.solr.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SolrMetricRegistry extends MetricRegistry {
  protected ConcurrentMap<String,Metric> buildMap() {
    // some hold as many 500+
    return new ConcurrentHashMap<>(712);
  }
}
