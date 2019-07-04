package org.apache.solr.managed;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.core.SolrInfoBean;

/**
 *
 */
public interface ManagedMetricProducer extends SolrInfoBean, ManagedResource {

  @Override
  default Map<String, Float> getManagedValues(Collection<String> tags) {
    Map<String, Object> metrics = getMetricsSnapshot();
    if (metrics == null) {
      return Collections.emptyMap();
    }
    Map<String, Float> result = new HashMap<>();
    tags.forEach(tag -> {
      Object value = metrics.get(tag);
      if (value == null || !(value instanceof Number)) {
        return;
      }
      result.put(tag, ((Number)value).floatValue());
    });
    return result;
  }

  @Override
  default String getResourceName() {
    return getName();
  }
}
