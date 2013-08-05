package org.apache.solr.rest.admin;


import com.google.inject.Inject;
import org.apache.solr.core.JmxMonitoredMap;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.request.SolrQueryRequestDecoder;
import org.apache.solr.rest.BaseResource;

import java.util.HashMap;
import java.util.Map;

/**
 *
 *
 **/
public class StatsSR extends BaseResource implements StatsResource {
  private JmxMonitoredMap<String, SolrInfoMBean> jmx;

  @Inject
  public StatsSR(SolrQueryRequestDecoder requestDecoder, JmxMonitoredMap<String, SolrInfoMBean> jmx) {
    super(requestDecoder);
    this.jmx = jmx;
  }

  @Override
  public Map<String, Object> getStatistics() {
    Map<String, Object> result = new HashMap<String, Object>(jmx.size());
    for (Map.Entry<String, SolrInfoMBean> entry : jmx.entrySet()) {
      result.put(entry.getKey(), entry.getValue().getStatistics());
    }
    return result;
  }
}
