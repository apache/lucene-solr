package org.apache.solr.rest.admin;


import org.apache.solr.core.JmxMonitoredMap;
import org.apache.solr.rest.APIModule;

/**
 * This is an internal API that can be run on each SDAServer.  See the Admin service for the main access to this.
 */
public class MonitoringAPIModule extends APIModule {
  private JmxMonitoredMap jmx;

  public MonitoringAPIModule(JmxMonitoredMap jmx) {
    this.jmx = jmx;
  }


  @Override
  protected void defineBindings() {
    bind(InfoResource.class).to(InfoSR.class);
    bind(JmxMonitoredMap.class).toInstance(jmx);
  }
}
