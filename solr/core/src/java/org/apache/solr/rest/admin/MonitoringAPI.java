package org.apache.solr.rest.admin;


import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.rest.API;
import org.apache.solr.rest.ResourceFinder;

import java.util.Map;

/**
 *
 *
 **/
@Singleton
public class MonitoringAPI extends API {
  @Inject
  public MonitoringAPI(ResourceFinder finder) {
    super(finder);
    setDescription("Provides info and stats about this server");
  }


  @Override
  protected void initAttachments() {
    attach("/info", InfoSR.class);
    attach("/statistics", StatsSR.class);
    attach("/endpoints", EndpointsSR.class);
  }

  @Override
  public String getAPIRoot() {
    return "/monitor";
  }

  @Override
  public String getAPIName() {
    return "MONITOR";
  }

  @Override
  public NamedList getStatistics() {
    NamedList result = super.getStatistics();
    Runtime runtime = Runtime.getRuntime();
    result.add("freeMemory", runtime.freeMemory());
    result.add("totalMemory", runtime.totalMemory());
    result.add("maxMemory", runtime.maxMemory());
    result.add("availableProcessors", runtime.availableProcessors());
    return result;
  }
}
