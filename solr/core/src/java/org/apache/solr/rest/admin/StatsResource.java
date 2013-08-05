package org.apache.solr.rest.admin;


import org.restlet.resource.Get;

import java.util.Map;

/**
 *
 *
 **/
public interface StatsResource {
  @Get
  public Map<String, Object> getStatistics();
}
