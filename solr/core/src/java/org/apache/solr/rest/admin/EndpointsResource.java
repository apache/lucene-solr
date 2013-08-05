package org.apache.solr.rest.admin;


import org.restlet.resource.Get;

import java.util.Collection;
import java.util.Map;

/**
 *
 *
 **/
public interface EndpointsResource {
  @Get
  public Map<String, Collection<String>> endpoints();
}
