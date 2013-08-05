package org.apache.solr.rest;


import org.apache.solr.core.SolrInfoMBean;

import java.util.Collection;

/**
 *
 *
 **/
public interface APIMBean extends SolrInfoMBean {
  public Collection<String> getEndpoints();
}