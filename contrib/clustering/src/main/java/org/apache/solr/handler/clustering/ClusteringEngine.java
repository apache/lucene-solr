package org.apache.solr.handler.clustering;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;


/**
 *
 *
 **/
public class ClusteringEngine {
  private String name;
  public static final String ENGINE_NAME = "name";
  public static final String DEFAULT_ENGINE_NAME = "default";

  public String init(NamedList config, SolrCore core) {
    name = (String) config.get(ENGINE_NAME);

    return name;
  }

  public String getName() {
    return name;
  }
}
