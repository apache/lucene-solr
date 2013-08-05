package org.apache.solr.core;


import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.rest.API;
import org.apache.solr.rest.ResourceFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 *
 **/
@Singleton
public class CoreContainerAPI extends API {
  //TODO: move this someplace common
  public static final String CORE_NAME = "/{coreName}";
  private transient static Logger log = LoggerFactory.getLogger(CoreContainerAPI.class);
  private final CoreContainer coreContainer;

  @Inject
  public CoreContainerAPI(ResourceFinder finder, CoreContainer coreContainer) {
    super(finder);
    this.coreContainer = coreContainer;
  }

  @Override
  protected void initAttachments() {
    //TODO: how to handle new cores?
    Set<String> seen = new HashSet<>();
    for (SolrCore solrCore : coreContainer.getCores()) {
      SolrConfig config = solrCore.getSolrConfig();
      String coreName = solrCore.getName();

      for (Map.Entry<String, SolrRequestHandler> entry : solrCore.getRequestHandlers().entrySet()) {
        if (seen.contains(entry.getKey()) == false) {
          log.info("Attach: " + entry.getKey());
          attach(CORE_NAME + "/" + entry.getKey(), CoreContainerProxySR.class);
          seen.add(entry.getKey());
        }
      }
    }
  }

  @Override
  public String getAPIRoot() {
    return "/collections"; //TODO: change?
  }

  @Override
  public String getAPIName() {
    return "CORE";
  }
}
