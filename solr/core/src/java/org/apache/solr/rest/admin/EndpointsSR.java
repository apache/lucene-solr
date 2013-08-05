package org.apache.solr.rest.admin;


import com.google.inject.Inject;
import org.apache.solr.request.SolrQueryRequestDecoder;
import org.apache.solr.rest.API;
import org.apache.solr.rest.BaseResource;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 *
 **/
public class EndpointsSR extends BaseResource implements EndpointsResource {
  protected Set<API> apis;

  @Inject
  public EndpointsSR(SolrQueryRequestDecoder requestDecoder, Set<API> apis) {
    super(requestDecoder);
    this.apis = apis;
  }



  @Override
  public Map<String, Collection<String>> endpoints() {
    Map<String, Collection<String>> result = new HashMap<String, Collection<String>>();
    for (API api : apis) {
      Collection<String> uris = result.get(api.getAPIName());
      if (uris == null) {
        uris = new HashSet<String>();
        result.put(api.getAPIName(), uris);
      }
      uris.addAll(api.getEndpoints());
    }
    return result;
  }
}
