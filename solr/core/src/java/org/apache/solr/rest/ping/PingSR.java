package org.apache.solr.rest.ping;

import com.google.inject.Inject;
import org.apache.solr.request.SolrQueryRequestDecoder;
import org.apache.solr.rest.BaseResource;
import org.restlet.representation.Representation;

/**
 * Is the server alive?  Doesn't mean other API's are alive.
 */
//Singleton?
public class PingSR extends BaseResource implements PingResource {

  @Inject
  public PingSR(SolrQueryRequestDecoder requestDecoder) {
    super(requestDecoder);
  }



  @Override
  public Representation get() {
    getSolrResponse().add("ping", "alive");
    Representation result = new BaseResource.SolrOutputRepresentation();
    return result;
  }

}
