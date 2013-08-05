package org.apache.solr.core;


import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.solr.request.SolrQueryRequestDecoder;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.rest.BaseCoreResource;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;

/**
 *
 *
 **/
@Singleton
public class CoreContainerProxySR extends BaseCoreResource implements CoreContainerResource {
  protected CoreContainer coreContainer;
  protected SolrRequestHandler requestHandler;

  @Inject
  public CoreContainerProxySR(SolrQueryRequestDecoder requestDecoder, CoreContainer coreContainer) {
    super(requestDecoder);
    this.coreContainer = coreContainer;
  }

  @Override
  protected SolrCore findCore() {
    String coreName = (String) getRequest().getAttributes().get("coreName");
    if (coreName != null) {
      return coreContainer.getCore(coreName);
    }
    return null;
  }

  @Override
  protected void setupResource() {

    super.setupResource();
    String handlerName = getReference().getLastSegment();
    requestHandler = solrCore.getRequestHandler("/" + handlerName);
    if (handlerName == null) {
      requestHandler = solrCore.getRequestHandler(handlerName);
    }
  }

  @Override
  public Representation get() throws ResourceException {
    requestHandler.handleRequest(request, getSolrResponse());
    return new SolrOutputRepresentation();
  }

  @Override
  public Representation post(Representation entity) throws ResourceException {
    requestHandler.handleRequest(request, getSolrResponse());
    return new SolrOutputRepresentation();
  }

}
