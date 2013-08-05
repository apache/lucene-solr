package org.apache.solr.rest;


import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequestDecoder;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.schema.IndexSchema;
import org.restlet.data.Status;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 *
 *
 **/
public abstract class BaseCoreResource extends BaseResource {

  protected boolean doIndent;
  protected SolrCore solrCore;
  protected IndexSchema schema;
  protected QueryResponseWriter responseWriter;


  public BaseCoreResource(SolrQueryRequestDecoder requestDecoder) {
    super(requestDecoder);
    doIndent = true; // default to indenting
  }


  /**
   * Decode URL-encoded strings as UTF-8, and avoid converting "+" to space
   */
  protected static String urlDecode(String str) throws UnsupportedEncodingException {
    return URLDecoder.decode(str.replace("+", "%2B"), "UTF-8");
  }

  protected SolrCore getSolrCore() {
    return solrCore;
  }

  protected IndexSchema getSchema() {
    return schema;
  }


  /**
   * @return the response writer from this core
   */
  @Override
  protected QueryResponseWriter getResponseWriter() {
    return responseWriter;
  }

  @Override
  protected void setupResource() {
    solrCore = request.getCore();
    schema = request.getSchema();
    String responseWriterName = request.getParams().get(CommonParams.WT);
    if (null == responseWriterName) {
      responseWriterName = "json"; // Default to json writer
    }
    String indent = request.getParams().get("indent");
    if (null != indent && ("".equals(indent) || "off".equals(indent))) {
      doIndent = false;
    } else {                       // indent by default
      ModifiableSolrParams newParams = new ModifiableSolrParams(request.getParams());
      newParams.remove(indent);
      newParams.add("indent", "on");
      request.setParams(newParams);
    }
    responseWriter = solrCore.getQueryResponseWriter(responseWriterName);
    contentType = responseWriter.getContentType(request, solrResponse);
    final String path = getRequest().getRootRef().getPath();
    //TODO: fix this?
    if (!"/schema".equals(path)) {
      // don't set webapp property on the request when context and core/collection are excluded
      final int cutoffPoint = path.indexOf("/", 1);
      final String firstPathElement = -1 == cutoffPoint ? path : path.substring(0, cutoffPoint);
      request.getContext().put("webapp", firstPathElement); // Context path
    }
    SolrCore.preDecorateResponse(request, solrResponse);
  }

  /**
   * Override so that we can use the core
   */
  @Override
  protected void decodeRequest() {
    solrCore = findCore();
    if (solrCore != null) {
      try {
        request = requestProvider.decode(solrCore);
      } catch (Exception e) {
        throw new ResourceException(e);
      }
    } else {
      throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, "Couldn't find a Solr Core in the request");
    }
  }

  /**
   * Derived classes must speficy a way to find the core from the Request
   * @return The {@link org.apache.solr.core.SolrCore} for this request.
   */
  protected abstract SolrCore findCore();

  /**
   * Pulls the SolrQueryRequest constructed in SolrDispatchFilter
   * from the SolrRequestInfo thread local, then gets the SolrCore
   * and IndexSchema and sets up the response.
   * writer.
   * <p/>
   * If an error occurs during initialization, setExisting(false) is
   * called and an error status code and message is set; in this case,
   * Restlet will not continue servicing the request (by calling the
   * method annotated to associate it with GET, etc., but rather will
   * send an error response.
   */


  @Override
  protected void handlePostExecution(Logger log) {
    super.handlePostExecution(log);
    if (log.isInfoEnabled() && solrResponse.getToLog().size() > 0) {
      log.info(solrResponse.getToLogAsString(solrCore.getLogId()));
    }
  }

}
