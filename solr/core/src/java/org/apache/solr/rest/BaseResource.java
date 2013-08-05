package org.apache.solr.rest;


import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestDecoder;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.ResponseUtils;
import org.apache.solr.util.FastWriter;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Status;
import org.restlet.representation.OutputRepresentation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;

/**
 *
 *
 **/
public abstract class BaseResource extends ServerResource {

  private static final Charset UTF8 = Charset.forName("UTF-8");
  protected final SolrQueryRequestDecoder requestProvider;
  protected SolrQueryRequest request;
  protected SolrQueryResponse solrResponse = new SolrQueryResponse();

  protected String contentType;
  @Inject
  Injector injector;


  public BaseResource(SolrQueryRequestDecoder requestDecoder) {
    this.requestProvider = requestDecoder;
  }

  @Override
  protected void doInit() throws ResourceException {
    setNegotiated(false); // Turn off content negotiation for now
    //Decode the solrRequest -- do this first
    decodeRequest();
    //Setup the resource once we have the request
    setupResource();

  }

  /**
   * Overridding classes should implement to do custom parsing of the request, for
   * instance to get the core, etc.
   *
   * This is called before {@link #decodeRequest()}
   */
  protected void setupResource(){}

  /**
   * Decodes the request, assuming no core
   */
  protected void decodeRequest() {
    try {
      request = requestProvider.decode();
    } catch (Exception e) {
      throw new ResourceException(e);
    }
  }

  protected SolrQueryRequest getSolrRequest() {

    return request;
  }

  protected SolrQueryResponse getSolrResponse() {
    return solrResponse;
  }

  protected String getContentType() {
    return contentType;
  }

  /**
   * If there is an exception on the SolrResponse:
   * <ul>
   * <li>error info is added to the SolrResponse;</li>
   * <li>the response status code is set to the error code from the exception; and</li>
   * <li>the exception message is added to the list of things to be logged.</li>
   * </ul>
   */
  protected void handleException(Logger log) {
    Exception exception = getSolrResponse().getException();
    if (null != exception) {
      NamedList info = new SimpleOrderedMap();
      int code = ResponseUtils.getErrorInfo(exception, info, log);
      setStatus(Status.valueOf(code));
      getSolrResponse().add("error", info);
      String message = (String) info.get("msg");
      if (null != message && !message.trim().isEmpty()) {
        getSolrResponse().getToLog().add("msg", "{" + message.trim() + "}");
      }
    }
  }

  /**
   * Deal with an exception on the SolrResponse, fill in response header info,
   * and log the accumulated messages on the SolrResponse.
   */
  protected void handlePostExecution(Logger log) {

    handleException(log);

    // TODO: should status=0 (success?) be left as-is in the response header?
    SolrCore.postDecorateResponse(null, getSolrRequest(), getSolrResponse());


  }

  /**
   * @return a default ResponseWriter
   */
  protected QueryResponseWriter getResponseWriter() {
    String responseWriterName = getSolrRequest().getParams().get(CommonParams.WT);
    if (null == responseWriterName) {
      responseWriterName = "json"; // Default to json writer
    }
    return SolrCore.DEFAULT_RESPONSE_WRITERS.get(responseWriterName);
  }


  //TODO: is there a way to inject this

  /**
   * This class serves as an adapter between Restlet and Solr's response writers.
   */
  public class SolrOutputRepresentation extends OutputRepresentation {


    public SolrOutputRepresentation() {
      // No normalization, in case of a custom media type
      super(MediaType.valueOf(contentType));
      // TODO: For now, don't send the Vary: header, but revisit if/when content negotiation is added
      getDimensions().clear();
    }


    /**
     * Called by Restlet to get the response body
     */
    @Override
    public void write(OutputStream outputStream) throws IOException {
      if (getRequest().getMethod() != Method.HEAD) {
        QueryResponseWriter responseWriter = getResponseWriter();
        if (responseWriter instanceof BinaryQueryResponseWriter) {
          BinaryQueryResponseWriter binWriter = (BinaryQueryResponseWriter) responseWriter;
          binWriter.write(outputStream, getSolrRequest(), getSolrResponse());
        } else {
          String charset = ContentStreamBase.getCharsetFromContentType(contentType);
          Writer out = (charset == null || charset.equalsIgnoreCase("UTF-8"))
              ? new OutputStreamWriter(outputStream, UTF8)
              : new OutputStreamWriter(outputStream, charset);
          out = new FastWriter(out);
          responseWriter.write(out, getSolrRequest(), getSolrResponse());
          out.flush();
        }
      }
    }
  }
}
