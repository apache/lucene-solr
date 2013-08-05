package org.apache.solr.servlet;


import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestDecoder;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 *
 *
 **/
public class HttpServletSQRDecoder implements SolrQueryRequestDecoder {

  protected final HttpServletRequest request;
  protected final Map<SolrConfig, SolrRequestParsers> parsers;

  @Inject
  public HttpServletSQRDecoder(HttpServletRequest request, Map<SolrConfig, SolrRequestParsers> parsers) {
    this.request = request;
    this.parsers = parsers;
  }

  @Override
  public SolrQueryRequest decode() throws Exception {
    return SolrRequestParsers.DEFAULT.parse(null, request.getServletPath(), request);
  }

  @Override
  public SolrQueryRequest decode(SolrCore core) throws Exception {
    SolrConfig solrConfig = core.getSolrConfig();
    SolrRequestParsers parser = parsers.get(solrConfig);
    if (parser == null) {
      parser = new SolrRequestParsers(solrConfig);
      parsers.put(solrConfig, parser);
    }
    return parser.parse(core, request.getServletPath(), request);
  }
}
