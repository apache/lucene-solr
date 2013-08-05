package org.apache.solr.servlet;


import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.request.SolrQueryRequestDecoder;

import java.util.Map;

/**
 * Bind things that are HttpServlet specific
 */
public class HttpServletModule extends AbstractModule {


  private final Map<SolrConfig, SolrRequestParsers> parsers;

  public HttpServletModule(Map<SolrConfig, SolrRequestParsers> parsers) {
    this.parsers = parsers;
  }

  @Override
  protected void configure() {
    bind(SolrQueryRequestDecoder.class).to(HttpServletSQRDecoder.class);
  }

  @Provides
  Map<SolrConfig, SolrRequestParsers> providesRequestParsers() {
    return parsers;
  }
}
