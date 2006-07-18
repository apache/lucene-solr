package org.apache.solr.request;

import java.io.Writer;
import java.io.IOException;

public class RubyResponseWriter implements QueryResponseWriter {
  static String CONTENT_TYPE_RUBY_UTF8="text/x-ruby;charset=UTF-8";

  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    RubyWriter w = new RubyWriter(writer, req, rsp);
    w.writeResponse();
  }

  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return CONTENT_TYPE_TEXT_UTF8;
  }
}
