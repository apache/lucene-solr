package org.apache.solr.request;

import java.io.Writer;
import java.io.IOException;

import org.apache.solr.util.NamedList;

public class PythonResponseWriter implements QueryResponseWriter {
  static String CONTENT_TYPE_PYTHON_ASCII="text/x-python;charset=US-ASCII";

  public void init(NamedList n) {
    /* NOOP */
  }
  
  public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    PythonWriter w = new PythonWriter(writer, req, rsp);
    w.writeResponse();
  }

  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return CONTENT_TYPE_TEXT_ASCII;
  }
}
