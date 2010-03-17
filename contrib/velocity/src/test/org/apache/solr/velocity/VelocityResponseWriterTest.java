package org.apache.solr.velocity;

import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.VelocityResponseWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.AbstractSolrTestCase;

import java.io.StringWriter;
import java.io.IOException;

public class VelocityResponseWriterTest extends AbstractSolrTestCase {
  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }


  public void testTemplateName() throws IOException {
    org.apache.solr.response.VelocityResponseWriter vrw = new VelocityResponseWriter();
    SolrQueryRequest req = req("v.template","custom", "v.template.custom","$response.response.response_data");
    SolrQueryResponse rsp = new SolrQueryResponse();
    StringWriter buf = new StringWriter();
    rsp.add("response_data", "testing");
    vrw.write(buf, req, rsp);
    assertEquals("testing", buf.toString());
  }
}
