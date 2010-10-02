package org.apache.solr.search;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.function.ConstValueSource;
import org.apache.solr.search.function.FunctionQuery;
import org.apache.solr.search.function.LiteralValueSource;
import org.apache.solr.util.AbstractSolrTestCase;

import java.util.HashMap;


/**
 *
 *
 **/
public class FunctionQParserTest extends AbstractSolrTestCase {
  public String getSchemaFile() {
    return "schema11.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig-functionquery.xml";
  }

  public String getCoreName() {
    return "basic";
  }


  public void testFunctionQParser() throws Exception {
    ModifiableSolrParams local = new ModifiableSolrParams();
    ModifiableSolrParams params = new ModifiableSolrParams();
    SolrQueryRequest req = new LocalSolrQueryRequest(h.getCore(), "_val_:'foo'", "", 0, 10, new HashMap());
    FunctionQParser parser;
    Query query;
    FunctionQuery fq;
    parser = new FunctionQParser("'foo'", local, params, req);
    query = parser.parse();
    assertTrue("query is not a FunctionQuery", query instanceof FunctionQuery);
    fq = (FunctionQuery) query;
    assertTrue("ValueSource is not a LiteralValueSource", fq.getValueSource() instanceof LiteralValueSource);
  }

}
