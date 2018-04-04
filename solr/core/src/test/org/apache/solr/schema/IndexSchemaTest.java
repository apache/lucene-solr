/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.schema;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class IndexSchemaTest extends SolrTestCaseJ4 {

  final private static String solrConfigFileName = "solrconfig.xml";
  final private static String schemaFileName = "schema.xml";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore(solrConfigFileName, schemaFileName);
  }

  /**
   * This test assumes the schema includes:
   * &lt;dynamicField name="dynamic_*" type="string" indexed="true" stored="true"/&gt;
   * &lt;dynamicField name="*_dynamic" type="string" indexed="true" stored="true"/&gt;
   */
  @Test
  public void testDynamicCopy()
  {
    SolrCore core = h.getCore();
    assertU(adoc("id", "10", "title", "test", "aaa_dynamic", "aaa"));
    assertU(commit());

    Map<String,String> args = new HashMap<>();
    args.put( CommonParams.Q, "title:test" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );

    assertQ("Make sure they got in", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='10']"
            );

    args = new HashMap<>();
    args.put( CommonParams.Q, "aaa_dynamic:aaa" );
    args.put( "indent", "true" );
    req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("dynamic source", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='10']"
            );

    args = new HashMap<>();
    args.put( CommonParams.Q, "dynamic_aaa:aaa" );
    args.put( "indent", "true" );
    req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("dynamic destination", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/str[@name='id'][.='10']"
            );
    clearIndex();
  }

  @Test
  public void testIsDynamicField() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    assertFalse( schema.isDynamicField( "id" ) );
    assertTrue( schema.isDynamicField( "aaa_i" ) );
    assertFalse( schema.isDynamicField( "no_such_field" ) );
  }

  @Test
  public void testProperties() throws Exception{
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    assertFalse(schema.getField("id").multiValued());

    final String dateClass = RANDOMIZED_NUMERIC_FIELDTYPES.get(Date.class);
    final boolean usingPoints = Boolean.getBoolean(NUMERIC_POINTS_SYSPROP);
    // Test TrieDate fields. The following asserts are expecting a field type defined as:
    String expectedDefinition = "<fieldtype name=\"tdatedv\" class=\""+dateClass+"\" " +
        "precisionStep=\"6\" docValues=\"true\" multiValued=\"true\"/>";
    FieldType tdatedv = schema.getFieldType("foo_tdtdvs");
    assertTrue("Expecting a field type defined as " + expectedDefinition, 
               (usingPoints ? DatePointField.class : TrieDateField.class).isInstance(tdatedv));
    assertTrue("Expecting a field type defined as " + expectedDefinition,
               tdatedv.hasProperty(FieldProperties.DOC_VALUES));
    assertTrue("Expecting a field type defined as " + expectedDefinition,
               tdatedv.isMultiValued());
    if ( ! usingPoints ) {
      assertEquals("Expecting a field type defined as " + expectedDefinition,
                   6, ((TrieDateField)tdatedv).getPrecisionStep());
    }
  }

  @Test // LUCENE-5803
  public void testReuseAnalysisComponents() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    Analyzer solrAnalyzer = schema.getIndexAnalyzer();
    // Get the tokenStream for two fields that both have the same field type (name "text")
    TokenStream ts1 = solrAnalyzer.tokenStream("text", "foo bar"); // a non-dynamic field
    TokenStream ts2 = solrAnalyzer.tokenStream("t_text", "whatever"); // a dynamic field
    assertSame(ts1, ts2);
    ts1.close();
    ts2.close();
  }
}
