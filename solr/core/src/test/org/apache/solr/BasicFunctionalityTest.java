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
package org.apache.solr;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LazyDocument;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.XMLWriter;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.util.BaseTestHarness;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests some basic functionality of Solr while demonstrating good
 * Best Practices for using SolrTestCaseJ4
 */
public class BasicFunctionalityTest extends SolrTestCaseJ4 {


  public String getCoreName() { return "basic"; }

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }
  // tests the performance of dynamic field creation and
  // field property testing.
  /*
  public void testFieldPerf() {
    IndexSchema schema = h.getCore().getSchema();
    SchemaField[] fields = schema.getDynamicFieldPrototypes();
    boolean createNew = false;

    long start = System.currentTimeMillis();
    int ret = 0;
    for (int i=0; i<10000000; i++) {
      for (SchemaField f : fields) {
        if (createNew) f = new SchemaField(f, "fakename");
        if (f.indexed()) ret += 1;
        if (f.isCompressed()) ret += 2;
        if (f.isRequired()) ret += 3;
        if (f.multiValued()) ret += 4;
        if (f.omitNorms()) ret += 5;
        if (f.sortMissingFirst()) ret += 6;
        if (f.sortMissingLast())ret += 7;
        if (f.stored()) ret += 8;
        if (f.storeTermOffsets()) ret += 9;
        if (f.storeTermPositions()) ret += 10;
        if (f.storeTermVector()) ret += 11;
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("ret=" + ret + " time="+ (end-start));
  }
  ***/
  
  @Test
  public void testIgnoredFields() throws Exception {
    lrf.args.put(CommonParams.VERSION,"2.2");
    assertU("adding doc with ignored field",
            adoc("id", "42", "foo_ignored", "blah blah"));
    assertU("commit",
            commit());
    
    // :TODO: the behavior of querying on an unindexed field should be better specified in the future.
    assertQ("query with ignored field",
            req("bar_ignored:yo id:42")
            ,"//*[@numFound='1']"
            ,"//str[@name='id'][.='42']"
            );
  }
  
  @Test
  public void testSomeStuff() throws Exception {
    clearIndex();

    SolrCore core = h.getCore();

    // test that we got the expected config, not just hardcoded defaults
    assertNotNull(core.getRequestHandler("/mock"));

    // test stats call
    SolrMetricManager manager = core.getCoreContainer().getMetricManager();
    String registry = core.getCoreMetricManager().getRegistryName();
    Map<String, Metric> metrics = manager.registry(registry).getMetrics();
    assertTrue(metrics.containsKey("CORE.coreName"));
    assertTrue(metrics.containsKey("CORE.refCount"));
    @SuppressWarnings({"unchecked"})
    Gauge<Number> g = (Gauge<Number>)metrics.get("CORE.refCount");
    assertTrue(g.getValue().intValue() > 0);

    lrf.args.put(CommonParams.VERSION,"2.2");
    assertQ("test query on empty index",
            req("qlkciyopsbgzyvkylsjhchghjrdf")
            ,"//result[@numFound='0']"
            );

    // test escaping of ";"
    assertU("deleting 42 for no reason at all",
            delI("42"));
    assertU("adding doc#42",
            adoc("id", "42", "val_s", "aa;bb"));
    assertU("does commit work?",
            commit());

    assertQ("backslash escaping semicolon",
            req("id:42 AND val_s:aa\\;bb")
            ,"//*[@numFound='1']"
            ,"//str[@name='id'][.='42']"
            );

    assertQ("quote escaping semicolon",
            req("id:42 AND val_s:\"aa;bb\"")
            ,"//*[@numFound='1']"
            ,"//str[@name='id'][.='42']"
            );

    assertQ("no escaping semicolon",
            req("id:42 AND val_s:aa")
            ,"//*[@numFound='0']"
            );

    assertU(delI("42"));
    assertU(commit());
    assertQ(req("id:42")
            ,"//*[@numFound='0']"
            );

    // test overwrite default of true

    assertU(adoc("id", "42", "val_s", "AAA"));
    assertU(adoc("id", "42", "val_s", "BBB"));
    assertU(commit());
    assertQ(req("id:42")
            ,"//*[@numFound='1']"
            ,"//str[.='BBB']"
            );
    assertU(adoc("id", "42", "val_s", "CCC"));
    assertU(adoc("id", "42", "val_s", "DDD"));
    assertU(commit());
    assertQ(req("id:42")
            ,"//*[@numFound='1']"
            ,"//str[.='DDD']"
            );

    // test deletes
    String [] adds = new String[] {
      add( doc("id","101"), "overwrite", "true" ),
      add( doc("id","101"), "overwrite", "true" ),
      add( doc("id","105"), "overwrite", "false"  ),
      add( doc("id","102"), "overwrite", "true" ),
      add( doc("id","103"), "overwrite", "false"  ),
      add( doc("id","101"), "overwrite", "true" ),
    };
    for (String a : adds) {
      assertU(a, a);
    }
    assertU(commit());

    // test maxint
    assertQ(req("q","id:[100 TO 110]", "rows","2147483647")
            ,"//*[@numFound='4']"
            );

    // test big limit
    assertQ(req("q","id:[100 TO 111]", "rows","1147483647")
            ,"//*[@numFound='4']"
            );

    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='4']"
            );
    assertU(delI("102"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='3']"
            );
    assertU(delI("105"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='2']"
            );
    assertU(delQ("id:[100 TO 110]"));
    assertU(commit());
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='0']"
            );
    
    assertU(BaseTestHarness.simpleTag("rollback"));
    assertU(commit());
  }


  /**
   * verify that delete by query works with the QParser framework and
   * pure negative queries
   */
  public void testNonTrivialDeleteByQuery() throws Exception {
    clearIndex();
    
    // setup
    assertU( add(doc("id","101", "text", "red apple" )) );
    assertU( add(doc("id","102", "text", "purple grape" )) );
    assertU( add(doc("id","103", "text", "green grape" )) );
    assertU( add(doc("id","104", "text", "green pear" )) );
    assertU( add(doc("id","105", "text", "yellow banana" )) );
    assertU( add(doc("id","106", "text", "red cherry" )) );

    // sanity checks
    assertU(commit());
    assertQ(req("id:[100 TO 110]")
            ,"//*[@numFound='6']"
            );
    assertQ(req("*:*")
            ,"//*[@numFound='6']"
            );
    assertQ(req("text:red")
            ,"//*[@numFound='2']"
            );
    assertQ(req("-text:red")
            ,"//*[@numFound='4']"
            );
    assertQ(req("text:grape")
            ,"//*[@numFound='2']"
            );
    assertQ(req("-text:grape")
            ,"//*[@numFound='4']"
            );
    assertQ(req("-text:red -text:grape")
            ,"//*[@numFound='2']"
            );
    assertQ(req("{!lucene q.op=AND df=text}grape green")
            ,"//*[@numFound='1']"
            ,"//str[@name='id'][.='103']"
             );
    assertQ(req("-_val_:\"{!lucene q.op=AND df=text}grape green\"")
            ,"//*[@numFound='5']"
            ,"//str[@name='id'][.='101']"
            ,"//str[@name='id'][.='102']"
            ,"//str[@name='id'][.='104']"
            ,"//str[@name='id'][.='105']"
            ,"//str[@name='id'][.='106']"
            );

    // tests

    assertU(delQ("-*:*")); // NOOP
    assertU(commit());
    assertQ(req("*:*")
            ,"//*[@numFound='6']"
            );

    assertU(delQ("-text:grape -text:red"));
    assertU(commit());
    assertQ(req("*:*")
            ,"//*[@numFound='4']"
            ,"//str[@name='id'][.='101']"
            ,"//str[@name='id'][.='102']"
            ,"//str[@name='id'][.='103']"
            ,"//str[@name='id'][.='106']"
            );

    assertU(delQ("{!term f=id}106"));
    assertU(commit());
    assertQ(req("*:*")
            ,"//*[@numFound='3']"
            ,"//str[@name='id'][.='101']"
            ,"//str[@name='id'][.='102']"
            ,"//str[@name='id'][.='103']"
            );

    assertU(delQ("-_val_:\"{!lucene q.op=AND df=text}grape green\""));
    assertU(commit());
    assertQ(req("*:*")
            ,"//*[@numFound='1']"
            ,"//str[@name='id'][.='103']"
            );

    assertU(delQ("-text:doesnotexist"));
    assertU(commit());
    assertQ(req("*:*")
            ,"//*[@numFound='0']"
            );

  }

  @Test
  public void testHTMLStrip() {
    assertU(add(doc("id","200", "HTMLwhitetok","&#65;&#66;&#67;")));
    assertU(add(doc("id","201", "HTMLwhitetok","&#65;B&#67;")));      // do it again to make sure reuse is working
    assertU(commit());
    assertQ(req("q","HTMLwhitetok:A&#66;C")
        ,"//*[@numFound='2']"
    );
    assertQ(req("q","HTMLwhitetok:&#65;BC")
        ,"//*[@numFound='2']"
    );
  }

  @Test
  public void testClientErrorOnMalformedDate() throws Exception {
    final String BAD_VALUE = "NOT_A_DATE";
    ignoreException(BAD_VALUE);

    final List<String> FIELDS = new LinkedList<>();
    for (String type : new String[] {
        "tdt", "tdt1", "tdtdv", "tdtdv1",
        "dt_dv", "dt_dvo", "dt", "dt1", "dt_os"
        }) {
      FIELDS.add("malformed_" + type);
    }

    // test that malformed numerics cause client error not server error
    for (String field : FIELDS) {
      SolrException e1 = expectThrows(SolrException.class,
          "Didn't encounter an error trying to add a bad date: " + field,
          () -> h.update(add( doc("id","100", field, BAD_VALUE))));
      String msg1 = e1.getMessage();
      assertTrue("not an (update) client error on field: " + field +" : "+ msg1,
          400 <= e1.code() && e1.code() < 500);
      assertTrue("(update) client error does not mention bad value: " + msg1,
          msg1.contains(BAD_VALUE));
      assertTrue("client error does not mention document id: " + msg1,
          msg1.contains("[doc=100]"));
      SchemaField sf = h.getCore().getLatestSchema().getField(field);
      if (!sf.hasDocValues() && !sf.indexed()) {
        continue;
      }
      SolrException e2 = expectThrows(SolrException.class,
          "Didn't encounter an error trying to add a bad date: " + field,
          () -> h.query(req("q",field + ":" + BAD_VALUE))
      );
      String msg2 = e2.toString();
      assertTrue("not a (search) client error on field: " + field +" : "+ msg2,
          400 <= e2.code() && e2.code() < 500);
      assertTrue("(search) client error does not mention bad value: " + msg2,
          msg2.contains(BAD_VALUE));

      SolrException e3 = expectThrows(SolrException.class,
          "Didn't encounter an error trying to add a bad date: " + field,
          () -> h.query(req("q",field + ":[NOW TO " + BAD_VALUE + "]"))
      );
      String msg3 = e3.toString();
      assertTrue("not a (search) client error on field: " + field +" : "+ msg3,
          400 <= e3.code() && e3.code() < 500);
      assertTrue("(search) client error does not mention bad value: " + msg3,
          msg3.contains(BAD_VALUE));
    }
  }

  @Test
  public void testClientErrorOnMalformedNumbers() throws Exception {

    final String BAD_VALUE = "NOT_A_NUMBER";
    ignoreException(BAD_VALUE);

    final List<String> FIELDS = new LinkedList<>();
    for (String type : new String[] {
        "ti", "tf", "td", "tl",
        "i", "f", "d", "l",
        "i_dv", "f_dv", "d_dv", "l_dv",
        "i_dvo", "f_dvo", "d_dvo", "l_dvo",
        "i_os", "f_os", "d_os", "l_os"
        }) {
      FIELDS.add("malformed_" + type);
    }

    // test that malformed numerics cause client error not server error
    for (String field : FIELDS) {
      SolrException e1 = expectThrows(SolrException.class,
          "Didn't encounter an error trying to add a non-number: " + field,
          () -> h.update(add( doc("id","100", field, BAD_VALUE))));
      String msg1 = e1.toString();
      assertTrue("not an (update) client error on field: " + field +" : "+ msg1,
          400 <= e1.code() && e1.code() < 500);
      assertTrue("(update) client error does not mention bad value: " + msg1,
          msg1.contains(BAD_VALUE));
      assertTrue("client error does not mention document id",
          msg1.contains("[doc=100]"));
      SchemaField sf = h.getCore().getLatestSchema().getField(field); 
      if (!sf.hasDocValues() && !sf.indexed()) {
        continue;
      }

      SolrException e2 = expectThrows(SolrException.class,
          "Didn't encounter an error trying to add a non-number: " + field,
          () -> h.query(req("q",field + ":" + BAD_VALUE))
      );
      String msg2 = e2.toString();
      assertTrue("not a (search) client error on field: " + field +" : "+ msg2,
          400 <= e2.code() && e2.code() < 500);
      assertTrue("(search) client error does not mention bad value: " + msg2,
          msg2.contains(BAD_VALUE));

      SolrException e3 = expectThrows(SolrException.class,
          "Didn't encounter an error trying to add a non-number: " + field,
          () -> h.query(req("q",field + ":[10 TO " + BAD_VALUE + "]"))
      );
      String msg3 = e3.toString();
      assertTrue("not a (search) client error on field: " + field +" : "+ msg3,
          400 <= e3.code() && e3.code() < 500);
      assertTrue("(search) client error does not mention bad value: " + msg3,
          msg3.contains(BAD_VALUE));
    }
  }
  
  @Test
  @SuppressWarnings({"rawtypes"})
  public void testRequestHandlerBaseException() {
    final String tmp = "BOO! ignore_exception";
    SolrRequestHandler handler = new RequestHandlerBase() {
        @Override
        public String getDescription() { return tmp; }
        @Override
        public void handleRequestBody
          ( SolrQueryRequest req, SolrQueryResponse rsp ) {
          throw new RuntimeException(tmp);
        }
      };
    handler.init(new NamedList());
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = req();
    h.getCore().execute(handler, 
                        req,
                        rsp);
    assertNotNull("should have found an exception", rsp.getException());
    req.close();                    
  }

  @Test
  public void testMultipleUpdatesPerAdd() {
    clearIndex();
    // big freaking kludge since the response is currently not well formed.
    String res = h.update("<add><doc><field name=\"id\">1</field></doc><doc><field name=\"id\">2</field></doc></add>");
    // assertEquals("<result status=\"0\"></result>", res);
    assertU("<commit/>");
    assertQ(req("id:[0 TO 99]")
            ,"//*[@numFound='2']"
            );

  }

  @Test
  public void testDocBoost() throws Exception {
    String res = h.update("<add>" + "<doc><field name=\"id\">1</field>"+
                                          "<field name=\"text\">hello</field></doc>" + 
                          "<doc boost=\"2.0\"><field name=\"id\">2</field>" +
                                          "<field name=\"text\">hello</field></doc>" + 
                          "</add>");

    // assertEquals("<result status=\"0\"></result>", res);
    assertU("<commit/>");
    assertQ(req("text:hello")
            ,"//*[@numFound='2']"
            );
    String resp = h.query(lrf.makeRequest("q", "text:hello", CommonParams.DEBUG_QUERY, "true"));
    //System.out.println(resp);
    // second doc ranked first
    assertTrue( resp.indexOf("\"2\"") < resp.indexOf("\"1\"") );
  }

  @Test
  public void testFieldBoost() throws Exception {
    String res = h.update("<add>" + "<doc><field name=\"id\">1</field>"+
                                      "<field name=\"text\">hello</field></doc>" + 
                                    "<doc><field name=\"id\">2</field>" +
                                      "<field boost=\"2.0\" name=\"text\">hello</field></doc>" + 
                          "</add>");

    // assertEquals("<result status=\"0\"></result>", res);
    assertU("<commit/>");
    assertQ(req("text:hello"),
            "//*[@numFound='2']"
            );
    String resp = h.query(lrf.makeRequest("q", "text:hello", CommonParams.DEBUG_QUERY, "true"));
    //System.out.println(resp);
    // second doc ranked first
    assertTrue( resp.indexOf("\"2\"") < resp.indexOf("\"1\"") );
  }

  @Test
  public void testXMLWriter() throws Exception {

    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.add("\"quoted\"", "\"value\"");

    StringWriter writer = new StringWriter(32000);
    SolrQueryRequest req = req("foo");
    XMLWriter.writeResponse(writer,req,rsp);

    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    builder.parse(new ByteArrayInputStream
                  (writer.toString().getBytes(StandardCharsets.UTF_8)));
    req.close();
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testLocalSolrQueryRequestParams() {
    @SuppressWarnings({"rawtypes"})
    HashMap args = new HashMap();
    args.put("string", "string value");
    args.put("array", new String[] {"array", "value"});
    SolrQueryRequest req = new LocalSolrQueryRequest(null, null, null, 0, 20, args);
    assertEquals("string value", req.getParams().get("string"));
    assertEquals("array", req.getParams().get("array"));

    String[] stringParams = req.getParams().getParams("string");
    assertEquals(1, stringParams.length);
    assertEquals("string value", stringParams[0]);

    String[] arrayParams = req.getParams().getParams("array");
    assertEquals(2, arrayParams.length);
    assertEquals("array", arrayParams[0]);
    assertEquals("value", arrayParams[1]);
    req.close();
  }

  @Test
  public void testKeywordTokenizerFactory() {

    assertU(adoc("id", "42",
                 "keywordtok", "How nOw broWn-ish C.o.w. ?"));
    assertU(commit());
    assertQ("stored value matches?",
            req("id:42")
            ,"//str[.='How nOw broWn-ish C.o.w. ?']"
            );
    assertQ("query on exact matches?",
            req("keywordtok:\"How nOw broWn-ish C.o.w. ?\"")
            ,"//str[.='How nOw broWn-ish C.o.w. ?']"
            );
  }

  @Test
  public void testTermVectorFields() {
    
    IndexSchema ischema = IndexSchemaFactory.buildIndexSchema(getSchemaFile(), solrConfig);
    SchemaField f; // Solr field type
    IndexableField luf; // Lucene field

    f = ischema.getField("test_basictv");
    luf = f.createField("test");
    assertTrue(f.storeTermVector());
    assertTrue(luf.fieldType().storeTermVectors());

    f = ischema.getField("test_notv");
    luf = f.createField("test");
    assertTrue(!f.storeTermVector());
    assertTrue(!luf.fieldType().storeTermVectors());

    f = ischema.getField("test_postv");
    luf = f.createField("test");
    assertTrue(f.storeTermVector() && f.storeTermPositions());
    assertTrue(luf.fieldType().storeTermVectorPositions());

    f = ischema.getField("test_offtv");
    luf = f.createField("test");
    assertTrue(f.storeTermVector() && f.storeTermOffsets());
    assertTrue(luf.fieldType().storeTermVectorOffsets());

    f = ischema.getField("test_posofftv");
    luf = f.createField("test");
    assertTrue(f.storeTermVector() && f.storeTermPositions() && f.storeTermOffsets());
    assertTrue(luf.fieldType().storeTermVectorOffsets() && luf.fieldType().storeTermVectorPositions());

    f = ischema.getField("test_posoffpaytv");
    luf = f.createField("test");
    assertTrue(f.storeTermVector() && f.storeTermPositions() && f.storeTermOffsets() && f.storeTermPayloads());
    assertTrue(luf.fieldType().storeTermVectorOffsets() && luf.fieldType().storeTermVectorPositions() && luf.fieldType().storeTermVectorPayloads());

  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testSolrParams() throws Exception {
    @SuppressWarnings({"rawtypes"})
    NamedList nl = new NamedList();
    nl.add("i",555);
    nl.add("s","bbb");
    nl.add("bt","true");
    nl.add("bf","false");

    Map<String,String> m = new HashMap<>();
    m.put("f.field1.i", "1000");
    m.put("s", "BBB");
    m.put("ss", "SSS");

    LocalSolrQueryRequest req = new LocalSolrQueryRequest(null,nl);
    SolrParams p = req.getParams();

    assertEquals(p.get("i"), "555");
    assertEquals(p.getInt("i").intValue(), 555);
    assertEquals(p.getInt("i",5), 555);
    assertEquals(p.getInt("iii",5), 5);
    assertEquals(p.getFieldParam("field1","i"), "555");

    req.setParams(SolrParams.wrapDefaults(p, new MapSolrParams(m)));
    p = req.getParams();
    assertEquals(req.getOriginalParams().get("s"), "bbb");
    assertEquals(p.get("i"), "555");
    assertEquals(p.getInt("i").intValue(), 555);
    assertEquals(p.getInt("i",5), 555);
    assertEquals(p.getInt("iii",5), 5);

    assertEquals(p.getFieldParam("field1","i"), "1000");
    assertEquals(p.get("s"), "bbb");
    assertEquals(p.get("ss"), "SSS");

    assertEquals(!!p.getBool("bt"), !p.getBool("bf"));
    assertEquals(p.getBool("foo",true), true);
    assertEquals(p.getBool("foo",false), false);
    assertEquals(!!p.getBool("bt"), !p.getBool("bf"));

    @SuppressWarnings({"rawtypes"})
    NamedList more = new NamedList();
    more.add("s", "aaa");
    more.add("s", "ccc");
    more.add("ss","YYY");
    more.add("xx","XXX");
    p = SolrParams.wrapAppended(p, more.toSolrParams());
    assertEquals(3, p.getParams("s").length);
    assertEquals("bbb", p.getParams("s")[0]);
    assertEquals("aaa", p.getParams("s")[1]);
    assertEquals("ccc", p.getParams("s")[2]);
    assertEquals(3, p.getParams("s").length);
    assertEquals("SSS", p.get("ss"));
    assertEquals("XXX", p.get("xx"));

    req.close();
  }

  @Test
  public void testDefaultFieldValues() {
    clearIndex();
    lrf.args.put(CommonParams.VERSION,"2.2");
    assertU(adoc("id",  "4055",
                 "subject", "Hoss the Hoss man Hostetter"));
    assertU(adoc("id",  "4056",
                 "intDefault", "4",
                 "subject", "Some Other Guy"));
    assertU(adoc("id",  "4057",
                 "multiDefault", "a",
                 "multiDefault", "b",
                 "subject", "The Dude"));
    assertU(commit());

    assertQ("everthing should have recent timestamp",
            req("timestamp:[NOW-10MINUTES TO NOW]")
            ,"*[count(//doc)=3]"
            ,"//date[@name='timestamp']"
            );
    
    assertQ("2 docs should have the default for multiDefault",
            req("multiDefault:muLti-Default")
            ,"*[count(//doc)=2]"
            ,"//arr[@name='multiDefault']"
            );
    assertQ("1 doc should have its explicit multiDefault",
            req("multiDefault:a")
            ,"*[count(//doc)=1]"
            );

    assertQ("2 docs should have the default for intDefault",
            req("intDefault:42")
            ,"*[count(//doc)=2]"
            );
    assertQ("1 doc should have its explicit intDefault",
            req("intDefault:[3 TO 5]")
            ,"*[count(//doc)=1]"
            );
    
  }

  @Test
  public void testTokenizer() {

    assertU(adoc("id",  "4055",
            "patterntok", "Hello,There"));
    assertU(adoc("id",  "4056",
            "patterntok", "Goodbye,Now"));
    assertU(commit());

    assertQ("make sure it split ok",
            req("patterntok:Hello")
            ,"*[count(//doc)=1]"
    );
    assertQ("make sure it split ok",
            req("patterntok:Goodbye")
            ,"*[count(//doc)=1]"
    );
  }

  @Test
  public void testConfigDefaults() {
    assertU(adoc("id", "42",
                 "name", "Zapp Brannigan"));
    assertU(adoc("id", "43",
                 "title", "Democratic Order of Planets"));
    assertU(adoc("id", "44",
                 "name", "The Zapper"));
    assertU(adoc("id", "45",
                 "title", "25 star General"));
    assertU(adoc("id", "46",
                 "subject", "Defeated the pacifists of the Gandhi nebula"));
    assertU(adoc("id", "47",
                 "text", "line up and fly directly at the enemy death cannons, clogging them with wreckage!"));
    assertU(commit());

    assertQ("standard request handler returns all matches",
            req("id:[42 TO 47]"),
            "*[count(//doc)=6]"
            );

    assertQ("defaults handler returns fewer matches",
            req("q", "id:[42 TO 47]",   "qt","/defaults"),
            "*[count(//doc)=4]"
            );

    assertQ("defaults handler includes highlighting",
            req("q", "name:Zapp OR title:General",   "qt","/defaults"),
            "//lst[@name='highlighting']"
            );

  }

  private String mkstr(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append((char)(65 + i%26));
    }
    return new String(sb);
  }   

  @Test
  public void testNotLazyField() throws IOException {

    assertU(adoc("id", "7777",
                 "title", "keyword",
                 "test_hlt", mkstr(20000)));

    assertU(commit());
    SolrCore core = h.getCore();
   
    SolrQueryRequest req = req("q", "id:7777", "fl", "id,title,test_hlt");
    SolrQueryResponse rsp = new SolrQueryResponse();
    core.execute(core.getRequestHandler(req.getParams().get(CommonParams.QT)), req, rsp);

    DocList dl = ((ResultContext) rsp.getResponse()).getDocList();
    Document d = req.getSearcher().doc(dl.iterator().nextDoc());
    // ensure field in fl is not lazy
    assertFalse( ((Field) d.getField("test_hlt")).getClass().getSimpleName().equals("LazyField"));
    assertFalse( ((Field) d.getField("title")).getClass().getSimpleName().equals("LazyField"));
    req.close();
  }

  @Test
  public void testLazyField() throws IOException {
    assertU(adoc("id", "7777",
                 "title", "keyword",
                 "test_hlt", mkstr(10000),
                 "test_hlt", mkstr(20000),
                 "test_hlt", mkstr(30000),
                 "test_hlt", mkstr(40000)));

    assertU(commit());
    SolrCore core = h.getCore();
    
    // initial request
    SolrQueryRequest req = req("q", "id:7777", "fl", "id,title");
    SolrQueryResponse rsp = new SolrQueryResponse();
    core.execute(core.getRequestHandler(req.getParams().get(CommonParams.QT)), req, rsp);

    DocList dl = ((ResultContext) rsp.getResponse()).getDocList();
    DocIterator di = dl.iterator();    
    Document d1 = req.getSearcher().doc(di.nextDoc());
    IndexableField[] values1 = null;

    // ensure fl field is non lazy, and non-fl field is lazy
    assertFalse( d1.getField("title") instanceof LazyDocument.LazyField);
    assertFalse( d1.getField("id") instanceof LazyDocument.LazyField);
    values1 = d1.getFields("test_hlt");
    assertEquals(4, values1.length);
    for (int i = 0; i < values1.length; i++) {
      assertTrue( values1[i] instanceof LazyDocument.LazyField );
      LazyDocument.LazyField f = (LazyDocument.LazyField) values1[i];
      assertFalse( f.hasBeenLoaded() );
    }
    req.close();

    // followup request, different fl
    req = req("q", "id:7777", "fl", "id,test_hlt");
    rsp = new SolrQueryResponse();
    core.execute(core.getRequestHandler(req.getParams().get(CommonParams.QT)), req, rsp);

    dl = ((ResultContext) rsp.getResponse()).getDocList();
    di = dl.iterator();    
    Document d2 = req.getSearcher().doc(di.nextDoc());
    // ensure same doc, same lazy field now
    assertTrue("Doc was not cached", d1 == d2);
    IndexableField[] values2 = d2.getFields("test_hlt");
    assertEquals(values1.length, values2.length);
    for (int i = 0; i < values1.length; i++) {
      assertSame("LazyField wasn't reused", 
                 values1[i], values2[i]);
      LazyDocument.LazyField f = (LazyDocument.LazyField) values1[i];
      // still not a real boy, no response writer in play
      assertFalse(f.hasBeenLoaded()); 
    }

    assertNotNull(values2[0].stringValue()); // actuallize one value
    for (int i = 0; i < values2.length; i++) {
      // now all values for this field should be loaded & cached
      LazyDocument.LazyField f = (LazyDocument.LazyField) values2[i];
      assertTrue(f.hasBeenLoaded());
    }

    req.close();
  } 
            

  /** @see org.apache.solr.util.DateMathParserTest */
  @Test
  public void testDateMath() {
    clearIndex();

    // testing everything from query level is hard because
    // time marches on ... and there is no easy way to reach into the
    // bowels of DatePointField and muck with the definition of "now"
    //    ...
    // BUT: we can test that crazy combinations of "NOW" all work correctly,
    // assuming the test doesn't take too long to run...

    final String july4 = "1976-07-04T12:08:56.235Z";
    assertU(adoc("id", "1",  "bday", july4));
    assertU(adoc("id", "2",  "bday", "NOW"));
    assertU(adoc("id", "3",  "bday", "NOW/HOUR"));
    assertU(adoc("id", "4",  "bday", "NOW-30MINUTES"));
    assertU(adoc("id", "5",  "bday", "NOW+30MINUTES"));
    assertU(adoc("id", "6",  "bday", "NOW+2YEARS"));
    assertU(commit());
    
    // a ridiculoulsy long date math expression that's still equivalent to july4
    final StringBuilder july4Long = new StringBuilder(july4);
    final int iters = atLeast(10);
    for (int i = 0; i < iters; i++) {
      final String val = String.valueOf(atLeast(1));
      july4Long.append("+" + val + "SECONDS-" + val + "SECONDS");
    }

    // term queries using date math (all of these should match doc#1)
    for (String q : 
           new String[] {
             "bday:1976-07-04T12\\:08\\:56.45Z/SECOND+235MILLIS",
             "bday:1976-07-04T12\\:08\\:56.123Z/MINUTE+56SECONDS+235MILLIS",
             "bday:\"1976-07-04T12:08:56.45Z/SECOND+235MILLIS\"",
             "bday:\"1976-07-04T12:08:56.123Z/MINUTE+56SECONDS+235MILLIS\"",
             "{!term f=bday}1976-07-04T12:08:56.45Z/SECOND+235MILLIS",
             "{!term f=bday}1976-07-04T12:08:56.123Z/MINUTE+56SECONDS+235MILLIS",             
             "{!term f=bday}"+july4,
             "{!term f=bday}"+july4Long,
             "bday:\"" + july4Long + "\""
           }) {
      assertQ("check math on field query: " + q,
              req("q", q),
              "*[count(//doc)=1]",
              "//str[@name='id'][.='1']");
    }

    // range queries using date math
    assertQ("check math on absolute date#1",
            req("q", "bday:[* TO "+july4+"/SECOND]"),
            "*[count(//doc)=0]");
    assertQ("check math on absolute date#2",
            req("q", "bday:[* TO "+july4+"/SECOND+1SECOND]"),
            "*[count(//doc)=1]");
    assertQ("check math on absolute date#3",
            req("q", "bday:["+july4+"/SECOND TO "+july4+"/SECOND+1SECOND]"),
            "*[count(//doc)=1]");
    assertQ("check math on absolute date#4",
            req("q", "bday:["+july4+"/MINUTE+1MINUTE TO *]"),
            "*[count(//doc)=5]");
    
    assertQ("check count for before now",
            req("q", "bday:[* TO NOW]"), "*[count(//doc)=4]");

    assertQ("check count for after now",
            req("q", "bday:[NOW TO *]"), "*[count(//doc)=2]");

    assertQ("check count for old stuff",
            req("q", "bday:[* TO NOW-2YEARS]"), "*[count(//doc)=1]");

    assertQ("check count for future stuff",
            req("q", "bday:[NOW+1MONTH TO *]"), "*[count(//doc)=1]");

    assertQ("check count for near stuff",
            req("q", "bday:[NOW-1MONTH TO NOW+2HOURS]"), "*[count(//doc)=4]");
    
    assertQ("check counts using fixed NOW",
            req("q", "bday:[NOW/DAY TO NOW/DAY+1DAY]",
                "NOW", "205369736000" // 1976-07-04T23:08:56.235Z
                ),
            "*[count(//doc)=1]");
                
    assertQ("check counts using fixed NOW and TZ rounding",
            req("q", "bday:[NOW/DAY TO NOW/DAY+1DAY]",
                "TZ", "GMT+01",
                "NOW", "205369736000" // 1976-07-04T23:08:56.235Z
                ),
            "*[count(//doc)=0]");

  }

  // commented after SOLR-8904; both are false
//  public void testDateRoundtrip() {
//    assertU(adoc("id", "99",  "bday", "99-01-01T12:34:56.789Z"));
//    assertU(commit());
//    assertQ("year should be canonicallized to 4 digits",
//            req("q", "id:99"),
//            "//date[@name='bday'][.='0099-01-01T12:34:56.789Z']");
//    assertU(adoc("id", "99",  "bday", "1999-01-01T12:34:56.900Z"));
//    assertU(commit());
//    assertQ("millis should be canonicallized to no trailing zeros",
//            req("q", "id:99"),
//            "//date[@name='bday'][.='1999-01-01T12:34:56.9Z']");
//  }
  
  @Test
  public void testPatternReplaceFilter() {

    assertU(adoc("id", "1",
                 "patternreplacefilt", "My  fine-feathered friend!"));
    assertU(adoc("id", "2",
                 "patternreplacefilt", "  What's Up Doc?"));
    assertU(commit());
 
    assertQ("don't find Up",
            req("q", "patternreplacefilt:Up"),
            "*[count(//doc)=0]");
    
    assertQ("find doc",
            req("q", "patternreplacefilt:__What_s_Up_Doc_"),
            "*[count(//doc)=1]");

    assertQ("find birds",
            req("q", "patternreplacefilt:My__fine_feathered_friend_"),
            "*[count(//doc)=1]");
  }

  @Test
  public void testAbuseOfSort() {

    assertU(adoc("id", "9999991",
                 "sortabuse_not_uninvertible", "xxx",
                 "sortabuse_t", "zzz xxx ccc vvv bbb nnn aaa sss ddd fff ggg"));
    assertU(adoc("id", "9999992",
                 "sortabuse_not_uninvertible", "yyy",
                 "sortabuse_t", "zzz xxx ccc vvv bbb nnn qqq www eee rrr ttt"));

    assertU(commit());

    for (String f : Arrays.asList("sortabuse_not_uninvertible", "sortabuse_t")) {
      RuntimeException outerEx = expectThrows(RuntimeException.class, () -> {
          ignoreException("sortabuse");
          assertQ("sort on something that shouldn't work",
                  req("q", "*:*",
                      "sort", f+ " asc"),
                  "*[count(//doc)=2]");
        });
      Throwable root = getRootCause(outerEx);
      assertEquals("sort exception root cause",
                   SolrException.class, root.getClass());
      SolrException e = (SolrException) root;
      assertEquals("incorrect error type",
                   SolrException.ErrorCode.BAD_REQUEST,
                   SolrException.ErrorCode.getErrorCode(e.code()));
      assertTrue("exception doesn't contain field name",
                 e.getMessage().contains(f));
    }
  }
  
//   /** this doesn't work, but if it did, this is how we'd test it. */
//   public void testOverwriteFalse() {

//     assertU(adoc("id", "overwrite", "val_s", "AAA"));
//     assertU(commit());

//     assertU(add(doc("id", "overwrite", "val_s", "BBB")
//                 ,"allowDups", "false"
//                 ,"overwriteCommitted","false"
//                 ,"overwritePending","false"
//                 ));
//     assertU(commit());
//     assertQ(req("id:overwrite")
//             ,"//*[@numFound='1']"
//             ,"//str[.='AAA']"
//             );
//   }


}
