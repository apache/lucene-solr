/**
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

import org.apache.lucene.document.*;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.solr.common.params.AppendedSolrParams;
import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.*;
import org.apache.solr.handler.*;
import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.apache.solr.schema.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.io.IOException;
import java.io.StringWriter;
import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.HashMap;

/**
 * Tests some basic functionality of Solr while demonstrating good
 * Best Practices for using AbstractSolrTestCase
 */
public class BasicFunctionalityTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  public String getCoreName() { return "basic"; }

  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
  }
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.tearDown();

  }
  
  public void testIgnoredFields() throws Exception {
    lrf.args.put("version","2.0");
    assertU("adding doc with ignored field",
            adoc("id", "42", "foo_ignored", "blah blah"));
    assertU("commit",
            commit());
    
    // :TODO: the behavior of querying on an unindexed field should be better specified in the future.
    assertQ("query with ignored field",
            req("bar_ignored:yo id:42")
            ,"//*[@numFound='1']"
            ,"//int[@name='id'][.='42']"
            );
  }

  public void testSomeStuff() throws Exception {
    lrf.args.put("version","2.0");
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
            ,"//int[@name='id'][.='42']"
            );

    assertQ("quote escaping semicolon",
            req("id:42 AND val_s:\"aa;bb\"")
            ,"//*[@numFound='1']"
            ,"//int[@name='id'][.='42']"
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

    // test allowDups default of false

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
      add( doc("id","101"), "allowDups", "false" ),
      add( doc("id","101"), "allowDups", "false" ),
      add( doc("id","105"), "allowDups", "true"  ),
      add( doc("id","102"), "allowDups", "false" ),
      add( doc("id","103"), "allowDups", "true"  ),
      add( doc("id","101"), "allowDups", "false" ),
    };
    for (String a : adds) {
      assertU(a, a);
    }
    assertU(commit());
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
  }

  public void testRequestHandlerBaseException() {
    final String tmp = "BOO!";
    SolrRequestHandler handler = new RequestHandlerBase() {
        public String getDescription() { return tmp; }
        public String getSourceId() { return tmp; }
        public String getSource() { return tmp; }
        public String getVersion() { return tmp; }
        public void handleRequestBody
          ( SolrQueryRequest req, SolrQueryResponse rsp ) {
          throw new RuntimeException(tmp);
        }
      };
    handler.init(new NamedList());
    SolrQueryResponse rsp = new SolrQueryResponse();
    h.getCore().execute(handler, 
                        new LocalSolrQueryRequest(h.getCore(),
                                                  new NamedList()),
                        rsp);
    assertNotNull("should have found an exception", rsp.getException());
                        
  }

  public void testMultipleUpdatesPerAdd() {

    // big freaking kludge since the response is currently not well formed.
    String res = h.update("<add><doc><field name=\"id\">1</field></doc><doc><field name=\"id\">2</field></doc></add>");
    assertEquals("<result status=\"0\"></result>", res);
    assertU("<commit/>");
    assertQ(req("id:[0 TO 99]")
            ,"//*[@numFound='2']"
            );

  }

  public void testDocBoost() throws Exception {
    String res = h.update("<add>" + "<doc><field name=\"id\">1</field>"+
                                          "<field name=\"text\">hello</field></doc>" + 
                          "<doc boost=\"2.0\"><field name=\"id\">2</field>" +
                                          "<field name=\"text\">hello</field></doc>" + 
                          "</add>");

    assertEquals("<result status=\"0\"></result>", res);
    assertU("<commit/>");
    assertQ(req("text:hello")
            ,"//*[@numFound='2']"
            );
    String resp = h.query(lrf.makeRequest("q", "text:hello", "debugQuery", "true"));
    //System.out.println(resp);
    // second doc ranked first
    assertTrue( resp.indexOf("id=2") < resp.indexOf("id=1") );
  }

  public void testFieldBoost() throws Exception {
    String res = h.update("<add>" + "<doc><field name=\"id\">1</field>"+
                                      "<field name=\"text\">hello</field></doc>" + 
                                    "<doc><field name=\"id\">2</field>" +
                                      "<field boost=\"2.0\" name=\"text\">hello</field></doc>" + 
                          "</add>");

    assertEquals("<result status=\"0\"></result>", res);
    assertU("<commit/>");
    assertQ(req("text:hello"),
            "//*[@numFound='2']"
            );
    String resp = h.query(lrf.makeRequest("q", "text:hello", "debugQuery", "true"));
    //System.out.println(resp);
    // second doc ranked first
    assertTrue( resp.indexOf("id=2") < resp.indexOf("id=1") );
  }

  public void testXMLWriter() throws Exception {

    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.add("\"quoted\"", "\"value\"");

    StringWriter writer = new StringWriter(32000);
    XMLWriter.writeResponse(writer,req("foo"),rsp);

    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    builder.parse(new ByteArrayInputStream
                  (writer.toString().getBytes("UTF-8")));
  }

  public void testLocalSolrQueryRequestParams() {
    HashMap args = new HashMap();
    args.put("string", "string value");
    args.put("array", new String[] {"array", "value"});
    SolrQueryRequest req = new LocalSolrQueryRequest(null, null, null, 0, 20, args);
    assertEquals("string value", req.getParam("string"));
    assertEquals("array", req.getParam("array"));

    String[] stringParams = req.getParams("string");
    assertEquals(1, stringParams.length);
    assertEquals("string value", stringParams[0]);

    String[] arrayParams = req.getParams("array");
    assertEquals(2, arrayParams.length);
    assertEquals("array", arrayParams[0]);
    assertEquals("value", arrayParams[1]);
  }

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

  /** @see org.apache.solr.analysis.TestRemoveDuplicatesTokenFilter */
  public void testRemoveDuplicatesTokenFilter() {
    Query q = QueryParsing.parseQuery("TV", "dedup",
                                      h.getCore().getSchema());
    assertTrue("not boolean?", q instanceof BooleanQuery);
    assertEquals("unexpected number of stemmed synonym tokens",
                 2, ((BooleanQuery) q).clauses().size());
  }

  
  public void testTermVectorFields() {
    
    IndexSchema ischema = new IndexSchema(solrConfig, getSchemaFile());
    SchemaField f; // Solr field type
    Field luf; // Lucene field

    f = ischema.getField("test_basictv");
    luf = f.createField("test", 0f);
    assertTrue(f.storeTermVector());
    assertTrue(luf.isTermVectorStored());

    f = ischema.getField("test_notv");
    luf = f.createField("test", 0f);
    assertTrue(!f.storeTermVector());
    assertTrue(!luf.isTermVectorStored());    

    f = ischema.getField("test_postv");
    luf = f.createField("test", 0f);
    assertTrue(f.storeTermVector() && f.storeTermPositions());
    assertTrue(luf.isStorePositionWithTermVector());

    f = ischema.getField("test_offtv");
    luf = f.createField("test", 0f);
    assertTrue(f.storeTermVector() && f.storeTermOffsets());
    assertTrue(luf.isStoreOffsetWithTermVector());

    f = ischema.getField("test_posofftv");
    luf = f.createField("test", 0f);
    assertTrue(f.storeTermVector() && f.storeTermPositions() && f.storeTermOffsets());
    assertTrue(luf.isStoreOffsetWithTermVector() && luf.isStorePositionWithTermVector());

  }


  public void testSolrParams() throws Exception {
    NamedList nl = new NamedList();
    nl.add("i",555);
    nl.add("s","bbb");
    nl.add("bt","true");
    nl.add("bf","false");

    Map<String,String> m = new HashMap<String,String>();
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

    req.setParams(new DefaultSolrParams(p, new MapSolrParams(m)));
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

    NamedList more = new NamedList();
    more.add("s", "aaa");
    more.add("s", "ccc");
    more.add("ss","YYY");
    more.add("xx","XXX");
    p = new AppendedSolrParams(p, SolrParams.toSolrParams(more));
    assertEquals(3, p.getParams("s").length);
    assertEquals("bbb", p.getParams("s")[0]);
    assertEquals("aaa", p.getParams("s")[1]);
    assertEquals("ccc", p.getParams("s")[2]);
    assertEquals(3, p.getParams("s").length);
    assertEquals("SSS", p.get("ss"));
    assertEquals("XXX", p.get("xx"));

    
  }


  public void testDefaultFieldValues() {
    
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
    assertQ("1 doc should have it's explicit multiDefault",
            req("multiDefault:a")
            ,"*[count(//doc)=1]"
            );

    assertQ("2 docs should have the default for intDefault",
            req("intDefault:42")
            ,"*[count(//doc)=2]"
            );
    assertQ("1 doc should have it's explicit intDefault",
            req("intDefault:[3 TO 5]")
            ,"*[count(//doc)=1]"
            );
    
  }

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
            req("q", "id:[42 TO 47]",   "qt","defaults"),
            "*[count(//doc)=4]"
            );

    assertQ("defaults handler includes highlighting",
            req("q", "name:Zapp OR title:General",   "qt","defaults"),
            "//lst[@name='highlighting']"
            );

  }
      
  public void testSimpleFacetCounts() {
    assertU(adoc("id", "42", "trait_s", "Tool", "trait_s", "Obnoxious",
                 "name", "Zapp Brannigan"));
    assertU(adoc("id", "43" ,
                 "title", "Democratic Order of Planets"));
    assertU(adoc("id", "44", "trait_s", "Tool",
                 "name", "The Zapper"));
    assertU(adoc("id", "45", "trait_s", "Chauvinist",
                 "title", "25 star General"));
    assertU(adoc("id", "46", "trait_s", "Obnoxious",
                 "subject", "Defeated the pacifists of the Gandhi nebula"));
    assertU(adoc("id", "47", "trait_s", "Pig",
                 "text", "line up and fly directly at the enemy death cannons, clogging them with wreckage!"));
    assertU(commit());
 
    assertQ("standard request handler returns all matches",
            req("id:[42 TO 47]"),
            "*[count(//doc)=6]"
            );
 
    assertQ("filter results using fq",
            req("q","id:[42 TO 46]",
                "fq", "id:[43 TO 47]"),
            "*[count(//doc)=4]"
            );
    
    assertQ("don't filter results using blank fq",
            req("q","id:[42 TO 46]",
                "fq", " "),
            "*[count(//doc)=5]"
            );
     
    assertQ("filter results using multiple fq params",
            req("q","id:[42 TO 46]",
                "fq", "trait_s:Obnoxious",
                "fq", "id:[43 TO 47]"),
            "*[count(//doc)=1]"
            );
 
    assertQ("check counts for facet queries",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.query", "trait_s:Obnoxious"
                ,"facet.query", "id:[42 TO 45]"
                ,"facet.query", "id:[43 TO 47]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=6]"
 
            ,"//lst[@name='facet_counts']/lst[@name='facet_queries']"
            ,"//lst[@name='facet_queries']/int[@name='trait_s:Obnoxious'][.='2']"
            ,"//lst[@name='facet_queries']/int[@name='id:[42 TO 45]'][.='4']"
            ,"//lst[@name='facet_queries']/int[@name='id:[43 TO 47]'][.='5']"
 
            ,"//lst[@name='facet_counts']/lst[@name='facet_fields']"
            ,"//lst[@name='facet_fields']/lst[@name='trait_s']"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='1']"
            );
 
    assertQ("check counts for applied facet queries using filtering (fq)",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.query", "id:[42 TO 45]"
                ,"facet.query", "id:[43 TO 47]"
                )
            ,"*[count(//doc)=4]"
            ,"//lst[@name='facet_counts']/lst[@name='facet_queries']"
            ,"//lst[@name='facet_queries']/int[@name='id:[42 TO 45]'][.='4']"
            ,"//lst[@name='facet_queries']/int[@name='id:[43 TO 47]'][.='3']"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='0']"
            );
 
    assertQ("check counts with facet.zero=false&facet.missing=true using fq",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.zeros", "false"
                ,"f.trait_s.facet.missing", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=4]"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[not(@name)][.='1']"
            );

    assertQ("check counts with facet.mincount=1&facet.missing=true using fq",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.mincount", "1"
                ,"f.trait_s.facet.missing", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=4]"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[not(@name)][.='1']"
            );

    assertQ("check counts with facet.mincount=2&facet.missing=true using fq",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.mincount", "2"
                ,"f.trait_s.facet.missing", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=4]"
            ,"*[count(//lst[@name='trait_s']/int)=2]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[not(@name)][.='1']"               
            );

    assertQ("check sorted paging",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.mincount","0"
                ,"facet.offset","0"
                ,"facet.limit","4"
                )
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='0']"
            );

    assertQ("check sorted paging",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.mincount","0"
                ,"facet.offset","0"
                ,"facet.limit","3"
                )
            ,"*[count(//lst[@name='trait_s']/int)=3]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            );

  }
 
  public void testDateFacets() {
    final String f = "bday";
    final String pre = "//lst[@name='facet_dates']/lst[@name='"+f+"']";

    assertU(adoc("id", "1",  f, "1976-07-04T12:08:56.235Z"));
    assertU(adoc("id", "2",  f, "1976-07-05T00:00:00.000Z"));
    assertU(adoc("id", "3",  f, "1976-07-15T00:07:67.890Z"));
    assertU(adoc("id", "4",  f, "1976-07-21T00:07:67.890Z"));
    assertU(adoc("id", "5",  f, "1976-07-13T12:12:25.255Z"));
    assertU(adoc("id", "6",  f, "1976-07-03T17:01:23.456Z"));
    assertU(adoc("id", "7",  f, "1976-07-12T12:12:25.255Z"));
    assertU(adoc("id", "8",  f, "1976-07-15T15:15:15.155Z"));
    assertU(adoc("id", "9",  f, "1907-07-12T13:13:23.235Z"));
    assertU(adoc("id", "10", f, "1976-07-03T11:02:45.678Z"));
    assertU(adoc("id", "11", f, "1907-07-12T12:12:25.255Z"));
    assertU(adoc("id", "12", f, "2007-07-30T07:07:07.070Z"));
    assertU(adoc("id", "13", f, "1976-07-30T22:22:22.222Z"));
    assertU(adoc("id", "14", f, "1976-07-05T22:22:22.222Z"));
    assertU(commit());

    assertQ("check counts for month of facet by day",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.date", f
                ,"facet.date.start", "1976-07-01T00:00:00.000Z"
                ,"facet.date.end",   "1976-07-01T00:00:00.000Z+1MONTH"
                ,"facet.date.gap",   "+1DAY"
                ,"facet.date.other", "all"
                )
            // 31 days + pre+post+inner = 34
            ,"*[count("+pre+"/int)=34]"
            ,pre+"/int[@name='1976-07-01T00:00:00.000Z'][.='0'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00.000Z'][.='0'  ]"
            ,pre+"/int[@name='1976-07-03T00:00:00.000Z'][.='2'  ]"
            // july4th = 2 because exists doc @ 00:00:00.000 on July5
            // (date faceting is inclusive)
            ,pre+"/int[@name='1976-07-04T00:00:00.000Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00.000Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00.000Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00.000Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00.000Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-16T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-17T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-18T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-19T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-21T00:00:00.000Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-22T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-23T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-24T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-25T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-26T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-27T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-28T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-29T00:00:00.000Z'][.='0']"
            ,pre+"/int[@name='1976-07-30T00:00:00.000Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-31T00:00:00.000Z'][.='0']"
            
            ,pre+"/int[@name='before' ][.='2']"
            ,pre+"/int[@name='after'  ][.='1']"
            ,pre+"/int[@name='between'][.='11']"
            
            );

    assertQ("check hardend=false",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.date", f
                ,"facet.date.start",  "1976-07-01T00:00:00.000Z"
                ,"facet.date.end",    "1976-07-13T00:00:00.000Z"
                ,"facet.date.gap",    "+5DAYS"
                ,"facet.date.other",  "all"
                ,"facet.date.hardend","false"
                )
            // 3 gaps + pre+post+inner = 6
            ,"*[count("+pre+"/int)=6]"
            ,pre+"/int[@name='1976-07-01T00:00:00.000Z'][.='5'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00.000Z'][.='0'  ]"
            ,pre+"/int[@name='1976-07-11T00:00:00.000Z'][.='4'  ]"
            
            ,pre+"/int[@name='before' ][.='2']"
            ,pre+"/int[@name='after'  ][.='3']"
            ,pre+"/int[@name='between'][.='9']"
            );

    assertQ("check hardend=true",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.date", f
                ,"facet.date.start",  "1976-07-01T00:00:00.000Z"
                ,"facet.date.end",    "1976-07-13T00:00:00.000Z"
                ,"facet.date.gap",    "+5DAYS"
                ,"facet.date.other",  "all"
                ,"facet.date.hardend","true"
                )
            // 3 gaps + pre+post+inner = 6
            ,"*[count("+pre+"/int)=6]"
            ,pre+"/int[@name='1976-07-01T00:00:00.000Z'][.='5'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00.000Z'][.='0'  ]"
            ,pre+"/int[@name='1976-07-11T00:00:00.000Z'][.='1'  ]"
            
            ,pre+"/int[@name='before' ][.='2']"
            ,pre+"/int[@name='after'  ][.='6']"
            ,pre+"/int[@name='between'][.='6']"
            );
    
  }

  public void testFacetMultiValued() {
    doFacets("t_s");
    doFacets("t_s", "facet.enum.cache.minDf", "2");
    doFacets("t_s", "facet.enum.cache.minDf", "100");
  }

  public void testFacetSingleValued() {
    doFacets("t_s1");
  }

  public void doFacets(String f, String... params) {
    String pre = "//lst[@name='"+f+"']";
    String notc = "id:[* TO *] -"+f+":C";

    assertU(adoc("id", "1",  f, "A"));
    assertU(adoc("id", "2",  f, "B"));
    assertU(adoc("id", "3",  f, "C"));
    assertU(adoc("id", "4",  f, "C"));
    assertU(adoc("id", "5",  f, "D"));
    assertU(adoc("id", "6",  f, "E"));
    assertU(adoc("id", "7",  f, "E"));
    assertU(adoc("id", "8",  f, "E"));
    assertU(adoc("id", "9",  f, "F"));
    assertU(adoc("id", "10", f, "G"));
    assertU(adoc("id", "11", f, "G"));
    assertU(adoc("id", "12", f, "G"));
    assertU(adoc("id", "13", f, "G"));
    assertU(adoc("id", "14", f, "G"));
    assertU(commit());

    assertQ("check counts for unlimited facet",
            req(params, "q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=7]"

            ,pre+"/int[@name='G'][.='5']"
            ,pre+"/int[@name='E'][.='3']"
            ,pre+"/int[@name='C'][.='2']"

            ,pre+"/int[@name='A'][.='1']"
            ,pre+"/int[@name='B'][.='1']"
            ,pre+"/int[@name='D'][.='1']"
            ,pre+"/int[@name='F'][.='1']"
            );

    assertQ("check counts for facet with generous limit",
            req(params, "q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.limit", "100"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=7]"

            ,pre+"/int[1][@name='G'][.='5']"
            ,pre+"/int[2][@name='E'][.='3']"
            ,pre+"/int[3][@name='C'][.='2']"

            ,pre+"/int[@name='A'][.='1']"
            ,pre+"/int[@name='B'][.='1']"
            ,pre+"/int[@name='D'][.='1']"
            ,pre+"/int[@name='F'][.='1']"
            );

    assertQ("check counts for limited facet",
            req(params, "q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.limit", "2"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"

            ,pre+"/int[1][@name='G'][.='5']"
            ,pre+"/int[2][@name='E'][.='3']"
            );

   assertQ("check offset",
            req(params, "q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.offset", "1"
                ,"facet.limit", "1"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"

            ,pre+"/int[1][@name='E'][.='3']"
            );

    assertQ("test sorted facet paging with zero (don't count in limit)",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","0"
                ,"facet.limit","6"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=6]"
            ,pre+"/int[1][@name='G'][.='5']"
            ,pre+"/int[2][@name='E'][.='3']"
            ,pre+"/int[3][@name='A'][.='1']"
            ,pre+"/int[4][@name='B'][.='1']"
            ,pre+"/int[5][@name='D'][.='1']"
            ,pre+"/int[6][@name='F'][.='1']"
            );

    assertQ("test sorted facet paging with zero (test offset correctness)",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","3"
                ,"facet.limit","2"
                ,"facet.sort","true"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='B'][.='1']"
            ,pre+"/int[2][@name='D'][.='1']"
            );

   assertQ("test facet unsorted paging",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","0"
                ,"facet.limit","6"
                ,"facet.sort","false"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=6]"
            ,pre+"/int[1][@name='A'][.='1']"
            ,pre+"/int[2][@name='B'][.='1']"
            ,pre+"/int[3][@name='D'][.='1']"
            ,pre+"/int[4][@name='E'][.='3']"
            ,pre+"/int[5][@name='F'][.='1']"
            ,pre+"/int[6][@name='G'][.='5']"
            );

   assertQ("test facet unsorted paging",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","3"
                ,"facet.limit","2"
                ,"facet.sort","false"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='E'][.='3']"
            ,pre+"/int[2][@name='F'][.='1']"
            );

    assertQ("test facet unsorted paging, mincount=2",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","2"
                ,"facet.offset","1"
                ,"facet.limit","2"
                ,"facet.sort","false"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='G'][.='5']"
            );
  }



  public void testFacetPrefixMultiValued() {
    doFacetPrefix("t_s");   
    doFacetPrefix("t_s", "facet.enum.cache.minDf", "3");
    doFacetPrefix("t_s", "facet.enum.cache.minDf", "100");
  }

  public void testFacetPrefixSingleValued() {
    doFacetPrefix("t_s1");
  }

  public void doFacetPrefix(String f, String... params) {
    String indent="on";
    String pre = "//lst[@name='"+f+"']";
    String notc = "id:[* TO *] -"+f+":C";

    assertU(adoc("id", "1",  f, "AAA"));
    assertU(adoc("id", "2",  f, "B"));
    assertU(adoc("id", "3",  f, "BB"));
    assertU(adoc("id", "4",  f, "BB"));
    assertU(adoc("id", "5",  f, "BBB"));
    assertU(adoc("id", "6",  f, "BBB"));
    assertU(adoc("id", "7",  f, "BBB"));
    assertU(adoc("id", "8",  f, "CC"));
    assertU(adoc("id", "9",  f, "CC"));
    assertU(adoc("id", "10", f, "CCC"));
    assertU(adoc("id", "11", f, "CCC"));
    assertU(adoc("id", "12", f, "CCC"));
    assertU(commit());

    assertQ("test facet.prefix middle, exact match first term",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","true"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='BBB'][.='3']"
            ,pre+"/int[2][@name='BB'][.='2']"
            ,pre+"/int[3][@name='B'][.='1']"
    );

    assertQ("test facet.prefix middle, exact match first term, unsorted",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","false"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='B'][.='1']"
            ,pre+"/int[2][@name='BB'][.='2']"
            ,pre+"/int[3][@name='BBB'][.='3']"
    );


     assertQ("test facet.prefix middle, exact match first term, unsorted",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","false"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='B'][.='1']"
            ,pre+"/int[2][@name='BB'][.='2']"
            ,pre+"/int[3][@name='BBB'][.='3']"
    );


    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","100"
                    ,"facet.sort","true"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='BB'][.='2']"
            ,pre+"/int[2][@name='B'][.='1']"
    );

    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","1"
                    ,"facet.sort","true"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='BB'][.='2']"
    );

    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","1"
                    ,"facet.sort","true"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='BB'][.='2']"
    );

    assertQ("test facet.prefix end, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","true"
                    ,"facet.prefix","C"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='CCC'][.='3']"
            ,pre+"/int[2][@name='CC'][.='2']"
    );

    assertQ("test facet.prefix end, exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","true"
                    ,"facet.prefix","CC"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='CCC'][.='3']"
            ,pre+"/int[2][@name='CC'][.='2']"
    );

    assertQ("test facet.prefix past end",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","true"
                    ,"facet.prefix","X"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix past end",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","-1"
                    ,"facet.sort","true"
                    ,"facet.prefix","X"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix at start, exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","true"
                    ,"facet.prefix","AAA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA'][.='1']"
    );
    assertQ("test facet.prefix at Start, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","true"
                    ,"facet.prefix","AA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA'][.='1']"
    );
    assertQ("test facet.prefix at Start, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","true"
                    ,"facet.prefix","AA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA'][.='1']"
    );    
    assertQ("test facet.prefix before start",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","true"
                    ,"facet.prefix","999"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix before start",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", f
                    ,"facet.mincount","0"
                    ,"facet.offset","2"
                    ,"facet.limit","100"
                    ,"facet.sort","true"
                    ,"facet.prefix","999"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

  }


  private String mkstr(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append((char)(65 + i%26));
    }
    return new String(sb);
  }   
  public void testCompressableFieldType() {
    
    IndexSchema ischema = new IndexSchema(solrConfig, getSchemaFile());
    SchemaField f; // Solr field type
    Field luf; // Lucene field

    f = ischema.getField("test_hlt");
    luf = f.createField("test", 0f);
    assertFalse(luf.isCompressed());
    assertTrue(luf.isStored());

    f = ischema.getField("test_hlt");
    luf = f.createField(mkstr(345), 0f);
    assertTrue(luf.isCompressed());
    assertTrue(luf.isStored());

    f = ischema.getField("test_hlt_off");
    luf = f.createField(mkstr(400), 0f);
    assertFalse(luf.isCompressed());
    assertTrue(luf.isStored());
    
  }

  public void testNotLazyField() throws IOException {
    for(int i = 0; i < 10; i++) {
      assertU(adoc("id", new Integer(i).toString(), 
                   "title", "keyword",
                   "test_hlt", mkstr(20000)));
    }
    assertU(commit());
    SolrCore core = h.getCore();
   
    SolrQueryRequest req = req("q", "title:keyword", "fl", "id,title,test_hlt");
    SolrQueryResponse rsp = new SolrQueryResponse();
    core.execute(req, rsp);

    DocList dl = (DocList) rsp.getValues().get("response");
    org.apache.lucene.document.Document d = req.getSearcher().doc(dl.iterator().nextDoc());
    // ensure field is not lazy
    assertTrue( d.getFieldable("test_hlt") instanceof Field );
    assertTrue( d.getFieldable("title") instanceof Field );
  }

  public void testLazyField() throws IOException {
    for(int i = 0; i < 10; i++) {
      assertU(adoc("id", new Integer(i).toString(), 
                   "title", "keyword",
                   "test_hlt", mkstr(20000)));
    }
    assertU(commit());
    SolrCore core = h.getCore();
    
    SolrQueryRequest req = req("q", "title:keyword", "fl", "id,title");
    SolrQueryResponse rsp = new SolrQueryResponse();
    core.execute(req, rsp);

    DocList dl = (DocList) rsp.getValues().get("response");
    DocIterator di = dl.iterator();    
    org.apache.lucene.document.Document d = req.getSearcher().doc(di.nextDoc());
    // ensure field is lazy
    assertTrue( !( d.getFieldable("test_hlt") instanceof Field ) );
    assertTrue( d.getFieldable("title") instanceof Field );
  } 
            

  /** @see org.apache.solr.util.DateMathParserTest */
  public void testDateMath() {

    // testing everything from query level is hard because
    // time marches on ... and there is no easy way to reach into the
    // bowels of DateField and muck with the definition of "now"
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
    
  }
  
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
