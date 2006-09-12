/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.solr.search.*;
import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.apache.solr.schema.*;
import org.w3c.dom.Document;


import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.io.IOException;
import java.io.StringWriter;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.HashMap;

/**
 * Tests some basic functionality of Solr while demonstrating good
 * Best Practices for using AbstractSolrTestCase
 */
public class BasicFunctionalityTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }

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

  public void testSomeStuff() throws Exception {

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


  public void testMultipleUpdatesPerAdd() {

    // big freaking kludge since the response is currently not well formed.
    String res = h.update("<add><doc><field name=\"id\">1</field></doc><doc><field name=\"id\">2</field></doc></add>");
    assertEquals("<result status=\"0\"></result><result status=\"0\"></result>", res);
    assertU("<commit/>");
    assertQ(req("id:[0 TO 99]")
            ,"//*[@numFound='2']"
            );

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

  /** @see TestRemoveDuplicatesTokenFilter */
  public void testRemoveDuplicatesTokenFilter() {
    Query q = QueryParsing.parseQuery("TV", "dedup",
                                      h.getCore().getSchema());
    assertTrue("not boolean?", q instanceof BooleanQuery);
    assertEquals("unexpected number of stemmed synonym tokens",
                 2, ((BooleanQuery) q).getClauses().length);
  }

  
  public void testTermVectorFields() {
    
    IndexSchema ischema = new IndexSchema(getSchemaFile());
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
 
  }
 
  public void testSimpleFacetCountsWithLimits() {
    assertU(adoc("id", "1",  "t_s", "A"));
    assertU(adoc("id", "2",  "t_s", "B"));
    assertU(adoc("id", "3",  "t_s", "C"));
    assertU(adoc("id", "4",  "t_s", "C"));
    assertU(adoc("id", "5",  "t_s", "D"));
    assertU(adoc("id", "6",  "t_s", "E"));
    assertU(adoc("id", "7",  "t_s", "E"));
    assertU(adoc("id", "8",  "t_s", "E"));
    assertU(adoc("id", "9",  "t_s", "F"));
    assertU(adoc("id", "10", "t_s", "G"));
    assertU(adoc("id", "11", "t_s", "G"));
    assertU(adoc("id", "12", "t_s", "G"));
    assertU(adoc("id", "13", "t_s", "G"));
    assertU(adoc("id", "14", "t_s", "G"));
    assertU(commit());
 
    assertQ("check counts for unlimited facet",
            req("q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.field", "t_s"
                )
            ,"*[count(//lst[@name='facet_fields']/lst[@name='t_s']/int)=7]"
 
            ,"//lst[@name='t_s']/int[@name='G'][.='5']"
            ,"//lst[@name='t_s']/int[@name='E'][.='3']"
            ,"//lst[@name='t_s']/int[@name='C'][.='2']"
 
            ,"//lst[@name='t_s']/int[@name='A'][.='1']"
            ,"//lst[@name='t_s']/int[@name='B'][.='1']"
            ,"//lst[@name='t_s']/int[@name='D'][.='1']"
            ,"//lst[@name='t_s']/int[@name='F'][.='1']"
            );
 
    assertQ("check counts for facet with generous limit",
            req("q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.limit", "100"
                ,"facet.field", "t_s"
                )
            ,"*[count(//lst[@name='facet_fields']/lst[@name='t_s']/int)=7]"
 
            ,"//lst[@name='t_s']/int[1][@name='G'][.='5']"
            ,"//lst[@name='t_s']/int[2][@name='E'][.='3']"
            ,"//lst[@name='t_s']/int[3][@name='C'][.='2']"
 
            ,"//lst[@name='t_s']/int[@name='A'][.='1']"
            ,"//lst[@name='t_s']/int[@name='B'][.='1']"
            ,"//lst[@name='t_s']/int[@name='D'][.='1']"
            ,"//lst[@name='t_s']/int[@name='F'][.='1']"
            );
 
    assertQ("check counts for limited facet",
            req("q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.limit", "2"
                ,"facet.field", "t_s"
                )
            ,"*[count(//lst[@name='facet_fields']/lst[@name='t_s']/int)=2]"
 
            ,"//lst[@name='t_s']/int[1][@name='G'][.='5']"
            ,"//lst[@name='t_s']/int[2][@name='E'][.='3']"
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
    
    IndexSchema ischema = new IndexSchema(getSchemaFile());
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
