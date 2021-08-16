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
package org.apache.solr.search.function;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests some basic functionality of Solr while demonstrating good
 * Best Practices for using SolrTestCaseJ4
 */
public class TestFunctionQuery extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-functionquery.xml","schema11.xml");
  }


  String base = "external_foo_extf";

  static long start = System.nanoTime();

  void makeExternalFile(String field, String contents) {
    String dir = h.getCore().getDataDir();
    String filename = dir + "/external_" + field + "." + (start++);

    try (Writer out = new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8)) {
      out.write(contents);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  void createIndex(String field, int... values) {
    // lrf.args.put("version","2.0");
    for (int val : values) {
      String s = Integer.toString(val);

      if (field!=null) assertU(adoc("id", s, field, s));
      else assertU(adoc("id", s));

      if (random().nextInt(100) < 20) {
        if (field!=null) assertU(adoc("id", s, field, s));
        else assertU(adoc("id", s));
      }

      if (random().nextInt(100) < 20) {
        assertU(commit());

      }


      // System.out.println("added doc for " + val);
    }
    // assertU(optimize()); // squeeze out any possible deleted docs
    assertU(commit());
  }

  // replace \0 with the field name and create a parsable string
  public String func(String field, String template) {
    int size = 7 + field.length() + template.length();
    StringBuilder sb = new StringBuilder(size);
    sb.append("{!func}");
    for (int i = 0; i < template.length(); i++) {
      char ch = template.charAt(i);
      if (ch != '\0') {
        sb.append(ch);
      } else {
        sb.append(field);
      }
    }
    return sb.toString();
  }

  protected void singleTest(String field, String funcTemplate, List<String> args, float... results) {
    // NOTE: we're abusing the "results" float[] here ...
    // - even elements are ids which must be valid 'ints'
    // - odd elements are the expected score values

    String parseableQuery = func(field, funcTemplate);

    List<String> nargs = new ArrayList<>(Arrays.asList("q", parseableQuery
            ,"fl", "*,score"
            ,"indent","on"
            ,"rows","100"));

    if (args != null) {
      for (String arg : args) {
        nargs.add(arg.replace("\0",field));
      }
    }

    List<String> tests = new ArrayList<>();

    for (int i=0; i<results.length; i+=2) {
      final int id = (int) results[i];
      assert ((float) id) == results[i];

      String xpath = "//doc[./str[@name='id']='" + id + "' "
        + " and ./float[@name='score']='" + results[i+1] + "']";
      tests.add(xpath);
    }

    assertQ(req(nargs.toArray(new String[]{}))
            , tests.toArray(new String[]{})
    );
  }

  void singleTest(String field, String funcTemplate, float... results) {
    singleTest(field, funcTemplate, null, results);
  }

  void doTest(String field) {
    clearIndex();

    // lrf.args.put("version","2.0");
    int[] vals = { 100,-4,0,10,25,5 };
    createIndex(field,vals);
    createIndex(null, 88);  // id with no value

    // test identity (straight field value)
    singleTest(field, "\0", 10,10);

    // test constant score
    singleTest(field,"1.414213", 10, 1.414213f);
    singleTest(field,"-1.414213", 10, 0f);

    singleTest(field,"sum(\0,1)", 10, 11);
    singleTest(field,"sum(\0,\0)", 10, 20);
    singleTest(field,"sum(\0,\0,5)", 10, 25);

    singleTest(field,"sub(\0,1)", 10, 9);

    singleTest(field,"product(\0,1)", 10, 10);
    singleTest(field,"product(\0,-2,-4)", 10, 80);

    singleTest(field,"log(\0)",10,1, 100,2);
    singleTest(field,"sqrt(\0)",100,10, 25,5, 0,0);
    singleTest(field,"abs(\0)",10,10, -4,4);
    singleTest(field,"pow(\0,\0)",0,1, 5,3125);
    singleTest(field,"pow(\0,0.5)",100,10, 25,5, 0,0);
    singleTest(field,"div(1,\0)",-4,0f, 10,.1f, 100,.01f);
    singleTest(field,"div(1,1)",-4,1, 10,1);

    singleTest(field,"sqrt(abs(\0))",-4,2);
    singleTest(field,"sqrt(sum(29,\0))",-4,5);

    singleTest(field,"map(\0,0,0,500)",10,10, -4,0, 0,500);
    singleTest(field,"map(\0,-4,5,500)",100,100, -4,500, 0,500, 5,500, 10,10, 25,25);
    singleTest(field,"map(\0,0,0,sum(\0,500))",10,10, -4,0, 0,500);
    singleTest(field,"map(\0,0,0,sum(\0,500),sum(\0,1))",10,11, -4,0, 0,500);
    singleTest(field,"map(\0,-4,5,sum(\0,1))",100,100, -4,0, 0,1, 5,6, 10,10, 25,25);

    singleTest(field,"scale(\0,-1,1)",-4,0, 100,1, 0,0);
    singleTest(field,"scale(\0,-10,1000)",-4,0, 100,1000, 0,28.846153f);

    // test that infinity doesn't mess up scale function
    singleTest(field,"scale(log(\0),-1000,1000)",100,1000);

    // test use of an ValueSourceParser plugin: nvl function
    singleTest(field,"nvl(\0,1)", 0, 1, 100, 100);

    // compose the ValueSourceParser plugin function with another function
    singleTest(field, "nvl(sum(0,\0),1)", 0, 1, 100, 100);

    // test simple embedded query
    singleTest(field,"query({!func v=\0})", 10, 10, 88, 0);
    // test default value for embedded query
    singleTest(field,"query({!lucene v='\0:[* TO *]'},8)", 88, 8);
    singleTest(field,"sum(query({!func v=\0},7.1),query({!func v=\0}))", 10, 20, 100, 200);
    // test with sub-queries specified by other request args
    singleTest(field,"query({!func v=$vv})", Arrays.asList("vv","\0"), 10, 10, 88, 0);
    singleTest(field,"query($vv)",Arrays.asList("vv","{!func}\0"), 10, 10, 88, 0);
    singleTest(field,"sum(query($v1,5),query($v1,7))",
            Arrays.asList("v1","\0:[* TO *]"),  88,12
            );
  }

  @Test
  public void testFunctions() {
    doTest("foo_f");  // a sortable float field
    doTest("foo_tf");  // a trie float field
  }

  @Test
  public void testExternalField() throws Exception {
    clearIndex();

    String field = "foo_extf";

    int[] ids = {100,-4,0,10,25,5,77,23,55,-78,-45,-24,63,78,94,22,34,54321,261,-627};

    createIndex(null,ids);

    // Unsorted field, largest first
    makeExternalFile(field, "54321=543210\n0=-999\n25=250");
    // test identity (straight field value)
    singleTest(field, "\0", 54321, 543210, 0,0, 25,250, 100, 1);
    Object orig = FileFloatSource.onlyForTesting;
    singleTest(field, "log(\0)");
    // make sure the values were cached
    assertTrue(orig == FileFloatSource.onlyForTesting);
    singleTest(field, "sqrt(\0)");
    assertTrue(orig == FileFloatSource.onlyForTesting);

    makeExternalFile(field, "0=1");
    assertU(h.query("/reloadCache",lrf.makeRequest("","")));
    singleTest(field, "sqrt(\0)");
    assertTrue(orig != FileFloatSource.onlyForTesting);


    Random r = random();
    for (int i=0; i<10; i++) {   // do more iterations for a thorough test
      int len = r.nextInt(ids.length+1);
      boolean sorted = r.nextBoolean();
      // shuffle ids
      for (int j=0; j<ids.length; j++) {
        int other=r.nextInt(ids.length);
        int v=ids[0];
        ids[0] = ids[other];
        ids[other] = v;
      }

      if (sorted) {
        // sort only the first elements
        Arrays.sort(ids,0,len);
      }

      // make random values
      float[] vals = new float[len];
      for (int j=0; j<len; j++) {
        vals[j] = r.nextInt(200)-100;
      }

      // make and write the external file
      StringBuilder sb = new StringBuilder();
      for (int j=0; j<len; j++) {
        sb.append("" + ids[j] + "=" + vals[j]+"\n");
      }
      makeExternalFile(field, sb.toString());

      // make it visible
      assertU(h.query("/reloadCache",lrf.makeRequest("","")));

      // test it
      float[] answers = new float[ids.length*2];
      for (int j=0; j<len; j++) {
        answers[j*2] = ids[j];
        answers[j*2+1] = Math.max(0, vals[j]);
      }
      for (int j=len; j<ids.length; j++) {
        answers[j*2] = ids[j];
        answers[j*2+1] = 1;  // the default values
      }

      singleTest(field, "\0", answers);
      // System.out.println("Done test "+i);
    }
  }

  @Test
  public void testExternalFileFieldStringKeys() throws Exception {
    clearIndex();

    final String extField = "foo_extfs";
    final String keyField = "sfile_s";
    assertU(adoc("id", "991", keyField, "AAA=AAA"));
    assertU(adoc("id", "992", keyField, "BBB"));
    assertU(adoc("id", "993", keyField, "CCC=CCC"));
    assertU(commit());
    makeExternalFile(extField, "AAA=AAA=543210\nBBB=-8\nCCC=CCC=250");
    singleTest(extField,"\0",991,543210,992,0,993,250);
  }

  @Test
  public void testExternalFileFieldNumericKey() throws Exception {
    clearIndex();

    final String extField = "eff_trie";
    final String keyField = "eff_tint";
    assertU(adoc("id", "991", keyField, "91"));
    assertU(adoc("id", "992", keyField, "92"));
    assertU(adoc("id", "993", keyField, "93"));
    assertU(commit());
    makeExternalFile(extField, "91=543210\n92=-8\n93=250\n=67");
    singleTest(extField,"\0",991,543210,992,0,993,250);
  }

  @Test
  public void testOrdAndRordOverPointsField() throws Exception {
    assumeTrue("Skipping test when points=false", Boolean.getBoolean(NUMERIC_POINTS_SYSPROP));
    clearIndex();

    String field = "a_" + new String[] {"i","l","d","f"}[random().nextInt(4)];
    assertU(adoc("id", "1", field, "1"));
    assertU(commit());

    Exception e = expectThrows(SolrException.class, () -> h.query(req("q", "{!func}ord(" + field + ")", "fq", "id:1")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ((SolrException)e).code());
    assertTrue(e.getMessage().contains("ord() is not supported over Points based field " + field));

    e = expectThrows(SolrException.class, () -> h.query(req("q", "{!func}rord(" + field + ")", "fq", "id:1")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ((SolrException)e).code());
    assertTrue(e.getMessage().contains("rord() is not supported over Points based field " + field));
  }

  @Test
  public void testGeneral() throws Exception {
    clearIndex();

    assertU(adoc("id","1", "a_tdt","2009-08-31T12:10:10.123Z", "b_tdt","2009-08-31T12:10:10.124Z"));
    assertU(adoc("id","2", "a_t","how now brown cow"));
    assertU(commit()); // create more than one segment
    assertU(adoc("id","3", "a_t","brown cow"));
    assertU(adoc("id","4"));
    assertU(commit()); // create more than one segment
    assertU(adoc("id","5"));
    assertU(adoc("id","6", "a_t","cow cow cow cow cow"));
    assertU(commit());

    // test relevancy functions
    assertQ(req("fl","*,score","q", "{!func}numdocs()", "fq","id:6"), "//float[@name='score']='6.0'");
    assertQ(req("fl","*,score","q", "{!func}maxdoc()", "fq","id:6"), "//float[@name='score']='6.0'");
    assertQ(req("fl","*,score","q", "{!func}docfreq(a_t,cow)", "fq","id:6"), "//float[@name='score']='3.0'");
    assertQ(req("fl","*,score","q", "{!func}docfreq('a_t','cow')", "fq","id:6"), "//float[@name='score']='3.0'");
    assertQ(req("fl","*,score","q", "{!func}docfreq($field,$value)", "fq","id:6", "field","a_t", "value","cow"), "//float[@name='score']='3.0'");
    assertQ(req("fl","*,score","q", "{!func}termfreq(a_t,cow)", "fq","id:6"), "//float[@name='score']='5.0'");

    // make sure it doesn't get a NPE if no terms are present in a field.
    assertQ(req("fl","*,score","q", "{!func}termfreq(nofield_t,cow)", "fq","id:6"), "//float[@name='score']='0.0'");
    assertQ(req("fl","*,score","q", "{!func}docfreq(nofield_t,cow)", "fq","id:6"), "//float[@name='score']='0.0'");

    // test that ord and rord are working on a global index basis, not just
    // at the segment level (since Lucene 2.9 has switched to per-segment searching)
    assertQ(req("fl","*,score","q", "{!func}ord(id)", "fq","id:6"), "//float[@name='score']='5.0'");
    assertQ(req("fl","*,score","q", "{!func}top(ord(id))", "fq","id:6"), "//float[@name='score']='5.0'");
    assertQ(req("fl","*,score","q", "{!func}rord(id)", "fq","id:1"),"//float[@name='score']='5.0'");
    assertQ(req("fl","*,score","q", "{!func}top(rord(id))", "fq","id:1"),"//float[@name='score']='5.0'");


    // test that we can subtract dates to millisecond precision
    assertQ(req("fl","*,score","q", "{!func}ms(a_tdt,b_tdt)", "fq","id:1"), "//float[@name='score']='0.0'");
    assertQ(req("fl","*,score","q", "{!func}ms(b_tdt,a_tdt)", "fq","id:1"), "//float[@name='score']='1.0'");
    assertQ(req("fl","*,score","q", "{!func}ms(2009-08-31T12:10:10.125Z,2009-08-31T12:10:10.124Z)", "fq","id:1"), "//float[@name='score']='1.0'");
    assertQ(req("fl","*,score","q", "{!func}ms(2009-08-31T12:10:10.124Z,a_tdt)", "fq","id:1"), "//float[@name='score']='1.0'");
    assertQ(req("fl","*,score","q", "{!func}ms(2009-08-31T12:10:10.125Z,b_tdt)", "fq","id:1"), "//float[@name='score']='1.0'");

    assertQ(req("fl","*,score","q", "{!func}ms(2009-08-31T12:10:10.125Z/SECOND,2009-08-31T12:10:10.124Z/SECOND)", "fq","id:1"), "//float[@name='score']='0.0'");

    // test that we can specify "NOW"
    assertQ(req("fl","*,score","q", "{!func}ms(NOW)", "NOW","1000"), "//float[@name='score']='1000.0'");


    for (int i=100; i<112; i++) {
      assertU(adoc("id",""+i, "text","batman"));
    }
    assertU(commit());
    assertU(adoc("id","120", "text","batman superman"));   // in a smaller segment
    assertU(adoc("id","121", "text","superman junkterm"));
    assertU(commit());

    // superman has a higher df (thus lower idf) in one segment, but reversed in the complete index
    String q ="{!func}query($qq)";
    String fq="id:120";
    assertQ(req("fl","*,score","q", q, "qq","text:batman", "fq",fq), "//float[@name='score']<'0.6'");
    assertQ(req("fl","*,score","q", q, "qq","text:superman", "fq",fq), "//float[@name='score']>'0.6'");

    // test weighting through a function range query
    assertQ(req("fl","*,score", "fq",fq,  "q", "{!frange l=0.6 u=10}query($qq)", "qq","text:superman"), "//*[@numFound='1']");

    // test weighting through a complex function
    q ="{!func}sub(div(sum(0.0,product(1,query($qq))),1),0)";
    assertQ(req("fl","*,score","q", q, "qq","text:batman", "fq",fq), "//float[@name='score']<'0.6'");
    assertQ(req("fl","*,score","q", q, "qq","text:superman", "fq",fq), "//float[@name='score']>'0.6'");


    // test full param dereferencing
    assertQ(req("fl","*,score","q", "{!func}add($v1,$v2)", "v1","add($v3,$v4)", "v2","1", "v3","2", "v4","5"
        , "fq","id:1"), "//float[@name='score']='8.0'");

    // test ability to parse multiple values
    assertQ(req("fl","*,score","q", "{!func}dist(2,vector(1,1),$pt)", "pt","3,1"
        , "fq","id:1"), "//float[@name='score']='2.0'");

    // test that extra stuff after a function causes an error
    try {
      assertQ(req("fl","*,score","q", "{!func}10 wow dude ignore_exception"));
      fail();
    } catch (Exception e) {
      // OK
    }

    // test that sorting by function query weights correctly.  superman should sort higher than batman due to idf of the whole index

    assertQ(req("q", "*:*", "fq","id:120 OR id:121", "sort","{!func v=$sortfunc} desc", "sortfunc","query($qq)", "qq","text:(batman OR superman)")
           ,"*//doc[1]/str[.='120']"
           ,"*//doc[2]/str[.='121']"
    );

    // test a query that doesn't specify nested query val
    assertQEx("Should fail because of missing qq",
        "Missing param qq while parsing function 'query($qq)'",
        req("q", "*:*", "fq","id:120 OR id:121", "defType","edismax", "boost","query($qq)"),
        SolrException.ErrorCode.BAD_REQUEST
    );
    assertQEx("Should fail because of missing sortfunc in sort",
        "Can't determine a Sort Order (asc or desc) in sort spec '{!func v=$sortfunc} desc'",
        req("q", "*:*", "fq","id:120 OR id:121", "sort","{!func v=$sortfunc} desc", "sortfunc","query($qq)"),
        SolrException.ErrorCode.BAD_REQUEST
    );
    assertQEx("Should fail because of missing qq in boost",
        "Nested local params must have value in v parameter.  got 'query({!dismax v=$qq})",
        req("q", "*:*", "fq","id:120 OR id:121", "defType","edismax", "boost","query({!dismax v=$qq})"),
        SolrException.ErrorCode.BAD_REQUEST
    );
    assertQEx("Should fail as empty value is specified for v",
        "Nested function query returned null for 'query({!v=})'",
        req("q", "*:*", "defType","edismax", "boost","query({!v=})"), SolrException.ErrorCode.BAD_REQUEST
    );
    assertQEx("Should fail as v's value contains only spaces",
        "Nested function query returned null for 'query({!v=   })'",
        req("q", "*:*", "defType","edismax", "boost","query({!v=   })"), SolrException.ErrorCode.BAD_REQUEST
    );

    // no field specified in ord()
    assertQEx("Should fail as no field is specified in ord func",
        "Expected identifier instead of 'null' for function 'ord()'",
        req("q", "*:*", "defType","edismax","boost","ord()"), SolrException.ErrorCode.BAD_REQUEST
    );
    assertQEx("Should fail as no field is specified in rord func",
        "Expected identifier instead of 'null' for function 'rord()'",
        req("q", "*:*", "defType","edismax","boost","rord()"), SolrException.ErrorCode.BAD_REQUEST
    );

    // test parseFloat
    assertQEx("Should fail as less args are specified for recip func",
        "Expected float instead of 'null' for function 'recip(1,2)'",
        req("q", "*:*","defType","edismax", "boost","recip(1,2)"), SolrException.ErrorCode.BAD_REQUEST
    );
    assertQEx("Should fail as invalid value is specified for recip func",
        "Expected float instead of 'f' for function 'recip(1,2,3,f)'",
        req("q", "*:*","defType","edismax", "boost","recip(1,2,3,f)"), SolrException.ErrorCode.BAD_REQUEST
    );
    // this should pass
    assertQ(req("q", "*:*","defType","edismax", "boost","recip(1, 2, 3, 4)"));

    // for undefined field NPE shouldn't be thrown
    assertQEx("Should Fail as the field is undefined", "undefined field a",
        req("q", "*:*", "fl", "x:payload(a,b)"), SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testTFIDFFunctions() {
    clearIndex();

    TFIDFSimilarity similarity = null;
    {
      Similarity sim = h.getCore().getLatestSchema().getFieldType("a_tfidf").getSimilarity();
      assertNotNull("Test needs *_tfidf to use a TFIDFSimilarity ... who broke the config?", sim);
      assertTrue("Test needs *_tfidf to use a TFIDFSimilarity ... who broke the config: " + sim.getClass(),
                 sim instanceof TFIDFSimilarity);
      similarity = (TFIDFSimilarity) sim;
    }

    assertU(adoc("id","1", "a_tdt","2009-08-31T12:10:10.123Z", "b_tdt","2009-08-31T12:10:10.124Z"));
    assertU(adoc("id","2", "a_tfidf","how now brown cow"));
    assertU(commit()); // create more than one segment
    assertU(adoc("id","3", "a_tfidf","brown cow"));
    assertU(adoc("id","4"));
    assertU(commit()); // create more than one segment
    assertU(adoc("id","5"));
    assertU(adoc("id","6", "a_tfidf","cow cow cow cow cow"));
    assertU(commit());

    // make sure it doesn't get a NPE if no terms are present in a field.
    assertQ(req("fl","*,score","q", "{!func}idf(nofield_tfidf,cow)", "fq","id:6"),
            "//float[@name='score']='" + similarity.idf(0,6)  + "'");
    assertQ(req("fl","*,score","q", "{!func}tf(nofield_tfidf,cow)", "fq","id:6"),
            "//float[@name='score']='" + similarity.tf(0)  + "'");

    // fields with real values
    assertQ(req("fl","*,score","q", "{!func}idf(a_tfidf,cow)", "fq","id:6"),
            "//float[@name='score']='" + similarity.idf(3,6)  + "'");
    assertQ(req("fl","*,score","q", "{!func}tf(a_tfidf,cow)", "fq","id:6"),
            "//float[@name='score']='" + similarity.tf(5)  + "'");

    assertQ(req("fl","*,score","q", "{!func}norm(a_tfidf)", "fq","id:2"),
        "//float[@name='score']='0.5'");  // 1/sqrt(4)==1/2==0.5

  }

  /**
   * test collection-level term stats (new in 4.x indexes)
   */
  @Test
  public void testTotalTermFreq() throws Exception {
    clearIndex();

    assertU(adoc("id","1", "a_tdt","2009-08-31T12:10:10.123Z", "b_tdt","2009-08-31T12:10:10.124Z"));
    assertU(adoc("id","2", "a_t","how now brown cow"));
    assertU(commit()); // create more than one segment
    assertU(adoc("id","3", "a_t","brown cow"));
    assertU(adoc("id","4"));
    assertU(commit()); // create more than one segment
    assertU(adoc("id","5"));
    assertU(adoc("id","6", "a_t","cow cow cow cow cow"));
    assertU(commit());
    assertQ(req("fl","*,score","q", "{!func}totaltermfreq('a_t','cow')", "fq","id:6"), "//float[@name='score']='7.0'");
    assertQ(req("fl","*,score","q", "{!func}ttf(a_t,'cow')", "fq","id:6"), "//float[@name='score']='7.0'");
    assertQ(req("fl","*,score","q", "{!func}sumtotaltermfreq('a_t')", "fq","id:6"), "//float[@name='score']='11.0'");
    assertQ(req("fl","*,score","q", "{!func}sttf(a_t)", "fq","id:6"), "//float[@name='score']='11.0'");
  }

  @Test
  public void testPayloadFunction() {
    clearIndex();

    assertU(adoc("id","1", "vals_dpf","A|1.0 B|2.0 C|3.0 mult|50 mult|100 x|22 x|37 x|19", "default_f", "42.0"));
    assertU(commit());

    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,A)"), "//float[@name='score']='1.0'");
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,B)"), "//float[@name='score']='2.0'");
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,C,0)"), "//float[@name='score']='3.0'");

    // Test defaults, constant, field, and function value sources
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,D,37.0)"), "//float[@name='score']='37.0'");
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,E,default_f)"), "//float[@name='score']='42.0'");
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,E,mul(2,default_f))"), "//float[@name='score']='84.0'");

    // Test PayloadFunction's for multiple terms, average being the default
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,mult,0.0,min)"), "//float[@name='score']='50.0'");
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,mult,0.0,max)"), "//float[@name='score']='100.0'");
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,mult,0.0,average)"), "//float[@name='score']='75.0'");
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,mult)"), "//float[@name='score']='75.0'");

    // Test special "first" function, by checking the other functions with same term too
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,x,0.0,min)"), "//float[@name='score']='19.0'");
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,x,0.0,max)"), "//float[@name='score']='37.0'");
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,x,0.0,average)"), "//float[@name='score']='26.0'");
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,x,0.0,first)"), "//float[@name='score']='22.0'");

    // Test with debug
    assertQ(req("fl","*,score","q", "{!func}payload(vals_dpf,A)", CommonParams.DEBUG, "true"), "//float[@name='score']='1.0'");
  }

  @Test
  public void testRetrievePayloads() throws Exception {
    clearIndex();

    int numDocs = 100 + random().nextInt(100);
    int numLocations = 1000 + random().nextInt(2000);
    for (int docNum = 0 ; docNum < numDocs ; ++docNum) {
      StringBuilder amountsBuilder = new StringBuilder();
      for (int location = 1 ; location <= numLocations ; ++location) {
        String amount = "" + location + '.' + random().nextInt(100);
        amountsBuilder.append(location).append('|').append(amount).append(' ');
      }
      assertU(adoc("id","" + docNum,
                   "default_amount_f", "" + (10000 + random().nextInt(10000)) + ".0",
                   "amounts_dpf", amountsBuilder.toString()));
    }
    assertU(commit());
    assertJQ(req("q","*:*",
        "fl","id,location:$locationId,amount:$amount",
        "sort","$amount asc",
        "amount","payload(amounts_dpf,$locationId,default_amount_f)",
        "locationId",""+(1+random().nextInt(numLocations)),
        "wt","json"),
        "/response/numFound==" + numDocs);
  }

  @Test
  public void testSortByFunc() throws Exception {
    clearIndex();

    assertU(adoc("id",    "1",   "const_s", "xx",
                 "x_i",   "100", "1_s", "a",
                 "x:x_i", "100", "1-1_s", "a"));
    assertU(adoc("id",    "2",   "const_s", "xx",
                 "x_i",   "300", "1_s", "c",
                 "x:x_i", "300", "1-1_s", "c"));
    assertU(adoc("id",    "3",   "const_s", "xx",
                 "x_i",   "200", "1_s", "b",
                 "x:x_i", "200", "1-1_s", "b"));
    assertU(commit());

    String desc = "/response/docs==[{'x_i':300},{'x_i':200},{'x_i':100}]";
    String asc =  "/response/docs==[{'x_i':100},{'x_i':200},{'x_i':300}]";

    String threeonetwo =  "/response/docs==[{'x_i':200},{'x_i':100},{'x_i':300}]";

    String q = "id_i:[1 TO 3]";
    assertJQ(req("q",q,  "fl","x_i", "sort","add(x_i,x_i) desc")
      ,desc
    );

    // param sub of entire function
    assertJQ(req("q",q,  "fl","x_i", "sort", "const_s asc, $x asc", "x","add(x_i,x_i)")
      ,asc
    );

    // multiple functions
    assertJQ(req("q",q,  "fl","x_i", "sort", "$x asc, const_s asc, $y desc", "x", "5", "y","add(x_i,x_i)")
      ,desc
    );

    // multiple functions inline
    assertJQ(req("q",q,  "fl","x_i", "sort", "add( 10 , 10 ) asc, const_s asc, add(x_i , $const) desc", "const","50")
      ,desc
    );

    // test function w/ local params + func inline
    assertJQ(req("q",q,  "fl","x_i",
                 "sort", "const_s asc, {!key=foo}add(x_i,x_i) desc")
             ,desc
    );
    assertJQ(req("q",q,  "fl","x_i",
                 "sort", "{!key=foo}add(x_i,x_i) desc, const_s asc")
             ,desc
    );

    // test multiple functions w/ local params + func inline
    assertJQ(req("q",q,  "fl","x_i", "sort", "{!key=bar}add(10,20) asc, const_s asc, {!key=foo}add(x_i,x_i) desc")
      ,desc
    );

    // test multiple functions w/ local param value not inlined
    assertJQ(req("q",q,  "fl","x_i", "sort", "{!key=bar v=$s1} asc, {!key=foo v=$s2} desc", "s1","add(3,4)", "s2","add(x_i,5)")
      ,desc
    );

    // no space between inlined localparams and sort order
    assertJQ(req("q",q,  "fl","x_i", "sort", "{!key=bar v=$s1}asc,const_s asc,{!key=foo v=$s2}desc", "s1","add(3,4)", "s2","add(x_i,5)")
      ,desc
    );

    // field name that isn't a legal java Identifier
    // and starts with a number to trick function parser
    assertJQ(req("q",q,  "fl","x_i", "sort", "1_s asc")
             ,asc
    );
    assertJQ(req("q",q,  "fl","x_i", "sort", "x:x_i desc")
             ,desc
    );
    assertJQ(req("q",q,  "fl","x_i", "sort", "1-1_s asc")
             ,asc
    );

    // really ugly field name that isn't a java Id, and can't be
    // parsed as a func, but sorted fine in Solr 1.4
    assertJQ(req("q",q,  "fl","x_i",
                 "sort", "[]_s asc, {!key=foo}add(x_i,x_i) desc")
             ,desc
    );
    // use localparms to sort by a lucene query, then a function
    assertJQ(req("q",q,  "fl","x_i",
                 "sort", "{!lucene v='id:3'}desc, {!key=foo}add(x_i,x_i) asc")
             ,threeonetwo
    );


  }

  @Test
  public void testDegreeRads() throws Exception {
    clearIndex();

    assertU(adoc("id", "1", "x_td", "0", "y_td", "0"));
    assertU(adoc("id", "2", "x_td", "90", "y_td", String.valueOf(Math.PI / 2)));
    assertU(adoc("id", "3", "x_td", "45", "y_td", String.valueOf(Math.PI / 4)));


    assertU(commit());
    assertQ(req("fl", "*,score", "q", "{!func}rad(x_td)", "fq", "id:1"), "//float[@name='score']='0.0'");
    assertQ(req("fl", "*,score", "q", "{!func}rad(x_td)", "fq", "id:2"), "//float[@name='score']='" + (float) (Math.PI / 2) + "'");
    assertQ(req("fl", "*,score", "q", "{!func}rad(x_td)", "fq", "id:3"), "//float[@name='score']='" + (float) (Math.PI / 4) + "'");

    assertQ(req("fl", "*,score", "q", "{!func}deg(y_td)", "fq", "id:1"), "//float[@name='score']='0.0'");
    assertQ(req("fl", "*,score", "q", "{!func}deg(y_td)", "fq", "id:2"), "//float[@name='score']='90.0'");
    assertQ(req("fl", "*,score", "q", "{!func}deg(y_td)", "fq", "id:3"), "//float[@name='score']='45.0'");
  }

  @Test
  public void testStrDistance() throws Exception {
    clearIndex();

    assertU(adoc("id", "1", "x_s", "foil"));
    assertU(commit());
    assertQ(req("fl", "*,score", "q", "{!func}strdist(x_s, 'foit', edit)", "fq", "id:1"), "//float[@name='score']='0.75'");
    assertQ(req("fl", "*,score", "q", "{!func}strdist(x_s, 'foit', jw)", "fq", "id:1"), "//float[@name='score']='0.8833333'");
    assertQ(req("fl", "*,score", "q", "{!func}strdist(x_s, 'foit', ngram, 2)", "fq", "id:1"), "//float[@name='score']='0.875'");

    // strdist on a missing valuesource should itself by missing, so the ValueSourceAugmenter
    // should supress it...
    assertQ(req("q", "id:1",
                "fl", "good:strdist(x_s, 'toil', edit)",
                "fl", "bad1:strdist(missing1_s, missing2_s, edit)",
                "fl", "bad2:strdist(missing1_s, 'something', edit)",
                "fl", "bad3:strdist(missing1_s, x_s, edit)")
            , "//float[@name='good']='0.75'"
            , "count(//float[starts-with(@name,'bad')])=0"
            );

    // in a query context, there is always a number...
    //
    // if a ValueSource is missing, it is maximally distant from every other
    // value source *except* for another missing value source
    // ie: strdist(null,null)==1 but strdist(null,anything)==0
    assertQ(req("fl","score","fq", "id:1", "q", "{!func}strdist(missing1_s, missing2_s, edit)"),
            "//float[@name='score']='1.0'");
    assertQ(req("fl","score","fq", "id:1", "q", "{!func}strdist(missing1_s, x_s, edit)"),
            "//float[@name='score']='0.0'");
    assertQ(req("fl","score","fq", "id:1", "q", "{!func}strdist(missing1_s, 'const', edit)"),
            "//float[@name='score']='0.0'");
  }

  public void dofunc(String func, double val) throws Exception {
    // String sval = Double.toString(val);
    String sval = Float.toString((float)val);

    assertQ(req("fl", "*,score", "defType","func", "fq","id:1", "q",func),
            "//float[@name='score']='" + sval + "'");
  }

  @Test
  public void testFuncs() throws Exception {
    clearIndex();

    assertU(adoc("id", "1", "foo_d", "9"));
    assertU(commit());

    dofunc("1.0", 1.0);
    dofunc("e()", Math.E);
    dofunc("pi()", Math.PI);
    dofunc("add(2,3)", 2+3);
    dofunc("mul(2,3)", 2*3);
    dofunc("rad(45)", Math.toRadians(45));
    dofunc("deg(.5)", Math.toDegrees(.5));
    dofunc("sqrt(9)", Math.sqrt(9));
    dofunc("cbrt(8)", Math.cbrt(8));
    dofunc("max(0,1)", Math.max(0,1));
    dofunc("max(10,3,8,7,5,4)", Math.max(Math.max(Math.max(Math.max(Math.max(10,3),8),7),5),4));
    dofunc("min(0,1)", Math.min(0,1));
    dofunc("min(10,3,8,7,5,4)", Math.min(Math.min(Math.min(Math.min(Math.min(10,3),8),7),5),4));
    dofunc("log(100)", Math.log10(100));
    dofunc("ln(3)", Math.log(3));
    dofunc("exp(1)", Math.exp(1));
    dofunc("sin(.5)", Math.sin(.5));
    dofunc("cos(.5)", Math.cos(.5));
    dofunc("tan(.5)", Math.tan(.5));
    dofunc("asin(.5)", Math.asin(.5));
    dofunc("acos(.5)", Math.acos(.5));
    dofunc("atan(.5)", Math.atan(.5));
    dofunc("sinh(.5)", Math.sinh(.5));
    dofunc("cosh(.5)", Math.cosh(.5));
    dofunc("tanh(.5)", Math.tanh(.5));
    dofunc("ceil(2.3)", Math.ceil(2.3));
    dofunc("floor(2.3)", Math.floor(2.3));
    dofunc("rint(2.3)", Math.rint(2.3));
    dofunc("pow(2,0.5)", Math.pow(2,0.5));
    dofunc("hypot(3,4)", Math.hypot(3,4));
    dofunc("atan2(.25,.5)", Math.atan2(.25,.5));
  }

  /**
   * verify that both the field("...") value source parser as well as
   * ExternalFileField work with esoteric field names
   */
  @Test
  public void testExternalFieldValueSourceParser() {
    clearIndex();

    String field = "CoMpleX fieldName _extf";
    String fieldAsFunc = "field(\"CoMpleX fieldName _extf\")";

    int[] ids = {100,-4,0,10,25,5,77,23,55,-78,-45,-24,63,78,94,22,34,54321,261,-627};

    createIndex(null,ids);

    // Unsorted field, largest first
    makeExternalFile(field, "54321=543210\n0=-999\n25=250");
    // test identity (straight field value)
    singleTest(fieldAsFunc, "\0", 54321, 543210, 0,0, 25,250, 100, 1);
    Object orig = FileFloatSource.onlyForTesting;
    singleTest(fieldAsFunc, "log(\0)");
    // make sure the values were cached
    assertTrue(orig == FileFloatSource.onlyForTesting);
    singleTest(fieldAsFunc, "sqrt(\0)");
    assertTrue(orig == FileFloatSource.onlyForTesting);

    makeExternalFile(field, "0=1");
    assertU(adoc("id", "10000")); // will get same reader if no index change
    assertU(commit());
    singleTest(fieldAsFunc, "sqrt(\0)");
    assertTrue(orig != FileFloatSource.onlyForTesting);
  }

  /**
   * some platforms don't allow quote characters in filenames, so
   * in addition to testExternalFieldValueSourceParser above, test a field
   * name with quotes in it that does NOT use ExternalFileField
   * @see #testExternalFieldValueSourceParser
   */
  @Test
  public void testFieldValueSourceParser() {
    clearIndex();

    String field = "CoMpleX \" fieldName _f";
    String fieldAsFunc = "field(\"CoMpleX \\\" fieldName _f\")";

    int[] ids = {100,-4,0,10,25,5,77,1};

    createIndex(field, ids);

    // test identity (straight field value)
    singleTest(fieldAsFunc, "\0",
               100,100,  -4,0,  0,0,  10,10,  25,25,  5,5,  77,77,  1,1);
    singleTest(fieldAsFunc, "sqrt(\0)",
               100,10,  25,5,  0,0,   1,1);
    singleTest(fieldAsFunc, "log(\0)",  1,0);
  }

  @Test
  public void testBooleanFunctions() throws Exception {
    clearIndex();

    assertU(adoc("id", "1", "text", "hello", "foo_s","A", "foo_ti", "0", "foo_tl","0", "foo_tf", "0.00001"));
    assertU(adoc("id", "2"                              , "foo_ti","10", "foo_tl","11"));
    assertU(commit());

    // test weighting of functions
    assertJQ(req("q", "id:1", "fl", "a:testfunc(1)")
          , "/response/docs/[0]=={'a':1}");

    // true and false functions and constants
    assertJQ(req("q", "id:1", "fl", "t:true(),f:false(),tt:{!func}true,ff:{!func}false")
        , "/response/docs/[0]=={'t':true,'f':false,'tt':true,'ff':false}");

    // test that exists(query) depends on the query matching the document
    assertJQ(req("q", "id:1", "fl", "t:exists(query($q1)),f:exists(query($q2))", "q1","text:hello", "q2","text:there")
        , "/response/docs/[0]=={'t':true,'f':false}");

    // test if()
    assertJQ(req("q", "id:1", "fl", "a1:if(true,'A','B')", "fl","b1:if(false,'A',testfunc('B'))")
        , "/response/docs/[0]=={'a1':'A', 'b1':'B'}");
    // queries with positive scores < 1 should still evaluate to 'true' in boolean context
    assertJQ(req("q", "id:1", "nested", "*:*^=0.00001",
                 "fl", "a1:if(query($nested),'A','B')", "fl","b1:if(not(query($nested)),'A','B')")
        , "/response/docs/[0]=={'a1':'A', 'b1':'B'}");

    // test boolean operators
    assertJQ(req("q", "id:1", "fl", "t1:and(testfunc(true),true)", "fl","f1:and(true,false)", "fl","f2:and(false,true)", "fl","f3:and(false,false)")
        , "/response/docs/[0]=={'t1':true, 'f1':false, 'f2':false, 'f3':false}");
    assertJQ(req("q", "id:1", "fl", "t1:or(testfunc(true),true)", "fl","t2:or(true,false)", "fl","t3:or(false,true)", "fl","f1:or(false,false)")
        , "/response/docs/[0]=={'t1':true, 't2':true, 't3':true, 'f1':false}");
    assertJQ(req("q", "id:1", "fl", "f1:xor(testfunc(true),true)", "fl","t1:xor(true,false)", "fl","t2:xor(false,true)", "fl","f2:xor(false,false)")
        , "/response/docs/[0]=={'t1':true, 't2':true, 'f1':false, 'f2':false}");
    assertJQ(req("q", "id:1", "fl", "t:not(testfunc(false)),f:not(true)")
        , "/response/docs/[0]=={'t':true, 'f':false}");

    // test fields evaluated as booleans in wrapping functions
    assertJQ(req("q", "id:1", "fl", "a:not(foo_ti), b:if(foo_tf,'TT','FF'), c:and(true,foo_tf)")
        , "/response/docs/[0]=={'a':true, 'b':'TT', 'c':true}");
    assertJQ(req("q", "id:2", "fl", "a:not(foo_ti), b:if(foo_tf,'TT','FF'), c:and(true,foo_tf)")
        , "/response/docs/[0]=={'a':false, 'b':'FF', 'c':false}");


    // def(), the default function that returns the first value that exists
    assertJQ(req("q", "id:1", "fl", "x:def(id,testfunc(123)), y:def(foo_f,234.0)")
        , "/response/docs/[0]=={'x':'1', 'y':234.0}");
    assertJQ(req("q", "id:1", "fl", "x:def(foo_s,'Q'), y:def(missing_s,'W')")
        , "/response/docs/[0]=={'x':'A', 'y':'W'}");

    // test constant conversion to boolean
    assertJQ(req("q", "id:1", "fl", "a:not(0), b:not(1), c:not(0.0), d:not(1.1), e:not('A'), f:not(0.001)")
        , "/response/docs/[0]=={'a':true, 'b':false, 'c':true, 'd':false, 'e':false, 'f':false}");

  }

  @Test
  public void testConcatFunction() {
    clearIndex();

    assertU(adoc("id", "1", "field1_t", "buzz", "field2_t", "word"));
    assertU(adoc("id", "2", "field1_t", "1", "field2_t", "2","field4_t", "4"));
    assertU(commit());

    assertQ(req("q","id:1",
        "fl","field:concat(field1_t,field2_t)"),
        " //str[@name='field']='buzzword'");

    assertQ(req("q","id:2",
        "fl","field:concat(field1_t,field2_t,field4_t)"),
        " //str[@name='field']='124'");

    assertQ(req("q","id:1",
        "fl","field:def(concat(field3_t, field4_t), 'defValue')"),
        " //str[@name='field']='defValue'");
   }

  @Test
  public void testPseudoFieldFunctions() throws Exception {
    clearIndex();

    assertU(adoc("id", "1", "text", "hello", "foo_s","A", "yak_i", "32"));
    assertU(adoc("id", "2"));
    assertU(commit());

    // if exists() is false, no pseudo-field should be added
    assertJQ(req("q", "id:1", "fl", "a:1,b:2.0,c:'X',d:{!func}foo_s,e:{!func}bar_s")
             , "/response/docs/[0]=={'a':1, 'b':2.0,'c':'X','d':'A'}");
    assertJQ(req("q", "id:1", "fl", "a:sum(yak_i,bog_i),b:mul(yak_i,bog_i),c:min(yak_i,bog_i)")
             , "/response/docs/[0]=={ 'c':32.0 }");
    assertJQ(req("q", "id:1", "fl", "a:sum(yak_i,def(bog_i,42)), b:max(yak_i,bog_i)")
             , "/response/docs/[0]=={ 'a': 74.0, 'b':32.0 }");
  }

  @Test
  public void testMissingFieldFunctionBehavior() throws Exception {
    clearIndex();
    // add a doc that has no values in any interesting fields
    assertU(adoc("id", "1"));
    assertU(commit());

    // it's important that these functions not only use fields that
    // our doc has no values for, but also that no other doc ever added
    // to the index might have ever had a value for, so that the segment
    // term metadata doesn't exist

    for (String suffix : new String[] {"s", "b", "dt", "tdt",
                                       "i", "l", "f", "d",
                                       "ti", "tl", "tf", "td"    }) {
      final String field = "no__vals____" + suffix;
      assertQ(req("q","id:1",
                  "fl","noval_if:if("+field+",42,-99)",
                  "fl","noval_def:def("+field+",-99)",
                  "fl","noval_not:not("+field+")",
                  "fl","noval_exists:exists("+field+")"),
              "//long[@name='noval_if']='-99'",
              "//long[@name='noval_def']='-99'",
              "//bool[@name='noval_not']='true'",
              "//bool[@name='noval_exists']='false'");
    }
  }

  @Test
  public void testNumericComparisons() throws Exception {
    clearIndex();

    assertU(adoc("id", "1", "age_i", "35"));
    assertU(adoc("id", "2", "age_i", "25"));
    assertU(commit());

    // test weighting of functions
    assertJQ(req("q", "id:1", "fl", "a:gt(age_i,30),b:lt(age_i,30)")
        , "/response/docs/[0]=={'a':true,'b':false}");

    assertJQ(req("q", "id:1", "fl", "a:exists(gt(foo_i,30))")
        , "/response/docs/[0]=={'a':false}");

    singleTest("age_i", "if(gt(age_i,30),5,2)",
               /*id*/1, /*score*/5,
               /*id*/2, /*score*/2);

    singleTest("age_i", "if(lt(age_i,30),5,2)",
               /*id*/1, /*score*/2,
               /*id*/2, /*score*/5);

    singleTest("age_i", "if(lt(age_i,34.5),5,2)",
               /*id*/1, /*score*/2,
               /*id*/2, /*score*/5);

    singleTest("age_i", "if(lte(age_i,35),5,2)",
               /*id*/1, /*score*/5,
               /*id*/2, /*score*/5);

    singleTest("age_i", "if(gte(age_i,25),5,2)",
               /*id*/1, /*score*/5,
               /*id*/2, /*score*/5);

    singleTest("age_i", "if(lte(age_i,25),5,2)",
               /*id*/1, /*score*/2,
               /*id*/2, /*score*/5);

    singleTest("age_i", "if(gte(age_i,35),5,2)",
               /*id*/1, /*score*/5,
               /*id*/2, /*score*/2);


    singleTest("age_i", "if(eq(age_i,30),5,2)",
               /*id*/1, /*score*/2,
               /*id*/2, /*score*/2);

    singleTest("age_i", "if(eq(age_i,35),5,2)",
               /*id*/1, /*score*/5,
               /*id*/2, /*score*/2);
  }

  @Test
  public void testLongComparisons() {
    clearIndex();

    assertU(adoc("id", "1", "number_of_atoms_in_universe_l", Long.toString(Long.MAX_VALUE)));
    assertU(adoc("id", "2", "number_of_atoms_in_universe_l", Long.toString(Long.MAX_VALUE - 1)));
    assertU(commit());

    singleTest("number_of_atoms_in_universe_l", "if(gt(number_of_atoms_in_universe_l," + Long.toString(Long.MAX_VALUE - 1) + "),5,2)",
               /*id*/1, /*score*/5,
               /*id*/2, /*score*/2);

    singleTest("number_of_atoms_in_universe_l", "if(lt(number_of_atoms_in_universe_l," + Long.toString(Long.MAX_VALUE) + "),5,2)",
               /*id*/2, /*score*/5,
               /*id*/1, /*score*/2);
  }

  @Test
  public void testQueryAsFlParam() {
    clearIndex();

    assertU(adoc("id", "1", "age_i", "35"));
    assertU(adoc("id", "2", "age_i", "25"));
    assertU(commit());

    // some docs match the query func but some doesn't
    // if doc doesn't match, it should use default value
    assertQ(req("q", "*:*", "fl", "*,score,bleh:query($qq,5.0)", "qq", "id:2"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[1]/float[@name='bleh'][.='5.0']",
        "count(//result/doc[2]/float[@name='bleh'][.='5.0'])=0",
        "//result/doc[2]/float[@name='bleh']" // since score can't be known, doing existing match
        );

    // when the doc match the query func condition default value shouldn't be used
    // when no def val is passed in query func, 0.0 would be used
    assertQ(req("q", "*:*", "fl", "*,score,bleh:query($qq)", "qq", "id:*"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "count(//result/doc[1]/float[@name='bleh'][.='0.0'])=0",
        "count(//result/doc[2]/float[@name='bleh'][.='0.0'])=0",
        "//result/doc[1]/float[@name='bleh']",
        "//result/doc[2]/float[@name='bleh']"
    );
  }

  @Test
  public void testEqualFunction() {
    clearIndex();
    assertU(adoc("id", "1", "field1_s", "value1", "field2_s", "value1",
        "field1_s_dv", "value1", "field2_s_dv", "value2", "field_b", "true"));
    assertU(adoc("id", "2", "field1_s", "value1", "field2_s", "value2",
        "field1_s_dv", "value1", "field2_s_dv", "value1", "field_b", "false"));
    assertU(commit());

    singleTest("field1_s", "if(eq(field1_s,field2_s),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/2);
    singleTest("field1_s_dv", "if(eq(field1_s_dv,field2_s_dv),5,2)",
        /*id*/2, /*score*/5,
        /*id*/1, /*score*/2);
    singleTest("field1_s", "if(eq(field1_s,field1_s_dv),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/5);
    singleTest("field2_s", "if(eq(field2_s,field2_s_dv),5,2)",
        /*id*/1, /*score*/2,
        /*id*/2, /*score*/2);
    singleTest("field2_s", "if(eq(field2_s,'value1'),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/2);
    singleTest("field1_s", "if(eq('value1','value1'),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/5);
    singleTest("field_b", "if(eq(if(field_b,'value1','value2'),'value1'),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/2);
  }

  @Test
  public void testEqualNumericComparisons() {
    clearIndex();
    assertU(adoc("id", "1", "field_d", "5.0", "field_i", "5"));
    assertU(adoc("id", "2",  "field_d", "3.0", "field_i", "3"));
    assertU(commit());
    singleTest("field_d", "if(eq(field_d,5),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/2);
    singleTest("field_d", "if(eq(field_d,5.0),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/2);
    singleTest("field_d", "if(eq(5,def(field_d,5)),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/2);
    singleTest("field_i", "if(eq(5.0,def(field_i,5)),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/2);
    singleTest("field_not_existed_i", "if(def(field_not_existed_i,5.0),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/5);
    singleTest("field_not_existed_i", "if(def(field_not_existed_i,5),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/5);
  }

  @Test
  public void testDifferentTypesComparisons() {
    clearIndex();
    assertU(adoc("id", "1", "field_s", "value"));
    assertU(adoc("id", "2"));
    assertU(commit());
    singleTest("field_s", "if(eq(field_s,'value'),5,2)",
        /*id*/1, /*score*/5,
        /*id*/2, /*score*/2);
    singleTest("field_s", "if(eq(def(field_s,5),5),5,2)",
        /*id*/2, /*score*/5,
        /*id*/1, /*score*/2);
    singleTest("field_s", "if(eq(def(field_s,5),5.0),5,2)",
        /*id*/2, /*score*/5,
        /*id*/1, /*score*/2);
    singleTest("field_s", "if(eq(def(field_s,'5'),5),5,2)",
        /*id*/1, /*score*/2,
        /*id*/2, /*score*/2);
  }

  /**
   * Tests a specific (edge) case where a subQuery is null, because no terms are
   * found in the query. Below such subQuery is created from a field query on a
   * query text containing only stopwords. Feeding the resulting null-subQuery
   * into a QueryValueSource (and then using it in for example an if function)
   * may not produce NullPointerExceptions.
   */
  @Test
  public void testNullSubQuery() throws Exception {
    clearIndex();
    assertJQ(req("q", "{!func}if($subQuery,1,0)", "subQuery", "{!field f=text v='stopworda'}"));
  }
}
