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
package org.apache.solr.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.util.SolrPluginUtils.DisjunctionMaxQueryParser;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that the functions in SolrPluginUtils work as advertised.
 */
public class SolrPluginUtilsTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  @Test
  public void testPartialEscape() {

    assertEquals("",pe(""));
    assertEquals("foo",pe("foo"));
    assertEquals("foo\\:bar",pe("foo:bar"));
    assertEquals("+foo\\:bar",pe("+foo:bar"));
    assertEquals("foo \\! bar",pe("foo ! bar"));
    assertEquals("foo\\?",pe("foo?"));
    assertEquals("foo \"bar\"",pe("foo \"bar\""));
    assertEquals("foo\\! \"bar\"",pe("foo! \"bar\""));
        
  }

  @Test
  public void testStripUnbalancedQuotes() {
        
    assertEquals("",strip(""));
    assertEquals("foo",strip("foo"));
    assertEquals("foo \"bar\"",strip("foo \"bar\""));
    assertEquals("42",strip("42\""));
    assertEquals("\"how now brown cow?\"",strip("\"how now brown cow?\""));
    assertEquals("\"you go\" \"now!\"",strip("\"you go\" \"now!\""));
        
  }

  @Test
  public void testStripIllegalOperators() {

    assertEquals("",stripOp(""));
    assertEquals("foo",stripOp("foo"));
    assertEquals("foo -bar",stripOp("foo -bar"));
    assertEquals("foo +bar",stripOp("foo +bar"));
    assertEquals("foo + bar",stripOp("foo + bar"));
    assertEquals("foo+ bar",stripOp("foo+ bar"));
    assertEquals("foo+ bar",stripOp("foo+ bar"));
    assertEquals("foo+",stripOp("foo+"));
    assertEquals("foo bar",stripOp("foo bar -"));
    assertEquals("foo bar ",stripOp("foo bar - + ++"));
    assertEquals("foo bar",stripOp("foo --bar"));
    assertEquals("foo bar ",stripOp("foo -------------------------------------------------------------------------------------------------------------------------bar --"));
    assertEquals("foo bar ",stripOp("foo --bar -----------------------------------------------------------------------------------------------------------------------"));

  }

  @Test
  public void testParseFieldBoosts() throws Exception {

    Map<String,Float> e1 = new HashMap<>();
    e1.put("fieldOne",2.3f);
    e1.put("fieldTwo",null);
    e1.put("fieldThree",-0.4f);

    assertEquals("basic e1", e1, SolrPluginUtils.parseFieldBoosts
                 ("fieldOne^2.3 fieldTwo fieldThree^-0.4"));
    assertEquals("spacey e1", e1, SolrPluginUtils.parseFieldBoosts
                 ("  fieldOne^2.3   fieldTwo fieldThree^-0.4   "));
    assertEquals("really spacey e1", e1, SolrPluginUtils.parseFieldBoosts
                 (" \t fieldOne^2.3 \n  fieldTwo fieldThree^-0.4   "));
    assertEquals("really spacey e1", e1, SolrPluginUtils.parseFieldBoosts
                 (new String[]{" \t fieldOne^2.3 \n",
                               "  fieldTwo fieldThree^-0.4   ",
                               " "}));

    Map<String,Float> e2 = new HashMap<>();
    assertEquals("empty e2", e2, SolrPluginUtils.parseFieldBoosts
                 (""));
    assertEquals("spacey e2", e2, SolrPluginUtils.parseFieldBoosts
                 ("   \t   "));
  }

  @Test  
  public void testDisjunctionMaxQueryParser() throws Exception {
        
    Query out;
    String t;

    SolrQueryRequest req = req("df", "text");
    QParser qparser = QParser.getParser("hi", "dismax", req);

    DisjunctionMaxQueryParser qp =
      new SolrPluginUtils.DisjunctionMaxQueryParser(qparser, req.getParams().get("df"));

    qp.addAlias("hoss", 0.01f, SolrPluginUtils.parseFieldBoosts
                ("title^2.0 title_stemmed name^1.2 subject^0.5"));
    qp.addAlias("test", 0.01f, SolrPluginUtils.parseFieldBoosts("text^2.0"));
    qp.addAlias("unused", 1.0f, SolrPluginUtils.parseFieldBoosts
                ("subject^0.5 sind^1.5"));
                     

    /* first some sanity tests that don't use aliasing at all */

    t = "XXXXXXXX";
    out = qp.parse(t);
    assertNotNull(t+" sanity test gave back null", out);
    assertTrue(t+" sanity test isn't TermQuery: " + out.getClass(),
               out instanceof TermQuery);
    assertEquals(t+" sanity test is wrong field",
                 qp.getDefaultField(),
                 ((TermQuery)out).getTerm().field());

    t = "subject:XXXXXXXX";
    out = qp.parse(t);
    assertNotNull(t+" sanity test gave back null", out);
    assertTrue(t+" sanity test isn't TermQuery: " + out.getClass(),
               out instanceof TermQuery);
    assertEquals(t+" sanity test is wrong field", "subject",
                 ((TermQuery)out).getTerm().field());

    /* field has untokenzied type, so this should be a term anyway */
    t = "sind:\"simple phrase\"";
    out = qp.parse(t);
    assertNotNull(t+" sanity test gave back null", out);
    assertTrue(t+" sanity test isn't TermQuery: " + out.getClass(),
               out instanceof TermQuery);
    assertEquals(t+" sanity test is wrong field", "sind",
                 ((TermQuery)out).getTerm().field());

    t = "subject:\"simple phrase\"";
    out = qp.parse(t);
    assertNotNull(t+" sanity test gave back null", out);
    assertTrue(t+" sanity test isn't PhraseQuery: " + out.getClass(),
               out instanceof PhraseQuery);
    assertEquals(t+" sanity test is wrong field", "subject",
                 ((PhraseQuery)out).getTerms()[0].field());

        
    /* now some tests that use aliasing */

    /* basic usage of single "term" */
    t = "hoss:XXXXXXXX";
    out = qp.parse(t);
    assertNotNull(t+" was null", out);
    assertTrue(t+" wasn't a DMQ:" + out.getClass(),
               out instanceof DisjunctionMaxQuery);
    assertEquals(t+" wrong number of clauses", 4,
                 countItems(((DisjunctionMaxQuery)out).iterator()));
        

    /* odd case, but should still work, DMQ of one clause */
    t = "test:YYYYY";
    out = qp.parse(t);
    assertNotNull(t+" was null", out);
    assertTrue(t+" wasn't a DMQ:" + out.getClass(),
               out instanceof DisjunctionMaxQuery);
    assertEquals(t+" wrong number of clauses", 1,
                 countItems(((DisjunctionMaxQuery)out).iterator()));
        
    /* basic usage of multiple "terms" */
    t = "hoss:XXXXXXXX test:YYYYY";
    out = qp.parse(t);
    assertNotNull(t+" was null", out);
    assertTrue(t+" wasn't a boolean:" + out.getClass(),
               out instanceof BooleanQuery);
    {
      BooleanQuery bq = (BooleanQuery)out;
      List<BooleanClause> clauses = new ArrayList<>(bq.clauses());
      assertEquals(t+" wrong number of clauses", 2,
                   clauses.size());
      Query sub = clauses.get(0).getQuery();
      assertTrue(t+" first wasn't a DMQ:" + sub.getClass(),
                 sub instanceof DisjunctionMaxQuery);
      assertEquals(t+" first had wrong number of clauses", 4,
                   countItems(((DisjunctionMaxQuery)sub).iterator()));
      sub = clauses.get(1).getQuery();
      assertTrue(t+" second wasn't a DMQ:" + sub.getClass(),
                 sub instanceof DisjunctionMaxQuery);
      assertEquals(t+" second had wrong number of clauses", 1,
                   countItems(((DisjunctionMaxQuery)sub).iterator()));
    }
            
    /* a phrase, and a term that is a stop word for some fields */
    t = "hoss:\"XXXXXX YYYYY\" hoss:the";
    out = qp.parse(t);
    assertNotNull(t+" was null", out);
    assertTrue(t+" wasn't a boolean:" + out.getClass(),
               out instanceof BooleanQuery);
    {
      BooleanQuery bq = (BooleanQuery)out;
      List<BooleanClause> clauses = new ArrayList<>(bq.clauses());
      assertEquals(t+" wrong number of clauses", 2,
                   clauses.size());
      Query sub = clauses.get(0).getQuery();
      assertTrue(t+" first wasn't a DMQ:" + sub.getClass(),
                 sub instanceof DisjunctionMaxQuery);
      assertEquals(t+" first had wrong number of clauses", 4,
                   countItems(((DisjunctionMaxQuery)sub).iterator()));
      sub = clauses.get(1).getQuery();
      assertTrue(t+" second wasn't a DMQ:" + sub.getClass(),
                 sub instanceof DisjunctionMaxQuery);
      assertEquals(t+" second had wrong number of clauses (stop words)", 2,
                   countItems(((DisjunctionMaxQuery)sub).iterator()));
    }
        

        
  }

  private static int countItems(Iterator i) {
    int count = 0;
    while (i.hasNext()) {
      count++;
      i.next();
    }
    return count;
  }

  @Test                                    
  public void testMinShouldMatchCalculator() {

    /* zero is zero is zero */
    assertEquals(0, calcMSM(5, "0"));
    assertEquals(0, calcMSM(5, "0%"));
    assertEquals(0, calcMSM(5, " -5 "));
    assertEquals(0, calcMSM(5, "\n -100% \n"));

    /* basic integers */
    assertEquals(3, calcMSM(5, " \n3\n "));
    assertEquals(2, calcMSM(5, "-3"));
    assertEquals(3, calcMSM(3, "3"));
    assertEquals(0, calcMSM(3, "-3"));
    assertEquals(3, calcMSM(3, "5"));
    assertEquals(0, calcMSM(3, "-5"));

    /* positive percentages with rounding */
    assertEquals(0, calcMSM(3, " \n25% \n"));
    assertEquals(1, calcMSM(4, "25%"));
    assertEquals(1, calcMSM(5, " 25% "));
    assertEquals(2, calcMSM(10, "25%"));
        
    /* negative percentages with rounding */
    assertEquals(3, calcMSM(3, " \n-25%\n "));
    assertEquals(3, calcMSM(4, "-25%"));
    assertEquals(4, calcMSM(5, "-25%"));
    assertEquals(8, calcMSM(10, "-25%"));

    /* conditional */
    assertEquals(1, calcMSM(1, "3<0"));
    assertEquals(2, calcMSM(2, "3<0"));
    assertEquals(3, calcMSM(3, "3<0"));
    assertEquals(0, calcMSM(4, "3<0"));
    assertEquals(0, calcMSM(5, "3<0"));
    assertEquals(1, calcMSM(1, "3<25%"));
    assertEquals(2, calcMSM(2, " 3\n<\n25% "));
    assertEquals(3, calcMSM(3, "3<25%"));
    assertEquals(1, calcMSM(4, "\n 3 < \n25%\n "));
    assertEquals(1, calcMSM(5, "3<25%"));

    /* multiple conditionals */
    assertEquals(1, calcMSM(1, "\n3 < -25% 10 < -3 \n"));
    assertEquals(2, calcMSM(2, " 3 < -25% 10 < -3\n"));
    assertEquals(3, calcMSM(3, " 3 < -25% \n 10 < -3 \n"));
    assertEquals(3, calcMSM(4, " 3 < -25% 10 < -3 "));
    assertEquals(4, calcMSM(5, " 3 < -25% 10 < -3"));
    assertEquals(5, calcMSM(6, "3<-25% 10<-3"));
    assertEquals(6, calcMSM(7, " 3 < -25% 10 < -3 "));
    assertEquals(6, calcMSM(8, " 3 < -25% 10 \n < -3\n"));
    assertEquals(7, calcMSM(9, " 3 < -25% 10 < -3 \n"));
    assertEquals(8, calcMSM(10, " 3 < -25% 10 < -3"));
    assertEquals(8, calcMSM(11, "3<-25% 10<-3"));
    assertEquals(9, calcMSM(12, "3<-25% 10<-3"));
    assertEquals(97, calcMSM(100, "3<-25% 10<-3"));

    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(new TermQuery(new Term("a","b")), Occur.SHOULD);
    q.add(new TermQuery(new Term("a","c")), Occur.SHOULD);
    q.add(new TermQuery(new Term("a","d")), Occur.SHOULD);
    q.add(new TermQuery(new Term("a","d")), Occur.SHOULD);

    SolrPluginUtils.setMinShouldMatch(q, "0");
    assertEquals(0, q.build().getMinimumNumberShouldMatch());
        
    SolrPluginUtils.setMinShouldMatch(q, "1");
    assertEquals(1, q.build().getMinimumNumberShouldMatch());
        
    SolrPluginUtils.setMinShouldMatch(q, "50%");
    assertEquals(2, q.build().getMinimumNumberShouldMatch());

    SolrPluginUtils.setMinShouldMatch(q, "99");
    assertEquals(4, q.build().getMinimumNumberShouldMatch());

    q.add(new TermQuery(new Term("a","e")), Occur.MUST);
    q.add(new TermQuery(new Term("a","f")), Occur.MUST);

    SolrPluginUtils.setMinShouldMatch(q, "50%");
    assertEquals(2, q.build().getMinimumNumberShouldMatch());
        
  }

  @Test
  public void testMinShouldMatchBadQueries() {
    Exception e = expectThrows(SolrException.class, () -> calcMSM(2, "1<"));
    assertEquals("Invalid 'mm' spec: '1<'. Expecting values before and after '<'" , e.getMessage());
    e = expectThrows(SolrException.class, () -> calcMSM(2, "1<x"));
    assertEquals("Invalid 'mm' spec. Expecting an integer.", e.getMessage());
    e = expectThrows(SolrException.class, () -> calcMSM(1, "x%"));
    assertEquals("Invalid 'mm' spec. Expecting an integer.", e.getMessage());
    e = expectThrows(SolrException.class, () -> calcMSM(1, "%%"));
    assertEquals("Invalid 'mm' spec. Expecting an integer.", e.getMessage());
    e = expectThrows(SolrException.class, () -> calcMSM(1, "x"));
    assertEquals("Invalid 'mm' spec. Expecting an integer.", e.getMessage());
    
    e = expectThrows(SolrException.class, () -> calcMSM(10, "2<-25% 9<X"));
    assertEquals("Invalid 'mm' spec. Expecting an integer." , e.getMessage());
    e = expectThrows(SolrException.class, () -> calcMSM(10, "2<-25% 9<"));
    assertEquals("Invalid 'mm' spec: '9<'. Expecting values before and after '<'" , e.getMessage());
  }

  @Test
  public void testMinShouldMatchAutoRelax() {
    /* The basics should not be affected by autoRelax */
    BooleanQuery.Builder q = new BooleanQuery.Builder();
    q.add(new TermQuery(new Term("a","b")), Occur.SHOULD);
    q.add(new TermQuery(new Term("a","c")), Occur.SHOULD);
    q.add(new TermQuery(new Term("a","d")), Occur.SHOULD);
    q.add(new TermQuery(new Term("a","d")), Occur.SHOULD);

    SolrPluginUtils.setMinShouldMatch(q, "0", true);
    assertEquals(0, q.build().getMinimumNumberShouldMatch());

    SolrPluginUtils.setMinShouldMatch(q, "1", true);
    assertEquals(1, q.build().getMinimumNumberShouldMatch());

    SolrPluginUtils.setMinShouldMatch(q, "50%", true);
    assertEquals(2, q.build().getMinimumNumberShouldMatch());

    SolrPluginUtils.setMinShouldMatch(q, "99", true);
    assertEquals(4, q.build().getMinimumNumberShouldMatch());

    q.add(new TermQuery(new Term("a","e")), Occur.MUST);
    q.add(new TermQuery(new Term("a","f")), Occur.MUST);

    SolrPluginUtils.setMinShouldMatch(q, "50%", true);
    assertEquals(2, q.build().getMinimumNumberShouldMatch());

    /* Simulate stopwords through uneven disjuncts */
    q = new BooleanQuery.Builder();
    q.add(new DisjunctionMaxQuery(Collections.singleton(new TermQuery(new Term("a","foo"))), 0.0f), Occur.SHOULD);
    DisjunctionMaxQuery dmq = new DisjunctionMaxQuery(
        Arrays.asList(
            new TermQuery(new Term("a","foo")),
            new TermQuery(new Term("b","foo"))),
        0f);
    q.add(dmq, Occur.SHOULD);
    dmq = new DisjunctionMaxQuery(
        Arrays.asList(
            new TermQuery(new Term("a","bar")),
            new TermQuery(new Term("b","bar"))),
        0f);
    q.add(dmq, Occur.SHOULD);

    // Without relax
    SolrPluginUtils.setMinShouldMatch(q, "100%", false);
    assertEquals(3, q.build().getMinimumNumberShouldMatch());

    // With relax
    SolrPluginUtils.setMinShouldMatch(q, "100%", true);
    assertEquals(2, q.build().getMinimumNumberShouldMatch());

    // Still same result with a MUST clause extra
    q.add(new TermQuery(new Term("a","must")), Occur.MUST);
    SolrPluginUtils.setMinShouldMatch(q, "100%", true);
    assertEquals(2, q.build().getMinimumNumberShouldMatch());

    // Combination of dismax and non-dismax SHOULD clauses
    q.add(new TermQuery(new Term("b","should")), Occur.SHOULD);
    SolrPluginUtils.setMinShouldMatch(q, "100%", true);
    assertEquals(3, q.build().getMinimumNumberShouldMatch());
  }

  private static class InvokeSettersTestClass {
    private float aFloat = random().nextFloat();
    public float getAFloat() {
      return aFloat;
    }
    public void setAFloat(float aFloat) {
      this.aFloat = aFloat;
    }
    public void setAFloat(String aFloat) {
      this.aFloat = Float.parseFloat(aFloat);
    }
  }

  @Test
  public void testInvokeSetters() {
    final Float theFloat = random().nextFloat();
    implTestInvokeSetters(theFloat, theFloat);
    implTestInvokeSetters(theFloat, theFloat.toString());
  }

  public void implTestInvokeSetters(final Float theFloat, final Object theFloatObject) {
    final InvokeSettersTestClass bean = new InvokeSettersTestClass();
    final Map<String,Object> initArgs = new HashMap<>();
    initArgs.put("aFloat", theFloatObject);
    SolrPluginUtils.invokeSetters(bean, initArgs.entrySet());
    assertEquals(bean.getAFloat(), theFloat.floatValue(), 0.0);
  }

  /** macro */
  public String pe(CharSequence s) {
    return SolrPluginUtils.partialEscape(s).toString();
  }
    
  /** macro */
  public String strip(CharSequence s) {
    return SolrPluginUtils.stripUnbalancedQuotes(s).toString();
  }
   
  /** macro */
  public String stripOp(CharSequence s) {
    return SolrPluginUtils.stripIllegalOperators(s).toString();
  }
   
  /** macro */
  public int calcMSM(int clauses, String spec) {
    return SolrPluginUtils.calculateMinShouldMatch(clauses, spec);
  }
}

