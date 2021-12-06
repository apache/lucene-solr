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
package org.apache.solr.search;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.query.FilterQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;

public class TestSolrQueryParser extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    System.setProperty("solr.max.booleanClauses", "42"); // lower for testing
    System.setProperty("solr.filterCache.async", "true"); // for testLocalParamsInQP
    initCore("solrconfig.xml", "schema12.xml");
    createIndex();
  }

  private static final List<String> HAS_VAL_FIELDS = new ArrayList<String>(41);
  private static final List<String> HAS_NAN_FIELDS = new ArrayList<String>(12);

  @AfterClass
  public static void afterClass() throws Exception {
    HAS_VAL_FIELDS.clear();
    HAS_NAN_FIELDS.clear();
  }
  
  public static void createIndex() {
    String v;
    v = "how now brown cow";
    assertU(adoc("id", "1", "text", v, "text_np", v, "foo_i","11"));
    v = "now cow";
    assertU(adoc("id", "2", "text", v, "text_np", v, "foo_i","12"));
    assertU(adoc("id", "3", "foo_s", "a ' \" \\ {! ) } ( { z"));  // A value filled with special chars

    assertU(adoc("id", "10", "qqq_s", "X"));
    assertU(adoc("id", "11", "www_s", "X"));
    assertU(adoc("id", "12", "eee_s", "X"));
    assertU(adoc("id", "13", "eee_s", "'balance'", "rrr_s", "/leading_slash"));

    assertU(adoc("id", "20", "syn", "wifi ATM"));

    { // make a doc that has a value in *lots* of fields that no other doc has
      SolrInputDocument doc = sdoc("id", "999");
      
      // numbers...
      for (String t : Arrays.asList("i", "l", "f", "d")) { 
        for (String s : Arrays.asList("", "s", "_dv", "s_dv", "_dvo", "_norms")) {
          final String f = "has_val_" + t + s;
          HAS_VAL_FIELDS.add(f);
          doc.addField(f, "42");

          if (t.equals("f") || t.equals("d")) {
            String nanField = "nan_val_" + t + s;
            doc.addField(nanField, "NaN");
            HAS_NAN_FIELDS.add(nanField);

            // Add a NaN & non-NaN value for multivalue fields, these should match :* and :[* TO *] equivalently
            if (s.startsWith("s")) {
              String bothField = "both_val_" + t + s;
              doc.addField(bothField, "42");
              doc.addField(bothField, "NaN");
              HAS_VAL_FIELDS.add(bothField);
            }
          }
        }
      }
      // boolean...booleans
      for (String s : Arrays.asList("", "s", "_dv", "_norms")) {
        final String f = "has_val_b" + s;
        HAS_VAL_FIELDS.add(f);
        doc.addField(f, "false");
      }

      // dates (and strings/text -- they don't care about the format)...
      for (String s : Arrays.asList("dt", "s", "s1", "t", "t_on", "dt_norms", "s_norms", "dt_dv", "s_dv")) {
        final String f = "has_val_" + s;
        HAS_VAL_FIELDS.add(f);
        doc.addField(f, "2019-01-12T00:00:00Z");
      }
      assertU(adoc(doc));
    }
            
    assertU(adoc("id", "30", "shingle23", "A B X D E"));

    assertU(commit());
  }

  public void testDocsWithValuesInField() throws Exception {
    assertEquals("someone changed the test setup of HAS_VAL_FIELDS, w/o updating the sanity check",
                 41, HAS_VAL_FIELDS.size());
    for (String f : HAS_VAL_FIELDS) {
      // for all of these fields, these 2 syntaxes should be functionally equivilent
      // in matching the one doc that contains these fields
      for (String q : Arrays.asList( f + ":*", f + ":[* TO *]" )) {
        assertJQ(req("q", q)
                 , "/response/numFound==1"
                 , "/response/docs/[0]/id=='999'"
                 );
        // the same syntaxes should be valid even if no doc has the field...
        assertJQ(req("q", "bogus___" + q)
                 , "/response/numFound==0"
                 );

      }
    }
  }

  public void testDocsWithNaNInField() throws Exception {
    assertEquals("someone changed the test setup of HAS_NAN_FIELDS, w/o updating the sanity check",
        12, HAS_NAN_FIELDS.size());
    for (String f : HAS_NAN_FIELDS) {
      // for all of these fields, field:* should NOT be equivalent to field:[* TO *]
      assertJQ(req("q", f + ":*")
          , "/response/numFound==1"
          , "/response/docs/[0]/id=='999'"
      );
      assertJQ(req("q", f + ":[* TO *]")
          , "/response/numFound==0"
      );
      assertJQ(req("q", f + ":[-Infinity TO Infinity]")
          , "/response/numFound==0"
      );
      for (String q : Arrays.asList( f + ":*", f + ":[* TO *]", f + ":[-Infinity TO Infinity]" )) {
        // the same syntaxes should be valid even if no doc has the field...
        assertJQ(req("q", "bogus___" + q)
            , "/response/numFound==0"
        );

      }
    }
  }
  
  @Test
  public void testPhrase() {
    // "text" field's type has WordDelimiterGraphFilter (WDGFF) and autoGeneratePhraseQueries=true
    // should generate a phrase of "now cow" and match only one doc
    assertQ(req("q", "text:now-cow", "indent", "true", "sow","true")
        , "//*[@numFound='1']"
    );
    // When sow=false, autoGeneratePhraseQueries=true only works when a graph is produced
    // (i.e. overlapping terms, e.g. if WDGFF's preserveOriginal=1 or concatenateWords=1).
    // The WDGFF config on the "text" field doesn't produce a graph, so the generated query
    // is not a phrase query.  As a result, docs can match that don't match phrase query "now cow"
    assertQ(req("q", "text:now-cow", "indent", "true", "sow","false")
        , "//*[@numFound='2']"
    );
    assertQ(req("q", "text:now-cow", "indent", "true") // default sow=false
        , "//*[@numFound='2']"
    );
    
    // "text_np" field's type has WDGFF and (default) autoGeneratePhraseQueries=false
    // should generate a query of (now OR cow) and match both docs
    assertQ(req("q", "text_np:now-cow", "indent", "true")
        , "//*[@numFound='2']"
    );
  }

  @Test
  public void testLocalParamsInQP() throws Exception {
    assertJQ(req("q", "qaz {!term f=text v=$qq} wsx", "qq", "now")
        , "/response/numFound==2"
    );

    assertJQ(req("q", "qaz {!term f=text v=$qq} wsx", "qq", "nomatch")
        , "/response/numFound==0"
    );

    assertJQ(req("q", "qaz {!term f=text}now wsx", "qq", "now")
        , "/response/numFound==2"
    );

    assertJQ(req("q", "qaz {!term f=foo_s v='a \\' \" \\\\ {! ) } ( { z'} wsx")           // single quote escaping
        , "/response/numFound==1"
    );

    assertJQ(req("q", "qaz {!term f=foo_s v=\"a ' \\\" \\\\ {! ) } ( { z\"} wsx")         // double quote escaping
        , "/response/numFound==1"
    );

    // double-join to test back-to-back local params
    assertJQ(req("q", "qaz {!join from=www_s to=eee_s}{!join from=qqq_s to=www_s}id:10")
        , "/response/docs/[0]/id=='12'"
    );
  }

  @Test
  public void testSolr4121() throws Exception {
    // At one point, balanced quotes messed up the parser(SOLR-4121)
    assertJQ(req("q", "eee_s:'balance'", "indent", "true")
        , "/response/numFound==1"
    );
  }

  @Test
  public void testSyntax() throws Exception {
    // a bare * should be treated as *:*
    assertJQ(req("q", "*", "df", "doesnotexist_s")
        , "/response/docs/[0]=="   // make sure we get something...
    );
    assertJQ(req("q", "doesnotexist_s:*")
        , "/response/numFound==0"   // nothing should be found
    );
    assertJQ(req("q", "doesnotexist_s:( * * * )")
        , "/response/numFound==0"   // nothing should be found
    );

    // length of date math caused issues...
    {
      SchemaField foo_dt = h.getCore().getLatestSchema().getField("foo_dt");
      String expected = "foo_dt:2013-09-11T00:00:00Z";
      if (foo_dt.getType().isPointField()) {
        expected = "(foo_dt:[1378857600000 TO 1378857600000])";
        if (foo_dt.hasDocValues() && foo_dt.indexed()) {
          expected = "IndexOrDocValuesQuery"+expected ;
        }
      }
      assertJQ(req("q", "foo_dt:\"2013-03-08T00:46:15Z/DAY+000MILLISECONDS+00SECONDS+00MINUTES+00HOURS+0000000000YEARS+6MONTHS+3DAYS\"", "debug", "query")
               , "/debug/parsedquery=='"+expected+"'");
    }
  }

  @Test
  public void testNestedQueryModifiers() throws Exception {
    // One previous error was that for nested queries, outer parameters overrode nested parameters.
    // For example _query_:"\"a b\"~2" was parsed as "a b"

    String subqq = "_query_:\"{!v=$qq}\"";

    assertJQ(req("q", "_query_:\"\\\"how brown\\\"~2\""
        , "debug", "query"
        )
        , "/response/docs/[0]/id=='1'"
    );

    assertJQ(req("q", subqq, "qq", "\"how brown\"~2"
        , "debug", "query"
        )
        , "/response/docs/[0]/id=='1'"
    );

    // Should explicit slop override?  It currently does not, but that could be considered a bug.
    assertJQ(req("q", subqq + "~1", "qq", "\"how brown\"~2"
        , "debug", "query"
        )
        , "/response/docs/[0]/id=='1'"
    );

    // Should explicit slop override?  It currently does not, but that could be considered a bug.
    assertJQ(req("q", "  {!v=$qq}~1", "qq", "\"how brown\"~2"
        , "debug", "query"
        )
        , "/response/docs/[0]/id=='1'"
    );

    assertJQ(req("fq", "id:1", "fl", "id,score", "q", subqq + "^3", "qq", "text:x^2"
        , "debug", "query"
        )
        , "/debug/parsedquery_toString=='((text:x)^2.0)^3.0'"
    );

    assertJQ(req("fq", "id:1", "fl", "id,score", "q", "  {!v=$qq}^3", "qq", "text:x^2"
        , "debug", "query"
        )
        , "/debug/parsedquery_toString=='((text:x)^2.0)^3.0'"
    );

  }


  @Test
  public void testCSQ() throws Exception {
    SolrQueryRequest req = req();

    QParser qParser = QParser.getParser("text:x^=3", req);
    Query q = qParser.getQuery();
    assertTrue(q instanceof BoostQuery);
    assertTrue(((BoostQuery) q).getQuery() instanceof ConstantScoreQuery);
    assertEquals(3.0, ((BoostQuery) q).getBoost(), 0.0f);

    req.close();
  }


  // automatically use TermsQuery when appropriate
  @Test
  public void testAutoTerms() throws Exception {
    SolrQueryRequest req = req();
    QParser qParser;
    Query q,qq;

    Map<String, String> sowFalseParamsMap = new HashMap<>();
    sowFalseParamsMap.put("sow", "false");
    Map<String, String> sowTrueParamsMap = new HashMap<>();
    sowTrueParamsMap.put("sow", "true");
    List<MapSolrParams> paramMaps = Arrays.asList
        (new MapSolrParams(Collections.emptyMap()), // no sow param (i.e. the default sow value) 
         new MapSolrParams(sowFalseParamsMap),
         new MapSolrParams(sowTrueParamsMap));

    for (MapSolrParams params : paramMaps) {
      // relevance query should not be a filter
      qParser = QParser.getParser("foo_s:(a b c)", req);
      qParser.setParams(params);
      q = qParser.getQuery();
      assertEquals(3, ((BooleanQuery) q).clauses().size());

      // small filter query should still use BooleanQuery
      if (QueryParser.TERMS_QUERY_THRESHOLD > 3) {
        qParser = QParser.getParser("foo_s:(a b c)", req);
        qParser.setParams(params);
        qParser.setIsFilter(true); // this may change in the future
        q = qParser.getQuery();
        assertEquals(3, ((BooleanQuery) q).clauses().size());
      }

      // large relevancy query should use BooleanQuery
      // TODO: we may decide that string fields shouldn't have relevance in the future... change to a text field w/o a stop filter if so
      qParser = QParser.getParser("foo_s:(a b c d e f g h i j k l m n o p q r s t u v w x y z)", req);
      qParser.setParams(params);
      q = qParser.getQuery();
      assertEquals(26, ((BooleanQuery)q).clauses().size());

      // large filter query should use TermsQuery
      qParser = QParser.getParser("foo_s:(a b c d e f g h i j k l m n o p q r s t u v w x y z)", req);
      qParser.setIsFilter(true); // this may change in the future
      qParser.setParams(params);
      q = qParser.getQuery();
      assertEquals(26, ((TermInSetQuery)q).getTermData().size());

      // large numeric filter query should use TermsQuery
      qParser = QParser.getParser("foo_ti:(1 2 3 4 5 6 7 8 9 10 20 19 18 17 16 15 14 13 12 11)", req);
      qParser.setIsFilter(true); // this may change in the future
      qParser.setParams(params);
      q = qParser.getQuery();
      if (Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)) {
        assertEquals(20, ((PointInSetQuery)q).getPackedPoints().size());
      } else {
        assertEquals(20, ((TermInSetQuery)q).getTermData().size());
      }

      // for point fields large filter query should use PointInSetQuery
      qParser = QParser.getParser("foo_pi:(1 2 3 4 5 6 7 8 9 10 20 19 18 17 16 15 14 13 12 11)", req);
      qParser.setIsFilter(true); // this may change in the future
      qParser.setParams(params);
      q = qParser.getQuery();
      assertTrue(q instanceof PointInSetQuery);
      assertEquals(20, ((PointInSetQuery)q).getPackedPoints().size());

      // a filter() clause inside a relevancy query should be able to use a TermsQuery
      qParser = QParser.getParser("foo_s:aaa filter(foo_s:(a b c d e f g h i j k l m n o p q r s t u v w x y z))", req);
      qParser.setParams(params);
      q = qParser.getQuery();
      assertEquals(2, ((BooleanQuery)q).clauses().size());
      qq = ((BooleanQuery)q).clauses().get(0).getQuery();
      if (qq instanceof TermQuery) {
        qq = ((BooleanQuery)q).clauses().get(1).getQuery();
      }

      if (qq instanceof FilterQuery) {
        qq = ((FilterQuery)qq).getQuery();
      }

      assertEquals(26, ((TermInSetQuery) qq).getTermData().size());

      // test mixed boolean query, including quotes (which shouldn't matter)
      qParser = QParser.getParser("foo_s:(a +aaa b -bbb c d e f bar_s:(qqq www) g h i j k l m n o p q r s t u v w x y z)", req);
      qParser.setIsFilter(true); // this may change in the future
      qParser.setParams(params);
      q = qParser.getQuery();
      assertEquals(4, ((BooleanQuery)q).clauses().size());
      qq = null;
      for (BooleanClause clause : ((BooleanQuery)q).clauses()) {
        qq = clause.getQuery();
        if (qq instanceof TermInSetQuery) break;
      }
      assertEquals(26, ((TermInSetQuery)qq).getTermData().size());

      // test terms queries of two different fields (LUCENE-7637 changed to require all terms be in the same field)
      StringBuilder sb = new StringBuilder();
      for (int i=0; i<17; i++) {
        char letter = (char)('a'+i);
        sb.append("foo_s:" + letter + " bar_s:" + letter + " ");
      }
      qParser = QParser.getParser(sb.toString(), req);
      qParser.setIsFilter(true); // this may change in the future
      qParser.setParams(params);
      q = qParser.getQuery();
      assertEquals(2, ((BooleanQuery)q).clauses().size());
      for (BooleanClause clause : ((BooleanQuery)q).clauses()) {
        qq = clause.getQuery();
        assertEquals(17, ((TermInSetQuery)qq).getTermData().size());
      }
    }
    req.close();
  }

  @Test
  public void testManyClauses_Solr() throws Exception {
    final String a = "1 a 2 b 3 c 10 d 11 12 "; // 10 terms
    
    // this should exceed our solrconfig.xml level (solr specific) maxBooleanClauses limit
    // even though it's not long enough to trip the Lucene level (global) limit
    final String too_long = "id:(" + a + a + a + a + a + ")";

    final String expectedMsg = "Too many clauses";
    ignoreException(expectedMsg);
    SolrException e = expectThrows(SolrException.class, "expected SolrException",
                                   () -> assertJQ(req("q", too_long), "/response/numFound==6"));
    assertThat(e.getMessage(), containsString(expectedMsg));
    
    // but should still work as a filter query since TermsQuery can be used...
    assertJQ(req("q","*:*", "fq", too_long)
             ,"/response/numFound==6");
    assertJQ(req("q","*:*", "fq", too_long, "sow", "false")
             ,"/response/numFound==6");
    assertJQ(req("q","*:*", "fq", too_long, "sow", "true")
             ,"/response/numFound==6");
  }
    
  @Test
  public void testManyClauses_Lucene() throws Exception {
    final int numZ = BooleanQuery.getMaxClauseCount();
    
    final String a = "1 a 2 b 3 c 10 d 11 12 "; // 10 terms
    final StringBuilder sb = new StringBuilder("id:(");
    for (int i = 0; i < numZ; i++) {
      sb.append('z').append(i).append(' ');
    }
    sb.append(a);
    sb.append(")");
    
    // this should trip the lucene level global BooleanQuery.getMaxClauseCount() limit,
    // causing a parsing error, before Solr even get's a chance to enforce it's lower level limit
    final String way_too_long = sb.toString();

    final String expectedMsg = "too many boolean clauses";
    ignoreException(expectedMsg);
    SolrException e = expectThrows(SolrException.class, "expected SolrException",
                                   () -> assertJQ(req("q", way_too_long), "/response/numFound==6"));
    assertThat(e.getMessage(), containsString(expectedMsg));
    
    assertNotNull(e.getCause());
    assertEquals(SyntaxError.class, e.getCause().getClass());
    
    assertNotNull(e.getCause().getCause());
    assertEquals(BooleanQuery.TooManyClauses.class, e.getCause().getCause().getClass());

    // but should still work as a filter query since TermsQuery can be used...
    assertJQ(req("q","*:*", "fq", way_too_long)
        ,"/response/numFound==6");
    assertJQ(req("q","*:*", "fq", way_too_long, "sow", "false")
        ,"/response/numFound==6");
    assertJQ(req("q","*:*", "fq", way_too_long, "sow", "true")
        ,"/response/numFound==6");
  }

  @Test
  public void testComments() throws Exception {
    assertJQ(req("q", "id:1 id:2 /* *:* */ id:3")
        , "/response/numFound==3"
    );

    //
    assertJQ(req("q", "id:1 /**.*/")
        , "/response/numFound==1"  // if it matches more than one, it's being treated as a regex.
    );


    // don't match comment start in string
    assertJQ(req("q", " \"/*\" id:1 id:2 \"*/\" id:3")
        , "/response/numFound==3"
    );

    // don't match an end of comment within  a string
    // assertJQ(req("q","id:1 id:2 /* \"*/\" *:* */ id:3")
    //     ,"/response/numFound==3"
    // );
    // removed this functionality - there's more of a danger to thinking we're in a string.
    //   can't do it */  ......... '

    // nested comments
    assertJQ(req("q", "id:1 /* id:2 /* */ /* /**/ id:3 */ id:10 */ id:11")
        , "/response/numFound==2"
    );

  }

  @Test
  public void testFilter() throws Exception {

    // normal test "solrconfig.xml" has autowarm set to 2...
    for (int i = 0; i < 10; i++) {
      assertJQ(req("q", "*:* " + i, "fq", "filter(just_to_clear_the_cache) filter(id:10000" + i + ") filter(id:10001" + i + ")")
          , "/response/numFound==0"
      );
    }
    assertU(adoc("id", "777"));
    delI("777");
    assertU(commit());  // arg... commit no longer "commits" unless there has been a change.


    final MetricsMap filterCacheStats = (MetricsMap)((SolrMetricManager.GaugeWrapper)h.getCore().getCoreMetricManager().getRegistry()
        .getMetrics().get("CACHE.searcher.filterCache")).getGauge();
    assertNotNull(filterCacheStats);
    final MetricsMap queryCacheStats = (MetricsMap)((SolrMetricManager.GaugeWrapper)h.getCore().getCoreMetricManager().getRegistry()
        .getMetrics().get("CACHE.searcher.queryResultCache")).getGauge();

    assertNotNull(queryCacheStats);


    long inserts = (Long) filterCacheStats.getValue().get("inserts");
    long hits = (Long) filterCacheStats.getValue().get("hits");

    assertJQ(req("q", "doesnotexist filter(id:1) filter(qqq_s:X) filter(abcdefg)")
        , "/response/numFound==2"
    );

    inserts += 3;
    assertEquals(inserts, ((Long) filterCacheStats.getValue().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getValue().get("hits")).longValue());

    assertJQ(req("q", "doesnotexist2 filter(id:1) filter(qqq_s:X) filter(abcdefg)")
        , "/response/numFound==2"
    );

    hits += 3;
    assertEquals(inserts, ((Long) filterCacheStats.getValue().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getValue().get("hits")).longValue());

    // make sure normal "fq" parameters also hit the cache the same way
    assertJQ(req("q", "doesnotexist3", "fq", "id:1", "fq", "qqq_s:X", "fq", "abcdefg")
        , "/response/numFound==0"
    );

    hits += 3;
    assertEquals(inserts, ((Long) filterCacheStats.getValue().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getValue().get("hits")).longValue());

    // try a query deeply nested in a FQ
    assertJQ(req("q", "*:* doesnotexist4", "fq", "(id:* +(filter(id:1) filter(qqq_s:X) filter(abcdefg)) )")
        , "/response/numFound==2"
    );

    inserts += 1;  // +1 for top level fq
    hits += 3;
    assertEquals(inserts, ((Long) filterCacheStats.getValue().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getValue().get("hits")).longValue());

    // retry the complex FQ and make sure hashCode/equals works as expected w/ filter queries
    assertJQ(req("q", "*:* doesnotexist5", "fq", "(id:* +(filter(id:1) filter(qqq_s:X) filter(abcdefg)) )")
        , "/response/numFound==2"
    );

    hits += 1;  // top-level fq should have been found.
    assertEquals(inserts, ((Long) filterCacheStats.getValue().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getValue().get("hits")).longValue());


    // try nested filter with multiple top-level args (i.e. a boolean query)
    assertJQ(req("q", "*:* +filter(id:1 filter(qqq_s:X) abcdefg)")
        , "/response/numFound==2"
    );

    hits += 1;  // the inner filter
    inserts += 1; // the outer filter
    assertEquals(inserts, ((Long) filterCacheStats.getValue().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getValue().get("hits")).longValue());

    // test the score for a filter, and that default score is 0
    assertJQ(req("q", "+filter(*:*) +filter(id:1)", "fl", "id,score", "sort", "id asc")
        , "/response/docs/[0]/score==0.0"
    );

    assertJQ(req("q", "+filter(*:*)^=10 +filter(id:1)", "fl", "id,score", "sort", "id asc")
        , "/response/docs/[0]/score==10.0"
    );

    assertU(adoc("id", "40", "wdf_nocase", "just some text, don't want NPE"));
    assertU(commit());

    // See SOLR-11555. If wdff removes all the characters, an NPE occurs.
    // try q and fq
    assertJQ(req("q", "filter(wdf_nocase:&)", "fl", "id", "debug", "query")
        , "/response/numFound==0"
    );
    assertJQ(req("fq", "filter(wdf_nocase:.,)", "fl", "id", "debug", "query")
        , "/response/numFound==0"
    );

    // Insure the same behavior as with bare clause, just not filter
    assertJQ(req("q", "wdf_nocase:&", "fl", "id", "debug", "query")
        , "/response/numFound==0"
    );
    assertJQ(req("fq", "wdf_nocase:.,", "fl", "id", "debug", "query")
        , "/response/numFound==0"
    );

  }


  @Test
  public void testRegex() throws Exception {
    // leading slash in a regex fixed by SOLR-8605
    assertJQ(req("q", "rrr_s:/\\/lead.*/", "fl","id")
        , "/response/docs==[{id:'13'}]"
    );

  }

  // parsing performance test
  // Run from command line with ant test -Dtestcase=TestSolrQueryParser -Dtestmethod=testParsingPerformance -Dtests.asserts=false 2>/dev/null | grep QPS
  @Test
  public void testParsingPerformance() throws Exception {
    String[] args = {"-queries","100" ,"-iter","1000", "-clauses","100", "-format","term%d", "-seed","0"};
    args = new String[] {"-queries","1000" ,"-iter","2000", "-clauses","10", "-format","term%d", "-seed","0"};
    // args = new String[] {"-queries","1000" ,"-iter","1000000000", "-clauses","10", "-format","term%d", "-seed","0"};

    boolean assertOn = false;
    assert assertOn = true;
    if (assertOn) {
      // System.out.println("WARNING! Assertions are enabled!!!! Will only execute small run.  Change with -Dtests.asserts=false");
      args = new String[]{"-queries","10" ,"-iter","2", "-clauses","20", "-format","term%d", "-seed","0"};
    }


    int iter = 1000;
    int numQueries = 100;
    int maxClauses = 5;
    int maxTerm = 10000000;
    String format = "term%d";
    String field = "foo_s";
    long seed = 0;
    boolean isFilter = true;
    boolean rewrite = false;

    String otherStuff = "";

    for (int i = 0; i < args.length; i++) {
      String a = args[i];
      if ("-queries".equals(a)) {
        numQueries = Integer.parseInt(args[++i]);
      } else if ("-iter".equals(a)) {
        iter = Integer.parseInt(args[++i]);
      } else if ("-clauses".equals(a)) {
        maxClauses = Integer.parseInt(args[++i]);
      } else if ("-format".equals(a)) {
        format = args[++i];
      } else if ("-seed".equals(a)) {
        seed = Long.parseLong(args[++i]);
      } else {
        otherStuff = otherStuff + " " + a;
      }
    }

    Random r = new Random(seed);

    String[] queries = new String[numQueries];
    for (int i = 0; i < queries.length; i++) {
      StringBuilder sb = new StringBuilder();
      boolean explicitField = r.nextInt(5) == 0;
      if (!explicitField) {
        sb.append(field + ":(");
      }

      sb.append(otherStuff).append(" ");

      int nClauses = r.nextInt(maxClauses) + 1;  // TODO: query parse can't parse () for some reason???

      for (int c = 0; c<nClauses; c++) {
        String termString = String.format(Locale.US, format, r.nextInt(maxTerm));
        if (explicitField) {
          sb.append(field).append(':');
        }
        sb.append(termString);
        sb.append(' ');
      }

      if (!explicitField) {
        sb.append(")");
      }
      queries[i] = sb.toString();
      // System.out.println(queries[i]);
    }

    SolrQueryRequest req = req();

    long start = System.nanoTime();

    int ret = 0;
    for (int i=0; i<iter; i++) {
      for (String qStr : queries) {
        QParser parser = QParser.getParser(qStr,req);
        parser.setIsFilter(isFilter);
        Query q = parser.getQuery();
        if (rewrite) {
          // TODO: do rewrite
        }
        ret += q.getClass().hashCode(); // use the query somehow
      }
    }

    long end = System.nanoTime();

    System.out.println((assertOn ? "WARNING, assertions enabled. " : "") + "ret=" + ret + " Parser QPS:" + ((long)numQueries * iter)*1000000000/(end-start));

    req.close();
  }

  @Test
  public void testSplitOnWhitespace_Basic() throws Exception {
    // The "syn" field has synonyms loaded from synonyms.txt

    assertJQ(req("df", "syn", "q", "wifi", "sow", "true") // retrieve the single document containing literal "wifi"
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );

    assertJQ(req("df", "syn", "q", "wi fi", "sow", "false") // trigger the "wi fi => wifi" synonym
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );

    assertJQ(req("df", "syn", "q", "wi fi", "sow", "true")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );

    assertJQ(req("df", "syn", "q", "{!lucene sow=false}wi fi")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "{!lucene sow=true}wi fi")
        , "/response/numFound==0"
    );

    assertJQ(req("df", "syn", "q", "{!lucene}wi fi") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
  }

  public void testSplitOnWhitespace_Comments() throws Exception {
    // The "syn" field has synonyms loaded from synonyms.txt

    assertJQ(req("df", "syn", "q", "wifi", "sow", "true") // retrieve the single document containing literal "wifi"
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "wi fi", "sow", "false") // trigger the "wi fi => wifi" synonym
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "wi /* foo */ fi", "sow", "false") // trigger the "wi fi => wifi" synonym
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "wi /* foo */ /* bar */ fi", "sow", "false") // trigger the "wi fi => wifi" synonym
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", " /* foo */ wi fi /* bar */", "sow", "false") // trigger the "wi fi => wifi" synonym
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", " /* foo */ wi /* bar */ fi /* baz */", "sow", "false") // trigger the "wi fi => wifi" synonym
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );

    assertJQ(req("df", "syn", "q", "wi fi", "sow", "true")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi /* foo */ fi", "sow", "true")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi /* foo */ /* bar */ fi", "sow", "true")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "/* foo */ wi fi /* bar */", "sow", "true")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "/* foo */ wi /* bar */ fi /* baz */", "sow", "true")
        , "/response/numFound==0"
    );

    assertJQ(req("df", "syn", "q", "wi fi") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "wi /* foo */ fi") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "wi /* foo */ /* bar */ fi") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", " /* foo */ wi fi /* bar */") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", " /* foo */ wi /* bar */ fi /* baz */") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );


    assertJQ(req("df", "syn", "q", "{!lucene sow=false}wi fi")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "{!lucene sow=false}wi /* foo */ fi")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "{!lucene sow=false}wi /* foo */ /* bar */ fi")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "{!lucene sow=false}/* foo */ wi fi /* bar */")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "{!lucene sow=false}/* foo */ wi /* bar */ fi /* baz */")
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );

    assertJQ(req("df", "syn", "q", "{!lucene sow=true}wi fi")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "{!lucene sow=true}wi /* foo */ fi")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "{!lucene sow=true}wi /* foo */ /* bar */ fi")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "{!lucene sow=true}/* foo */ wi fi /* bar */")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "{!lucene sow=true}/* foo */ wi /* bar */ fi /* baz */")
        , "/response/numFound==0"
    );

    assertJQ(req("df", "syn", "q", "{!lucene}wi fi") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "{!lucene}wi /* foo */ fi") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "{!lucene}wi /* foo */ /* bar */ fi") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "{!lucene}/* foo */ wi fi /* bar */") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "{!lucene}/* foo */ wi /* bar */ fi /* baz */") // default sow=false
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
  }

  public void testOperatorsAndMultiWordSynonyms() throws Exception {
    // The "syn" field has synonyms loaded from synonyms.txt

    assertJQ(req("df", "syn", "q", "wifi", "sow", "true") // retrieve the single document containing literal "wifi"
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );
    assertJQ(req("df", "syn", "q", "wi fi", "sow", "false") // trigger the "wi fi => wifi" synonym
        , "/response/numFound==1"
        , "/response/docs/[0]/id=='20'"
    );

    assertJQ(req("df", "syn", "q", "+wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "-wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "!wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi* fi", "sow", "false")    // matches because wi* matches wifi
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "w? fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi~1 fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi^2 fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi^=2 fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi +fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi -fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi !fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi*", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi?", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi~1", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi^2", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi^=2", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "syn:wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi syn:fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "NOT wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi NOT fi", "sow", "false")
        , "/response/numFound==0"
    );

    assertJQ(req("df", "syn", "q", "wi fi AND ATM", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "ATM AND wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi && ATM", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "ATM && wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "(wi fi) AND ATM", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "ATM AND (wi fi)", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "(wi fi) && ATM", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "ATM && (wi fi)", "sow", "false")
        , "/response/numFound==1"
    );

    assertJQ(req("df", "syn", "q", "wi fi OR NotThereAtAll", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "NotThereAtAll OR wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi || NotThereAtAll", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "NotThereAtAll || wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "(wi fi) OR NotThereAtAll", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "NotThereAtAll OR (wi fi)", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "(wi fi) || NotThereAtAll", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "NotThereAtAll || (wi fi)", "sow", "false")
        , "/response/numFound==1"
    );

    assertJQ(req("df", "syn", "q", "\"wi\" fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi \"fi\"", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "(wi) fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi (fi)", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "/wi/ fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi /fi/", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "(wi fi)", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "+(wi fi)", "sow", "false")
        , "/response/numFound==1"
    );

    @SuppressWarnings({"rawtypes"})
    Map all = (Map) Utils.fromJSONString(h.query(req("q", "*:*", "rows", "0", "wt", "json")));
    int totalDocs = Integer.parseInt(((Map)all.get("response")).get("numFound").toString());
    int allDocsExceptOne = totalDocs - 1;

    assertJQ(req("df", "syn", "q", "-(wi fi)", "sow", "false")
        , "/response/numFound==" + allDocsExceptOne  // one doc contains "wifi" in the syn field
    );
    assertJQ(req("df", "syn", "q", "!(wi fi)", "sow", "false")
        , "/response/numFound==" + allDocsExceptOne  // one doc contains "wifi" in the syn field
    );
    assertJQ(req("df", "syn", "q", "NOT (wi fi)", "sow", "false")
        , "/response/numFound==" + allDocsExceptOne  // one doc contains "wifi" in the syn field
    );
    assertJQ(req("df", "syn", "q", "(wi fi)^2", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "(wi fi)^=2", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "syn:(wi fi)", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "+ATM wi fi", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "-ATM wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "-NotThereAtAll wi fi", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "!ATM wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "!NotThereAtAll wi fi", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "NOT ATM wi fi", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "NOT NotThereAtAll wi fi", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "AT* wi fi", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "AT? wi fi", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "\"ATM\" wi fi", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "wi fi +ATM", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "wi fi -ATM", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi -NotThereAtAll", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "wi fi !ATM", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi !NotThereAtAll", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "wi fi NOT ATM", "sow", "false")
        , "/response/numFound==0"
    );
    assertJQ(req("df", "syn", "q", "wi fi NOT NotThereAtAll", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "wi fi AT*", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "wi fi AT?", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "wi fi \"ATM\"", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "\"wi fi\"~2", "sow", "false")
        , "/response/numFound==1"
    );
    assertJQ(req("df", "syn", "q", "syn:\"wi fi\"", "sow", "false")
        , "/response/numFound==1"
    );
  }

  @Test
  public void testAutoGeneratePhraseQueries() throws Exception {
    ModifiableSolrParams noSowParams = new ModifiableSolrParams();
    ModifiableSolrParams sowFalseParams = new ModifiableSolrParams();
    sowFalseParams.add("sow", "false");
    ModifiableSolrParams sowTrueParams = new ModifiableSolrParams();
    sowTrueParams.add("sow", "true");

    // From synonyms.txt:
    //
    //     crow blackbird, grackle
    //
    try (SolrQueryRequest req = req()) {

      for (SolrParams params : Arrays.asList(noSowParams, sowFalseParams)) {
        QParser qParser = QParser.getParser("text:grackle", req); // "text" has autoGeneratePhraseQueries="true"
        qParser.setParams(sowFalseParams);
        Query q = qParser.getQuery();
        assertEquals("(text:\"crow blackbird\" text:grackl)", q.toString());
      }

      QParser qParser = QParser.getParser("text:grackle", req);
      qParser.setParams(sowTrueParams);
      Query q = qParser.getQuery();
      assertEquals("spanOr([spanNear([text:crow, text:blackbird], 0, true), text:grackl])", q.toString());

      for (SolrParams params : Arrays.asList(noSowParams, sowTrueParams, sowFalseParams)) {
        qParser = QParser.getParser("text_sw:grackle", req); // "text_sw" doesn't specify autoGeneratePhraseQueries => default false
        qParser.setParams(params);
        q = qParser.getQuery();
        assertEquals("((+text_sw:crow +text_sw:blackbird) text_sw:grackl)", q.toString());
      }
    }
  }

  @Test
  public void testShingleQueries() throws Exception {
    ModifiableSolrParams sowFalseParams = new ModifiableSolrParams();
    sowFalseParams.add("sow", "false");

    try (SolrQueryRequest req = req(sowFalseParams)) {
      QParser qParser = QParser.getParser("shingle23:(A B C)", req);
      Query q = qParser.getQuery();
      assertEquals("Synonym(shingle23:A_B shingle23:A_B_C) shingle23:B_C", q.toString());
    }

    assertJQ(req("df", "shingle23", "q", "A B C", "sow", "false")
        , "/response/numFound==1"
    );
  }


  public void testSynonymQueryStyle() throws Exception {

    Query q = QParser.getParser("tabby", req(params("df", "t_pick_best_foo"))).getQuery();
    assertEquals("(t_pick_best_foo:tabbi | t_pick_best_foo:cat | t_pick_best_foo:felin | t_pick_best_foo:anim)", q.toString());

    q = QParser.getParser("tabby", req(params("df", "t_as_distinct_foo"))).getQuery();
    assertEquals("t_as_distinct_foo:tabbi t_as_distinct_foo:cat t_as_distinct_foo:felin t_as_distinct_foo:anim", q.toString());

    /*confirm autoGeneratePhraseQueries always builds OR queries*/
    q = QParser.getParser("jeans",  req(params("df", "t_as_distinct_foo", "sow", "false"))).getQuery();
    assertEquals("(t_as_distinct_foo:\"denim pant\" t_as_distinct_foo:jean)", q.toString());

    q = QParser.getParser("jeans",  req(params("df", "t_pick_best_foo", "sow", "false"))).getQuery();
    assertEquals("(t_pick_best_foo:\"denim pant\" | t_pick_best_foo:jean)", q.toString());
  }

  public void testSynonymsBoost_singleTermQuerySingleTermSynonyms_shouldParseBoostedQuery() throws Exception {
    //tiger, tigre|0.9
    Query q = QParser.getParser("tiger", req(params("df", "t_pick_best_boosted_foo"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:tigre)^0.9 | t_pick_best_boosted_foo:tiger)", q.toString());

    q = QParser.getParser("tiger", req(params("df", "t_as_distinct_boosted_foo"))).getQuery();
    assertEquals("(t_as_distinct_boosted_foo:tigre)^0.9 t_as_distinct_boosted_foo:tiger", q.toString());

    q = QParser.getParser("tiger", req(params("df", "t_as_same_term_boosted_foo"))).getQuery();
    assertEquals("Synonym(t_as_same_term_boosted_foo:tiger t_as_same_term_boosted_foo:tigre^0.9)", q.toString());

    //lynx => lince|0.8, lynx_canadensis|0.9
    q = QParser.getParser("lynx", req(params("df", "t_pick_best_boosted_foo"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:lince)^0.8 | (t_pick_best_boosted_foo:lynx_canadensis)^0.9)", q.toString());

    q = QParser.getParser("lynx", req(params("df", "t_as_distinct_boosted_foo"))).getQuery();
    assertEquals("(t_as_distinct_boosted_foo:lince)^0.8 (t_as_distinct_boosted_foo:lynx_canadensis)^0.9", q.toString());

    q = QParser.getParser("lynx", req(params("df", "t_as_same_term_boosted_foo"))).getQuery();
    assertEquals("Synonym(t_as_same_term_boosted_foo:lince^0.8 t_as_same_term_boosted_foo:lynx_canadensis^0.9)", q.toString());
  }

  public void testSynonymsBoost_singleTermQueryMultiTermSynonyms_shouldParseBoostedQuery() throws Exception {
    //leopard, big cat|0.8, bagheera|0.9, panthera pardus|0.85
    Query q = QParser.getParser("leopard", req(params("df", "t_pick_best_boosted_foo"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:\"big cat\")^0.8 | (t_pick_best_boosted_foo:bagheera)^0.9 | (t_pick_best_boosted_foo:\"panthera pardus\")^0.85 | t_pick_best_boosted_foo:leopard)", q.toString());

    q = QParser.getParser("leopard", req(params("df", "t_as_distinct_boosted_foo"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:\"big cat\")^0.8 (t_as_distinct_boosted_foo:bagheera)^0.9 (t_as_distinct_boosted_foo:\"panthera pardus\")^0.85 t_as_distinct_boosted_foo:leopard)", q.toString());

    q = QParser.getParser("leopard", req(params("df", "t_as_same_term_boosted_foo"))).getQuery();
    assertEquals("((t_as_same_term_boosted_foo:\"big cat\")^0.8 (t_as_same_term_boosted_foo:bagheera)^0.9 (t_as_same_term_boosted_foo:\"panthera pardus\")^0.85 t_as_same_term_boosted_foo:leopard)", q.toString());

    //lion => panthera leo|0.9, simba leo|0.8, kimba|0.75
    q = QParser.getParser("lion", req(params("df", "t_pick_best_boosted_foo"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:\"panthera leo\")^0.9 | (t_pick_best_boosted_foo:\"simba leo\")^0.8 | (t_pick_best_boosted_foo:kimba)^0.75)", q.toString());

    q = QParser.getParser("lion", req(params("df", "t_as_distinct_boosted_foo"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:\"panthera leo\")^0.9 (t_as_distinct_boosted_foo:\"simba leo\")^0.8 (t_as_distinct_boosted_foo:kimba)^0.75)", q.toString());

    q = QParser.getParser("lion", req(params("df", "t_as_same_term_boosted_foo"))).getQuery();
    assertEquals("((t_as_same_term_boosted_foo:\"panthera leo\")^0.9 (t_as_same_term_boosted_foo:\"simba leo\")^0.8 (t_as_same_term_boosted_foo:kimba)^0.75)", q.toString());
  }

  public void testSynonymsBoost_multiTermQuerySingleTermSynonyms_shouldParseBoostedQuery() throws Exception {
    //tiger, tigre|0.9
    //lynx => lince|0.8, lynx_canadensis|0.9
    Query q = QParser.getParser("tiger lynx", req(params("df", "t_pick_best_boosted_foo"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:tigre)^0.9 | t_pick_best_boosted_foo:tiger)" +
            " ((t_pick_best_boosted_foo:lince)^0.8 | (t_pick_best_boosted_foo:lynx_canadensis)^0.9)", q.toString());

    q = QParser.getParser("tiger lynx", req(params("df", "t_as_distinct_boosted_foo"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:tigre)^0.9 t_as_distinct_boosted_foo:tiger)" +
            " ((t_as_distinct_boosted_foo:lince)^0.8 (t_as_distinct_boosted_foo:lynx_canadensis)^0.9)", q.toString());

    q = QParser.getParser("tiger lynx", req(params("df", "t_as_same_term_boosted_foo"))).getQuery();
    assertEquals("Synonym(t_as_same_term_boosted_foo:tiger t_as_same_term_boosted_foo:tigre^0.9)" +
            " Synonym(t_as_same_term_boosted_foo:lince^0.8 t_as_same_term_boosted_foo:lynx_canadensis^0.9)", q.toString());
  }

  public void testSynonymsBoost_multiTermQueryMultiTermSynonyms_shouldParseBoostedQuery() throws Exception {
    //leopard, big cat|0.8, bagheera|0.9, panthera pardus|0.85
    //lion => panthera leo|0.9, simba leo|0.8, kimba|0.75
    Query q = QParser.getParser("leopard lion", req(params("df", "t_pick_best_boosted_foo"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:\"big cat\")^0.8 | (t_pick_best_boosted_foo:bagheera)^0.9 | (t_pick_best_boosted_foo:\"panthera pardus\")^0.85 | t_pick_best_boosted_foo:leopard)" +
        " ((t_pick_best_boosted_foo:\"panthera leo\")^0.9 | (t_pick_best_boosted_foo:\"simba leo\")^0.8 | (t_pick_best_boosted_foo:kimba)^0.75)", q.toString());

    q = QParser.getParser("leopard lion", req(params("df", "t_as_distinct_boosted_foo"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:\"big cat\")^0.8 (t_as_distinct_boosted_foo:bagheera)^0.9 (t_as_distinct_boosted_foo:\"panthera pardus\")^0.85 t_as_distinct_boosted_foo:leopard)" +
        " ((t_as_distinct_boosted_foo:\"panthera leo\")^0.9 (t_as_distinct_boosted_foo:\"simba leo\")^0.8 (t_as_distinct_boosted_foo:kimba)^0.75)", q.toString());

    q = QParser.getParser("leopard lion", req(params("df", "t_as_same_term_boosted_foo"))).getQuery();
    assertEquals("((t_as_same_term_boosted_foo:\"big cat\")^0.8 (t_as_same_term_boosted_foo:bagheera)^0.9 (t_as_same_term_boosted_foo:\"panthera pardus\")^0.85 t_as_same_term_boosted_foo:leopard)" +
            " ((t_as_same_term_boosted_foo:\"panthera leo\")^0.9 (t_as_same_term_boosted_foo:\"simba leo\")^0.8 (t_as_same_term_boosted_foo:kimba)^0.75)", q.toString());

  }

  public void testSynonymsBoost_singleConceptQuerySingleTermSynonym_shouldParseBoostedQuery() throws Exception {
    //panthera pardus, leopard|0.6
    Query q = QParser.getParser("panthera pardus story",req(params("df", "t_pick_best_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:leopard)^0.6 | t_pick_best_boosted_foo:\"panthera pardus\") t_pick_best_boosted_foo:story", q.toString());

    q = QParser.getParser("panthera pardus story", req(params("df", "t_as_distinct_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:leopard)^0.6 t_as_distinct_boosted_foo:\"panthera pardus\") t_as_distinct_boosted_foo:story", q.toString());

    q = QParser.getParser("panthera pardus story", req(params("df", "t_as_same_term_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_same_term_boosted_foo:leopard)^0.6 t_as_same_term_boosted_foo:\"panthera pardus\") t_as_same_term_boosted_foo:story", q.toString());

    //panthera tigris => tiger|0.99
    q = QParser.getParser("panthera tigris story", req(params("df", "t_pick_best_boosted_foo","sow", "false"))).getQuery();
    assertEquals("(t_pick_best_boosted_foo:tiger)^0.99 t_pick_best_boosted_foo:story", q.toString());

    q = QParser.getParser("panthera tigris story", req(params("df", "t_as_distinct_boosted_foo","sow", "false"))).getQuery();
    assertEquals("(t_as_distinct_boosted_foo:tiger)^0.99 t_as_distinct_boosted_foo:story", q.toString());

    q = QParser.getParser("panthera tigris story", req(params("df", "t_as_same_term_boosted_foo","sow", "false"))).getQuery();
    assertEquals("(t_as_same_term_boosted_foo:tiger)^0.99 t_as_same_term_boosted_foo:story", q.toString());
  }

  public void testSynonymsBoost_singleConceptQueryMultiTermSynonymWithMultipleBoost_shouldParseMultiplicativeBoostedQuery() throws Exception {
    //panthera blytheae, oldest|0.5 ancient|0.9 panthera
    Query q = QParser.getParser("panthera blytheae",req(params("df", "t_pick_best_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:\"oldest ancient panthera\")^0.45 | t_pick_best_boosted_foo:\"panthera blytheae\")", q.toString());

    q = QParser.getParser("panthera blytheae", req(params("df", "t_as_distinct_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:\"oldest ancient panthera\")^0.45 t_as_distinct_boosted_foo:\"panthera blytheae\")", q.toString());

    q = QParser.getParser("panthera blytheae", req(params("df", "t_as_same_term_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_same_term_boosted_foo:\"oldest ancient panthera\")^0.45 t_as_same_term_boosted_foo:\"panthera blytheae\")", q.toString());
  }

  public void testSynonymsBoost_singleConceptQueryMultiTermSynonyms_shouldParseBoostedQuery() throws Exception {
    //snow leopard, panthera uncia|0.9, big cat|0.8, white_leopard|0.6
    Query q = QParser.getParser("snow leopard",req(params("df", "t_pick_best_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:\"panthera uncia\")^0.9 | (t_pick_best_boosted_foo:\"big cat\")^0.8 | (t_pick_best_boosted_foo:white_leopard)^0.6 | t_pick_best_boosted_foo:\"snow leopard\")", q.toString());
    
    q = QParser.getParser("snow leopard", req(params("df", "t_as_distinct_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:\"panthera uncia\")^0.9 (t_as_distinct_boosted_foo:\"big cat\")^0.8 (t_as_distinct_boosted_foo:white_leopard)^0.6 t_as_distinct_boosted_foo:\"snow leopard\")", q.toString());

    q = QParser.getParser("snow leopard", req(params("df", "t_as_same_term_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_same_term_boosted_foo:\"panthera uncia\")^0.9 (t_as_same_term_boosted_foo:\"big cat\")^0.8 (t_as_same_term_boosted_foo:white_leopard)^0.6 t_as_same_term_boosted_foo:\"snow leopard\")", q.toString());

    //panthera onca => jaguar|0.95, big cat|0.85, black panther|0.65
    q = QParser.getParser("panthera onca", req(params("df", "t_pick_best_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:jaguar)^0.95 | (t_pick_best_boosted_foo:\"big cat\")^0.85 | (t_pick_best_boosted_foo:\"black panther\")^0.65)", q.toString());

    q = QParser.getParser("panthera onca", req(params("df", "t_as_distinct_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:jaguar)^0.95 (t_as_distinct_boosted_foo:\"big cat\")^0.85 (t_as_distinct_boosted_foo:\"black panther\")^0.65)", q.toString());

    q = QParser.getParser("panthera onca", req(params("df", "t_as_same_term_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_same_term_boosted_foo:jaguar)^0.95 (t_as_same_term_boosted_foo:\"big cat\")^0.85 (t_as_same_term_boosted_foo:\"black panther\")^0.65)", q.toString());

  }

  public void testSynonymsBoost_multiConceptQuerySingleTermSynonym_shouldParseBoostedQuery() throws Exception {
    //panthera pardus, leopard|0.6
    //tiger, tigre|0.9
    Query q = QParser.getParser("panthera pardus tiger",req(params("df", "t_pick_best_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:leopard)^0.6 | t_pick_best_boosted_foo:\"panthera pardus\") ((t_pick_best_boosted_foo:tigre)^0.9 | t_pick_best_boosted_foo:tiger)", q.toString());

    q = QParser.getParser("panthera pardus tiger", req(params("df", "t_as_distinct_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:leopard)^0.6 t_as_distinct_boosted_foo:\"panthera pardus\") ((t_as_distinct_boosted_foo:tigre)^0.9 t_as_distinct_boosted_foo:tiger)", q.toString());

    q = QParser.getParser("panthera pardus tiger", req(params("df", "t_as_same_term_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_same_term_boosted_foo:leopard)^0.6 t_as_same_term_boosted_foo:\"panthera pardus\") Synonym(t_as_same_term_boosted_foo:tiger t_as_same_term_boosted_foo:tigre^0.9)", q.toString());
  }

  public void testSynonymsBoost_multiConceptsQueryMultiTermSynonyms_shouldParseBoostedQuery() throws Exception {
    //snow leopard, panthera uncia|0.9, big cat|0.8, white_leopard|0.6
    //panthera onca => jaguar|0.95, big cat|0.85, black panther|0.65
    Query q = QParser.getParser("snow leopard panthera onca",req(params("df", "t_pick_best_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:\"panthera uncia\")^0.9 | (t_pick_best_boosted_foo:\"big cat\")^0.8 | (t_pick_best_boosted_foo:white_leopard)^0.6 | t_pick_best_boosted_foo:\"snow leopard\")" +
        " ((t_pick_best_boosted_foo:jaguar)^0.95 | (t_pick_best_boosted_foo:\"big cat\")^0.85 | (t_pick_best_boosted_foo:\"black panther\")^0.65)", q.toString());

    q = QParser.getParser("snow leopard panthera onca", req(params("df", "t_as_distinct_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:\"panthera uncia\")^0.9 (t_as_distinct_boosted_foo:\"big cat\")^0.8 (t_as_distinct_boosted_foo:white_leopard)^0.6 t_as_distinct_boosted_foo:\"snow leopard\")" +
        " ((t_as_distinct_boosted_foo:jaguar)^0.95 (t_as_distinct_boosted_foo:\"big cat\")^0.85 (t_as_distinct_boosted_foo:\"black panther\")^0.65)", q.toString());

    q = QParser.getParser("snow leopard panthera onca", req(params("df", "t_as_same_term_boosted_foo","sow", "false"))).getQuery();
    assertEquals("((t_as_same_term_boosted_foo:\"panthera uncia\")^0.9 (t_as_same_term_boosted_foo:\"big cat\")^0.8 (t_as_same_term_boosted_foo:white_leopard)^0.6 t_as_same_term_boosted_foo:\"snow leopard\")" +
            " ((t_as_same_term_boosted_foo:jaguar)^0.95 (t_as_same_term_boosted_foo:\"big cat\")^0.85 (t_as_same_term_boosted_foo:\"black panther\")^0.65)", q.toString());

  }
    
  public void testSynonymsBoost_edismaxBoost_shouldParseBoostedPhraseQuery() throws Exception {
    Query q = QParser.getParser("snow leopard lion","edismax",true, req(params("sow", "false","qf", "t_pick_best_boosted_foo^10"))).getQuery();
    assertEquals("+(" +
        "((((t_pick_best_boosted_foo:\"panthera uncia\")^0.9 | (t_pick_best_boosted_foo:\"big cat\")^0.8 | (t_pick_best_boosted_foo:white_leopard)^0.6 | t_pick_best_boosted_foo:\"snow leopard\"))^10.0)" +
        " ((((t_pick_best_boosted_foo:\"panthera leo\")^0.9 | (t_pick_best_boosted_foo:\"simba leo\")^0.8 | (t_pick_best_boosted_foo:kimba)^0.75))^10.0)" +
        ")", q.toString());

    q = QParser.getParser("snow leopard lion","edismax",true, req(params("sow", "false","qf", "t_as_distinct_boosted_foo^10"))).getQuery();
    assertEquals("+(" +
        "(((t_as_distinct_boosted_foo:\"panthera uncia\")^0.9 (t_as_distinct_boosted_foo:\"big cat\")^0.8 (t_as_distinct_boosted_foo:white_leopard)^0.6 t_as_distinct_boosted_foo:\"snow leopard\")^10.0)" +
        " (((t_as_distinct_boosted_foo:\"panthera leo\")^0.9 (t_as_distinct_boosted_foo:\"simba leo\")^0.8 (t_as_distinct_boosted_foo:kimba)^0.75)^10.0))", q.toString());

    q = QParser.getParser("snow leopard lion","edismax",true, req(params("sow", "false","qf", "t_as_same_term_boosted_foo^10"))).getQuery();
    assertEquals("+(" +
            "(((t_as_same_term_boosted_foo:\"panthera uncia\")^0.9 (t_as_same_term_boosted_foo:\"big cat\")^0.8 (t_as_same_term_boosted_foo:white_leopard)^0.6 t_as_same_term_boosted_foo:\"snow leopard\")^10.0)" +
            " (((t_as_same_term_boosted_foo:\"panthera leo\")^0.9 (t_as_same_term_boosted_foo:\"simba leo\")^0.8 (t_as_same_term_boosted_foo:kimba)^0.75)^10.0))", q.toString());

  }

  public void testSynonymsBoost_phraseQueryMultiTermSynonymsBoost_shouldParseBoostedSpanQuery() throws Exception {
    Query q = QParser.getParser("\"snow leopard lion\"", req(params("df", "t_pick_best_boosted_foo", "sow", "false"))).getQuery();
    assertEquals("spanNear([" +
        "spanOr([" +
        "(spanNear([t_pick_best_boosted_foo:panthera, t_pick_best_boosted_foo:uncia], 0, true))^0.9," +
        " (spanNear([t_pick_best_boosted_foo:big, t_pick_best_boosted_foo:cat], 0, true))^0.8," +
        " (t_pick_best_boosted_foo:white_leopard)^0.6," +
        " spanNear([t_pick_best_boosted_foo:snow, t_pick_best_boosted_foo:leopard], 0, true)])," +
        " spanOr([" +
        "(spanNear([t_pick_best_boosted_foo:panthera, t_pick_best_boosted_foo:leo], 0, true))^0.9," +
        " (spanNear([t_pick_best_boosted_foo:simba, t_pick_best_boosted_foo:leo], 0, true))^0.8," +
        " (t_pick_best_boosted_foo:kimba)^0.75])], 0, true)", q.toString());
  }

  public void testSynonymsBoost_phraseQueryMultiTermSynonymsMultipleBoost_shouldParseMultiplicativeBoostedSpanQuery() throws Exception {
    Query q = QParser.getParser("\"panthera blytheae lion\"", req(params("df", "t_pick_best_boosted_foo", "sow", "false"))).getQuery();
    assertEquals("spanNear([" +
            "spanOr([" +
            "(spanNear([t_pick_best_boosted_foo:oldest, t_pick_best_boosted_foo:ancient, t_pick_best_boosted_foo:panthera], 0, true))^0.45," +
            " spanNear([t_pick_best_boosted_foo:panthera, t_pick_best_boosted_foo:blytheae], 0, true)])," +
            " spanOr([" +
            "(spanNear([t_pick_best_boosted_foo:panthera, t_pick_best_boosted_foo:leo], 0, true))^0.9," +
            " (spanNear([t_pick_best_boosted_foo:simba, t_pick_best_boosted_foo:leo], 0, true))^0.8," +
            " (t_pick_best_boosted_foo:kimba)^0.75])], 0, true)", q.toString());
  }

  public void testSynonymsBoost_BoostMissing_shouldAssignDefaultBoost() throws Exception {
    //leopard, big cat|0.8, bagheera|0.9, panthera pardus|0.85
    Query q = QParser.getParser("leopard", req(params("df", "t_pick_best_boosted_foo"))).getQuery();
    assertEquals("((t_pick_best_boosted_foo:\"big cat\")^0.8 | (t_pick_best_boosted_foo:bagheera)^0.9 | (t_pick_best_boosted_foo:\"panthera pardus\")^0.85 | t_pick_best_boosted_foo:leopard)", q.toString());

    q = QParser.getParser("leopard", req(params("df", "t_as_distinct_boosted_foo"))).getQuery();
    assertEquals("((t_as_distinct_boosted_foo:\"big cat\")^0.8 (t_as_distinct_boosted_foo:bagheera)^0.9 (t_as_distinct_boosted_foo:\"panthera pardus\")^0.85 t_as_distinct_boosted_foo:leopard)", q.toString());
  }

  @Test
  public void testBadRequestInSetQuery() throws SyntaxError {
    SolrQueryRequest req = req();
    QParser qParser;
    String[] fieldSuffix = new String[] {
        "ti", "tf", "td", "tl",
        "i", "f", "d", "l",
        "is", "fs", "ds", "ls",
        "i_dv", "f_dv", "d_dv", "l_dv",
        "is_dv", "fs_dv", "ds_dv", "ls_dv",
        "i_dvo", "f_dvo", "d_dvo", "l_dvo",
    };

    for (String suffix:fieldSuffix) {
      //Good queries
      qParser = QParser.getParser("foo_" + suffix + ":(1 2 3 4 5 6 7 8 9 10 20 19 18 17 16 15 14 13 12 25)", req);
      qParser.setIsFilter(true);
      qParser.getQuery();
    }

    for (String suffix:fieldSuffix) {
      qParser = QParser.getParser("foo_" + suffix + ":(1 2 3 4 5 6 7 8 9 10 20 19 18 17 16 15 14 13 12 NOT_A_NUMBER)", req);
      qParser.setIsFilter(true); // this may change in the future
      SolrException e = expectThrows(SolrException.class, "Expecting exception", qParser::getQuery);
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue("Unexpected exception: " + e.getMessage(), e.getMessage().contains("Invalid Number: NOT_A_NUMBER"));
    }


  }

  @Test
  public void testFieldExistsQueries() throws SyntaxError {
    SolrQueryRequest req = req();
    String[] fieldSuffix = new String[] {
        "ti", "tf", "td", "tl", "tdt", // trie types
        "pi", "pf", "pd", "pl", "pdt", // point types
        "i", "f", "d", "l", "dt", "s", "b", // numeric types
        "is", "fs", "ds", "ls", "dts", "ss", "bs", // multi-valued
        "i_dv", "f_dv", "d_dv", "l_dv", "dt_dv", "s_dv", "b_dv", // numerics + docValues
        "is_dv", "fs_dv", "ds_dv", "ls_dv", "dts_dv", "ss_dv", "bs_dv", // multi-docValues
        "i_dvo", "f_dvo", "d_dvo", "l_dvo", "dt_dvo", // not indexed
        "t",
        "t_on", "b_norms", "s_norms", "dt_norms", "i_norms", "l_norms", "f_norms", "d_norms"
    };
    String[] existenceQueries = new String[] {
        "*", "[* TO *]"
    };

    for (String existenceQuery : existenceQueries) {
      for (String suffix : fieldSuffix) {
        IndexSchema indexSchema = h.getCore().getLatestSchema();
        String field = "foo_" + suffix;
        String query = field + ":" + existenceQuery;
        QParser qParser = QParser.getParser(query, req);
        Query createdQuery = qParser.getQuery();
        SchemaField schemaField = indexSchema.getField(field);

        // Test float & double realNumber queries differently
        if ("[* TO *]".equals(existenceQuery) && (schemaField.getType().getNumberType() == NumberType.DOUBLE || schemaField.getType().getNumberType() == NumberType.FLOAT)) {
          assertFalse("For float and double fields \"" + query + "\" is not an existence query, so the query returned should not be a DocValuesFieldExistsQuery.", createdQuery instanceof DocValuesFieldExistsQuery);
          assertFalse("For float and double fields \"" + query + "\" is not an existence query, so the query returned should not be a NormsFieldExistsQuery.", createdQuery instanceof NormsFieldExistsQuery);
          assertFalse("For float and double fields \"" + query + "\" is not an existence query, so NaN should not be matched via a ConstantScoreQuery.", createdQuery instanceof ConstantScoreQuery);
          assertFalse("For float and double fields\"" + query + "\" is not an existence query, so NaN should not be matched via a BooleanQuery (NaN and [* TO *]).", createdQuery instanceof BooleanQuery);
        } else {
          if (schemaField.hasDocValues()) {
            assertTrue("Field has docValues, so existence query \"" + query + "\" should return DocValuesFieldExistsQuery", createdQuery instanceof DocValuesFieldExistsQuery);
          } else if (!schemaField.omitNorms() && !schemaField.getType().isPointField()) { //TODO: Remove !isPointField() for SOLR-14199
            assertTrue("Field has norms and no docValues, so existence query \"" + query + "\" should return NormsFieldExistsQuery", createdQuery instanceof NormsFieldExistsQuery);
          } else if (schemaField.getType().getNumberType() == NumberType.DOUBLE || schemaField.getType().getNumberType() == NumberType.FLOAT) {
            assertTrue("PointField with NaN values must include \"exists or NaN\" if the field doesn't have norms or docValues: \"" + query + "\".", createdQuery instanceof ConstantScoreQuery);
            assertTrue("PointField with NaN values must include \"exists or NaN\" if the field doesn't have norms or docValues: \"" + query + "\".", ((ConstantScoreQuery)createdQuery).getQuery() instanceof BooleanQuery);
            assertEquals("PointField with NaN values must include \"exists or NaN\" if the field doesn't have norms or docValues: \"" + query + "\". This boolean query must be an OR.", 1, ((BooleanQuery)((ConstantScoreQuery)createdQuery).getQuery()).getMinimumNumberShouldMatch());
            assertEquals("PointField with NaN values must include \"exists or NaN\" if the field doesn't have norms or docValues: \"" + query + "\". This boolean query must have 2 clauses.", 2, ((BooleanQuery)((ConstantScoreQuery)createdQuery).getQuery()).clauses().size());
          } else {
            assertFalse("Field doesn't have docValues, so existence query \"" + query + "\" should not return DocValuesFieldExistsQuery", createdQuery instanceof DocValuesFieldExistsQuery);
            assertFalse("Field doesn't have norms, so existence query \"" + query + "\" should not return NormsFieldExistsQuery", createdQuery instanceof NormsFieldExistsQuery);
          }
        }
      }
    }
  }
}
