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

import java.util.Locale;
import java.util.Random;

import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.parser.QueryParser;
import org.apache.solr.query.FilterQuery;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestSolrQueryParser extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml", "schema12.xml");
    createIndex();
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

    assertU(commit());
  }

  @Test
  public void testPhrase() {
    // should generate a phrase of "now cow" and match only one doc
    assertQ(req("q", "text:now-cow", "indent", "true")
        , "//*[@numFound='1']"
    );
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
    assertJQ(req("q", "foo_dt:\"2013-03-08T00:46:15Z/DAY+000MILLISECONDS+00SECONDS+00MINUTES+00HOURS+0000000000YEARS+6MONTHS+3DAYS\"", "debug", "query")
        , "/debug/parsedquery=='foo_dt:2013-09-11T00:00:00Z'"
    );
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

    qParser = QParser.getParser("(text:x text:y)^=-3", req);
    q = qParser.getQuery();
    assertTrue(q instanceof BoostQuery);
    assertTrue(((BoostQuery) q).getQuery() instanceof ConstantScoreQuery);
    assertEquals(-3.0, ((BoostQuery) q).getBoost(), 0.0f);

    req.close();
  }


  // automatically use TermsQuery when appropriate
  @Test
  public void testAutoTerms() throws Exception {
    SolrQueryRequest req = req();
    QParser qParser;
    Query q,qq;

    // relevance query should not be a filter
    qParser = QParser.getParser("foo_s:(a b c)", req);
    q = qParser.getQuery();
    assertEquals(3, ((BooleanQuery)q).clauses().size());

    // small filter query should still use BooleanQuery
    if (QueryParser.TERMS_QUERY_THRESHOLD > 3) {
      qParser = QParser.getParser("foo_s:(a b c)", req);
      qParser.setIsFilter(true); // this may change in the future
      q = qParser.getQuery();
      assertEquals(3, ((BooleanQuery) q).clauses().size());
    }

    // large relevancy query should use BooleanQuery
    // TODO: we may decide that string fields shouldn't have relevance in the future... change to a text field w/o a stop filter if so
    qParser = QParser.getParser("foo_s:(a b c d e f g h i j k l m n o p q r s t u v w x y z)", req);
    q = qParser.getQuery();
    assertEquals(26, ((BooleanQuery)q).clauses().size());

    // large filter query should use TermsQuery
    qParser = QParser.getParser("foo_s:(a b c d e f g h i j k l m n o p q r s t u v w x y z)", req);
    qParser.setIsFilter(true); // this may change in the future
    q = qParser.getQuery();
    assertEquals(26, ((TermsQuery)q).getTermData().size());

    // large numeric filter query should use TermsQuery (for trie fields)
    qParser = QParser.getParser("foo_i:(1 2 3 4 5 6 7 8 9 10 20 19 18 17 16 15 14 13 12 11)", req);
    qParser.setIsFilter(true); // this may change in the future
    q = qParser.getQuery();
    assertEquals(20, ((TermsQuery)q).getTermData().size());

    // a filter() clause inside a relevancy query should be able to use a TermsQuery
    qParser = QParser.getParser("foo_s:aaa filter(foo_s:(a b c d e f g h i j k l m n o p q r s t u v w x y z))", req);
    q = qParser.getQuery();
    assertEquals(2, ((BooleanQuery)q).clauses().size());
    qq = ((BooleanQuery)q).clauses().get(0).getQuery();
    if (qq instanceof TermQuery) {
      qq = ((BooleanQuery)q).clauses().get(1).getQuery();
    }

    if (qq instanceof FilterQuery) {
      qq = ((FilterQuery)qq).getQuery();
    }

    assertEquals(26, ((TermsQuery)qq).getTermData().size());

    // test mixed boolean query, including quotes (which shouldn't matter)
    qParser = QParser.getParser("foo_s:(a +aaa b -bbb c d e f bar_s:(qqq www) g h i j k l m n o p q r s t u v w x y z)", req);
    qParser.setIsFilter(true); // this may change in the future
    q = qParser.getQuery();
    assertEquals(4, ((BooleanQuery)q).clauses().size());
    qq = null;
    for (BooleanClause clause : ((BooleanQuery)q).clauses()) {
      qq = clause.getQuery();
      if (qq instanceof TermsQuery) break;
    }
    assertEquals(26, ((TermsQuery)qq).getTermData().size());

    req.close();
  }

  @Test
  public void testManyClauses() throws Exception {
    String a = "1 a 2 b 3 c 10 d 11 12 "; // 10 terms
    StringBuilder sb = new StringBuilder("id:(");
    for (int i = 0; i < 1024; i++) { // historically, the max number of boolean clauses defaulted to 1024
      sb.append('z').append(i).append(' ');
    }
    sb.append(a);
    sb.append(")");

    String q = sb.toString();

    // This will still fail when used as the main query, but will pass in a filter query since TermsQuery can be used.
    assertJQ(req("q","*:*", "fq", q)
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


    final SolrInfoMBean filterCacheStats
        = h.getCore().getInfoRegistry().get("filterCache");
    assertNotNull(filterCacheStats);
    final SolrInfoMBean queryCacheStats
        = h.getCore().getInfoRegistry().get("queryResultCache");

    assertNotNull(queryCacheStats);


    long inserts = (Long) filterCacheStats.getStatistics().get("inserts");
    long hits = (Long) filterCacheStats.getStatistics().get("hits");

    assertJQ(req("q", "doesnotexist filter(id:1) filter(qqq_s:X) filter(abcdefg)")
        , "/response/numFound==2"
    );

    inserts += 3;
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue());

    assertJQ(req("q", "doesnotexist2 filter(id:1) filter(qqq_s:X) filter(abcdefg)")
        , "/response/numFound==2"
    );

    hits += 3;
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue());

    // make sure normal "fq" parameters also hit the cache the same way
    assertJQ(req("q", "doesnotexist3", "fq", "id:1", "fq", "qqq_s:X", "fq", "abcdefg")
        , "/response/numFound==0"
    );

    hits += 3;
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue());

    // try a query deeply nested in a FQ
    assertJQ(req("q", "*:* doesnotexist4", "fq", "(id:* +(filter(id:1) filter(qqq_s:X) filter(abcdefg)) )")
        , "/response/numFound==2"
    );

    inserts += 1;  // +1 for top level fq
    hits += 3;
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue());

    // retry the complex FQ and make sure hashCode/equals works as expected w/ filter queries
    assertJQ(req("q", "*:* doesnotexist5", "fq", "(id:* +(filter(id:1) filter(qqq_s:X) filter(abcdefg)) )")
        , "/response/numFound==2"
    );

    hits += 1;  // top-level fq should have been found.
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue());


    // try nested filter with multiple top-level args (i.e. a boolean query)
    assertJQ(req("q", "*:* +filter(id:1 filter(qqq_s:X) abcdefg)")
        , "/response/numFound==2"
    );

    hits += 1;  // the inner filter
    inserts += 1; // the outer filter
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue());
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue());

    // test the score for a filter, and that default score is 0
    assertJQ(req("q", "+filter(*:*) +filter(id:1)", "fl", "id,score", "sort", "id asc")
        , "/response/docs/[0]/score==0.0"
    );

    assertJQ(req("q", "+filter(*:*)^=10 +filter(id:1)", "fl", "id,score", "sort", "id asc")
        , "/response/docs/[0]/score==10.0"
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

}