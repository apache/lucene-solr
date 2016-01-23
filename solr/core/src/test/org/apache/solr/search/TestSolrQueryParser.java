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

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.ScoreAugmenter;
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
    v="how now brown cow";
    assertU(adoc("id","1", "text",v,  "text_np",v));
    v="now cow";
    assertU(adoc("id","2", "text",v,  "text_np",v));
    assertU(adoc("id","3", "foo_s","a ' \" \\ {! ) } ( { z"));  // A value filled with special chars

    assertU(adoc("id","10", "qqq_s","X"));
    assertU(adoc("id","11", "www_s","X"));
    assertU(adoc("id","12", "eee_s","X"));
    assertU(adoc("id","13", "eee_s","'balance'"));

    assertU(commit());
  }

  @Test
  public void testPhrase() {
    // should generate a phrase of "now cow" and match only one doc
    assertQ(req("q","text:now-cow", "indent","true")
        ,"//*[@numFound='1']"
    );
    // should generate a query of (now OR cow) and match both docs
    assertQ(req("q","text_np:now-cow", "indent","true")
        ,"//*[@numFound='2']"
    );
  }

  @Test
  public void testLocalParamsInQP() throws Exception {
    assertJQ(req("q","qaz {!term f=text v=$qq} wsx", "qq","now")
        ,"/response/numFound==2"
    );

    assertJQ(req("q","qaz {!term f=text v=$qq} wsx", "qq","nomatch")
        ,"/response/numFound==0"
    );

    assertJQ(req("q","qaz {!term f=text}now wsx", "qq","now")
        ,"/response/numFound==2"
    );

    assertJQ(req("q","qaz {!term f=foo_s v='a \\' \" \\\\ {! ) } ( { z'} wsx")           // single quote escaping
        ,"/response/numFound==1"
    );

    assertJQ(req("q","qaz {!term f=foo_s v=\"a ' \\\" \\\\ {! ) } ( { z\"} wsx")         // double quote escaping
        ,"/response/numFound==1"
    );

    // double-join to test back-to-back local params
    assertJQ(req("q","qaz {!join from=www_s to=eee_s}{!join from=qqq_s to=www_s}id:10" )
        ,"/response/docs/[0]/id=='12'"
    );
  }

  @Test
  public void testSolr4121() throws Exception {
    // At one point, balanced quotes messed up the parser(SOLR-4121)
    assertJQ(req("q","eee_s:'balance'", "indent","true")
        ,"/response/numFound==1"
    );
  }

  @Test
  public void testSyntax() throws Exception {
    // a bare * should be treated as *:*
    assertJQ(req("q","*", "df","doesnotexist_s")
        ,"/response/docs/[0]=="   // make sure we get something...
    );
    assertJQ(req("q","doesnotexist_s:*")
        ,"/response/numFound==0"   // nothing should be found
    );
    assertJQ(req("q","doesnotexist_s:( * * * )")
        ,"/response/numFound==0"   // nothing should be found
     );

    // length of date math caused issues...
    assertJQ(req("q","foo_dt:\"2013-03-08T00:46:15Z/DAY+000MILLISECONDS+00SECONDS+00MINUTES+00HOURS+0000000000YEARS+6MONTHS+3DAYS\"", "debug","query")
        ,"/debug/parsedquery=='foo_dt:2013-09-11T00:00:00Z'"
    );
  }

  @Test
  public void testNestedQueryModifiers() throws Exception {
    // One previous error was that for nested queries, outer parameters overrode nested parameters.
    // For example _query_:"\"a b\"~2" was parsed as "a b"

    String subqq="_query_:\"{!v=$qq}\"";

    assertJQ(req("q","_query_:\"\\\"how brown\\\"~2\""
        , "debug","query"
    )
        ,"/response/docs/[0]/id=='1'"
    );

    assertJQ(req("q",subqq, "qq","\"how brown\"~2"
        , "debug","query"
    )
        ,"/response/docs/[0]/id=='1'"
    );

    // Should explicit slop override?  It currently does not, but that could be considered a bug.
    assertJQ(req("q",subqq+"~1", "qq","\"how brown\"~2"
        , "debug","query"
    )
        ,"/response/docs/[0]/id=='1'"
    );

    // Should explicit slop override?  It currently does not, but that could be considered a bug.
    assertJQ(req("q","  {!v=$qq}~1", "qq","\"how brown\"~2"
        , "debug","query"
    )
        ,"/response/docs/[0]/id=='1'"
    );

    assertJQ(req("fq","id:1", "fl","id,score", "q", subqq+"^3", "qq","text:x^2"
        , "debug","query"
    )
        ,"/debug/parsedquery_toString=='((text:x)^2.0)^3.0'"
    );

    assertJQ(req("fq","id:1", "fl","id,score", "q", "  {!v=$qq}^3", "qq","text:x^2"
        , "debug","query"
    )
        ,"/debug/parsedquery_toString=='((text:x)^2.0)^3.0'"
    );

  }



  @Test
  public void testCSQ() throws Exception {
    SolrQueryRequest req = req();

    QParser qParser = QParser.getParser("text:x^=3", "lucene", req);
    Query q = qParser.getQuery();
    assertTrue( q instanceof BoostQuery);
    assertTrue(((BoostQuery) q).getQuery() instanceof ConstantScoreQuery);
    assertEquals(3.0, ((BoostQuery) q).getBoost(), 0.0f);

    qParser = QParser.getParser("(text:x text:y)^=-3", "lucene", req);
    q = qParser.getQuery();
    assertTrue( q instanceof BoostQuery);
    assertTrue(((BoostQuery) q).getQuery() instanceof ConstantScoreQuery);
    assertEquals(-3.0, ((BoostQuery) q).getBoost(), 0.0f);

    req.close();
  }

  @Test
  public void testComments() throws Exception {
    assertJQ(req("q","id:1 id:2 /* *:* */ id:3")
        ,"/response/numFound==3"
    );

    //
    assertJQ(req("q","id:1 /**.*/")
        ,"/response/numFound==1"  // if it matches more than one, it's being treated as a regex.
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
    assertJQ(req("q","id:1 /* id:2 /* */ /* /**/ id:3 */ id:10 */ id:11")
        ,"/response/numFound==2"
    );

  }

  @Test
  public void testFilter() throws Exception {

    // normal test "solrconfig.xml" has autowarm set to 2...
    for (int i=0; i<10; i++) {
      assertJQ(req("q","*:* "+ i, "fq","filter(just_to_clear_the_cache) filter(id:10000" + i + ") filter(id:10001" + i + ")")
          ,"/response/numFound==0"
      );
    }
    assertU(adoc("id","777"));
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

    assertJQ(req("q","doesnotexist filter(id:1) filter(qqq_s:X) filter(abcdefg)")
        ,"/response/numFound==2"
    );

    inserts+=3;
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue() );
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue() );

    assertJQ(req("q","doesnotexist2 filter(id:1) filter(qqq_s:X) filter(abcdefg)")
        ,"/response/numFound==2"
    );

    hits+=3;
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue() );
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue() );

    // make sure normal "fq" parameters also hit the cache the same way
    assertJQ(req("q","doesnotexist3", "fq","id:1", "fq", "qqq_s:X", "fq", "abcdefg")
        ,"/response/numFound==0"
    );

    hits+=3;
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue() );
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue() );

    // try a query deeply nested in a FQ
    assertJQ(req("q","*:* doesnotexist4", "fq","(id:* +(filter(id:1) filter(qqq_s:X) filter(abcdefg)) )")
        ,"/response/numFound==2"
    );

    inserts+=1;  // +1 for top level fq
    hits+=3;
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue() );
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue() );

    // retry the complex FQ and make sure hashCode/equals works as expected w/ filter queries
    assertJQ(req("q","*:* doesnotexist5", "fq","(id:* +(filter(id:1) filter(qqq_s:X) filter(abcdefg)) )")
        ,"/response/numFound==2"
    );

    hits+=1;  // top-level fq should have been found.
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue() );
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue() );


    // try nested filter with multiple top-level args (i.e. a boolean query)
    assertJQ(req("q","*:* +filter(id:1 filter(qqq_s:X) abcdefg)")
        ,"/response/numFound==2"
    );

    hits+=1;  // the inner filter
    inserts+=1; // the outer filter
    assertEquals(inserts, ((Long) filterCacheStats.getStatistics().get("inserts")).longValue() );
    assertEquals(hits, ((Long) filterCacheStats.getStatistics().get("hits")).longValue() );

    // test the score for a filter, and that default score is 0
    assertJQ(req("q","+filter(*:*) +filter(id:1)", "fl","id,score", "sort","id asc")
        ,"/response/docs/[0]/score==0.0"
    );

    assertJQ(req("q","+filter(*:*)^=10 +filter(id:1)", "fl","id,score", "sort","id asc")
        ,"/response/docs/[0]/score==10.0" 
    );

  }

}
