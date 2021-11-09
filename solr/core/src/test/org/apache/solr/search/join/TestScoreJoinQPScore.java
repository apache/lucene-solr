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
package org.apache.solr.search.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import com.codahale.metrics.Metric;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrCache;
import org.junit.BeforeClass;
import org.junit.Ignore;

public class TestScoreJoinQPScore extends SolrTestCaseJ4 {

  private static final String idField = "id";
  private static final String toField = "movieId_s";

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    System.setProperty("solr.filterCache.async", "true");
    initCore("solrconfig.xml", "schema12.xml");
  }

  public void testSimple() throws Exception {
    final String idField = "id";
    final String toField = "productId_s";

    clearIndex();

    // 0
    assertU(add(doc("t_description", "random text",
        "name", "name1",
        idField, "1")));

// 1

    assertU(add(doc("price_s", "10.0",
        idField, "2",
        toField, "1")));
// 2
    assertU(add(doc("price_s", "20.0",
        idField, "3",
        toField, "1")));
// 3
    assertU(add(doc("t_description", "more random text",
        "name", "name2",
        idField, "4")));
// 4
    assertU(add(doc("price_s", "10.0",
        idField, "5",
        toField, "4")));
// 5
    assertU(add(doc("price_s", "20.0",
        idField, "6",
        toField, "4")));

    assertU(commit());

    // Search for product
    assertJQ(req("q", "{!join from=" + idField + " to=" + toField + " score=None}name:name2", "fl", "id")
        , "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'5'},{'id':'6'}]}");
    
    /*Query joinQuery =
        JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name2")), indexSearcher, ScoreMode.None);

    TopDocs result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(4, result.scoreDocs[0].doc);
    assertEquals(5, result.scoreDocs[1].doc);
    */
    assertJQ(req("q", "{!join from=" + idField + " to=" + toField + " score=None}name:name1", "fl", "id")
        , "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'2'},{'id':'3'}]}");

    /*joinQuery = JoinUtil.createJoinQuery(idField, false, toField, new TermQuery(new Term("name", "name1")), indexSearcher, ScoreMode.None);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(1, result.scoreDocs[0].doc);
    assertEquals(2, result.scoreDocs[1].doc);*/

    // Search for offer
    assertJQ(req("q", "{!join from=" + toField + " to=" + idField + " score=None}id:5", "fl", "id")
        , "/response=={'numFound':1,'start':0,'numFoundExact':true,'docs':[{'id':'4'}]}");
    /*joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("id", "5")), indexSearcher, ScoreMode.None);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(1, result.totalHits);
    assertEquals(3, result.scoreDocs[0].doc);

    indexSearcher.getIndexReader().close();
    dir.close();*/
  }

  public void testDeleteByScoreJoinQuery() throws Exception {
    indexDataForScorring();
    String joinQuery = "{!join from=" + toField + " to=" + idField + " score=Max}title:random";
    assertJQ(req("q", joinQuery, "fl", "id"), "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'}]}");
    assertU(delQ(joinQuery));
    assertU(commit());
    assertJQ(req("q", joinQuery, "fl", "id"), "/response=={'numFound':0,'start':0,'numFoundExact':true,'docs':[]}");
  }

  public void testSimpleWithScoring() throws Exception {
    indexDataForScorring();

    // Search for movie via subtitle
    assertJQ(req("q", "{!join from=" + toField + " to=" + idField + " score=Max}title:random", "fl", "id")
        , "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'}]}");
    //dump(req("q","{!scorejoin from="+toField+" to="+idField+" score=Max}title:random", "fl","id,score", "debug", "true"));
    /*
    Query joinQuery =
        JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "random")), indexSearcher, ScoreMode.Max);
    TopDocs result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(0, result.scoreDocs[0].doc);
    assertEquals(3, result.scoreDocs[1].doc);*/


    // Score mode max.
    //dump(req("q","{!scorejoin from="+toField+" to="+idField+" score=Max}title:movie", "fl","id,score", "debug", "true"));

    // dump(req("q","title:movie", "fl","id,score", "debug", "true"));
    assertJQ(req("q", "{!join from=" + toField + " to=" + idField + " score=Max}title:movie", "fl", "id")
        , "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'4'},{'id':'1'}]}");
    
    /*joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "movie")), indexSearcher, ScoreMode.Max);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(3, result.scoreDocs[0].doc);
    assertEquals(0, result.scoreDocs[1].doc);*/

    // Score mode total
    assertJQ(req("q", "{!join from=" + toField + " to=" + idField + " score=Total}title:movie", "fl", "id")
        , "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'1'},{'id':'4'}]}");
  /*  joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "movie")), indexSearcher, ScoreMode.Total);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(0, result.scoreDocs[0].doc);
    assertEquals(3, result.scoreDocs[1].doc);
*/
    //Score mode avg
    assertJQ(req("q", "{!join from=" + toField + " to=" + idField + " score=Avg}title:movie", "fl", "id")
        , "/response=={'numFound':2,'start':0,'numFoundExact':true,'docs':[{'id':'4'},{'id':'1'}]}");
    
  /*  joinQuery = JoinUtil.createJoinQuery(toField, false, idField, new TermQuery(new Term("title", "movie")), indexSearcher, ScoreMode.Avg);
    result = indexSearcher.search(joinQuery, 10);
    assertEquals(2, result.totalHits);
    assertEquals(3, result.scoreDocs[0].doc);
    assertEquals(0, result.scoreDocs[1].doc);*/

  }

  final static Comparator<String> lessFloat = (o1, o2) -> {
    assertTrue(Float.parseFloat(o1) < Float.parseFloat(o2));
    return 0;
  };

  @Ignore("SOLR-7814, also don't forget cover boost at testCacheHit()")
  public void testBoost() throws Exception {
    indexDataForScorring();
    ScoreMode score = ScoreMode.values()[random().nextInt(ScoreMode.values().length)];

    final SolrQueryRequest req = req("q", "{!join from=movieId_s to=id score=" + score + " b=200}title:movie", "fl", "id,score", "omitHeader", "true");
    SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, new SolrQueryResponse()));
    final Query luceneQ = QParser.getParser(req.getParams().get("q"), req).getQuery().rewrite(req.getSearcher().getSlowAtomicReader());
    assertTrue(luceneQ instanceof BoostQuery);
    float boost = ((BoostQuery) luceneQ).getBoost();
    assertEquals("" + luceneQ, Float.floatToIntBits(200), Float.floatToIntBits(boost));
    SolrRequestInfo.clearRequestInfo();
    req.close();
  }

  public void testCacheHit() throws Exception {
    indexDataForScorring();

    Map<String, Metric> metrics = h.getCoreContainer().getMetricManager().registry(h.getCore().getCoreMetricManager().getRegistryName()).getMetrics();

    MetricsMap mm = (MetricsMap)((SolrMetricManager.GaugeWrapper)metrics.get("CACHE.searcher.queryResultCache")).getGauge();
    {
      Map<String,Object> statPre = mm.getValue();
      h.query(req("q", "{!join from=movieId_s to=id score=Avg}title:first", "fl", "id", "omitHeader", "true"));
      assertHitOrInsert(mm.getValue(), statPre);
    }

    {
      Map<String,Object> statPre = mm.getValue();
      h.query(req("q", "{!join from=movieId_s to=id score=Avg}title:first", "fl", "id", "omitHeader", "true"));
      assertHit(mm.getValue(), statPre);
    }

    {
      Map<String,Object> statPre = mm.getValue();

      Random r = random();
      boolean changed = false;
      boolean x = false;
      String from = (x = r.nextBoolean()) ? "id" : "movieId_s";
      changed |= x;
      String to = (x = r.nextBoolean()) ? "movieId_s" : "id";
      changed |= x;
      String score = (x = r.nextBoolean()) ? not(ScoreMode.Avg).name() : "Avg";
      changed |= x;
      /* till SOLR-7814
       * String boost = (x = r.nextBoolean()) ? "23" : "1";
      changed |= x; */
      String q = (!changed) ? (r.nextBoolean() ? "title:first^67" : "title:night") : "title:first";

      final String resp = h.query(req("q", "{!join from=" + from + " to=" + to +
              " score=" + score + 
              //" b=" + boost + 
              "}" + q, "fl", "id", "omitHeader", "true")
      );
      assertInsert(mm.getValue(), statPre);

      statPre = mm.getValue();
      final String repeat = h.query(req("q", "{!join from=" + from + " to=" + to + " score=" + score.toLowerCase(Locale.ROOT) +
          //" b=" + boost
              "}" + q, "fl", "id", "omitHeader", "true")
      );
      assertHit(mm.getValue(), statPre);

      assertEquals("lowercase shouldn't change anything", resp, repeat);

        final String aMod = score.substring(0, score.length() - 1);
        assertQEx("exception on "+aMod, "ScoreMode", 
            req("q", "{!join from=" + from + " to=" + to + " score=" + aMod +
                "}" + q, "fl", "id", "omitHeader", "true"), 
                SolrException.ErrorCode.BAD_REQUEST);
    }
    // this queries are not overlap, with other in this test case. 
    // however it might be better to extract this method into the separate suite
    // for a while let's nuke a cache content, in case of repetitions
    SolrCache cache = (SolrCache)h.getCore().getInfoRegistry().get("queryResultCache");
    cache.clear();
  }

  private ScoreMode not(ScoreMode s) {
    Random r = random();
    final List<ScoreMode> l = new ArrayList(Arrays.asList(ScoreMode.values()));
    l.remove(s);
    return l.get(r.nextInt(l.size()));
  }

  private void assertInsert(Map<String,Object> current, final Map<String,Object> statPre) {
    assertEquals("it lookups", 1,
        delta("lookups", current, statPre));
    assertEquals("it doesn't hit", 0, delta("hits", current, statPre));
    assertEquals("it inserts", 1,
        delta("inserts", current, statPre));
  }

  private void assertHit(Map<String,Object> current, final Map<String,Object> statPre) {
    assertEquals("it lookups", 1,
        delta("lookups", current, statPre));
    assertEquals("it hits", 1, delta("hits", current, statPre));
    assertEquals("it doesn't insert", 0,
        delta("inserts", current, statPre));
  }

  private void assertHitOrInsert(Map<String,Object> current, final Map<String,Object> statPre) {
    assertEquals("it lookups", 1,
        delta("lookups", current, statPre));
    final long mayHit = delta("hits", current, statPre);
    assertTrue("it may hit", 0 == mayHit || 1 == mayHit);
    assertEquals("or insert on cold", 1,
        delta("inserts", current, statPre) + mayHit);
  }

  private long delta(String key, Map<String,Object> a, Map<String,Object> b) {
    return (Long) a.get(key) - (Long) b.get(key);
  }

  private void indexDataForScorring() {
    clearIndex();
// 0
    assertU(add(doc("t_description", "A random movie",
        "name", "Movie 1",
        idField, "1")));
// 1

    assertU(add(doc("title", "The first subtitle of this movie",
        idField, "2",
        toField, "1")));


// 2

    assertU(add(doc("title", "random subtitle; random event movie",
        idField, "3",
        toField, "1")));

// 3

    assertU(add(doc("t_description", "A second random movie",
        "name", "Movie 2",
        idField, "4")));
// 4

    assertU(add(doc("title", "a very random event happened during christmas night",
        idField, "5",
        toField, "4")));


// 5

    assertU(add(doc("title", "movie end movie test 123 test 123 random",
        idField, "6",
        toField, "4")));


    assertU(commit());
  }
}
