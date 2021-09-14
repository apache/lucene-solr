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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;

import javax.xml.xpath.XPathConstants;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.BaseTestHarness;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class BJQParserTest extends SolrTestCaseJ4 {
  
  private static final String[] klm = new String[] {"k", "l", "m"};
  private static final List<String> xyz = Arrays.asList("x", "y", "z");
  private static final String[] abcdef = new String[] {"a", "b", "c", "d", "e", "f"};
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema15.xml");
    createIndex();
  }
  
  public static void createIndex() throws IOException, Exception {
    int i = 0;
    List<List<String[]>> blocks = createBlocks();
    for (List<String[]> block : blocks) {
      List<XmlDoc> updBlock = new ArrayList<>();
      
      for (String[] doc : block) {
        String[] idDoc = Arrays.copyOf(doc,doc.length+2);
        idDoc[doc.length]="id";
        idDoc[doc.length+1]=Integer.toString(i);
        updBlock.add(doc(idDoc));
        i++;
      }
      //got xmls for every doc. now nest all into the last one
      XmlDoc parentDoc = updBlock.get(updBlock.size()-1);
      parentDoc.xml = parentDoc.xml.replace("</doc>", 
          updBlock.subList(0, updBlock.size()-1).toString().replaceAll("[\\[\\]]","")+"</doc>");
      assertU(add(parentDoc));
      
      if (random().nextBoolean()) {
        assertU(commit());
        // force empty segment (actually, this will no longer create an empty segment, only a new segments_n)
        if (random().nextBoolean()) {
          assertU(commit());
        }
      }
    }
    assertU(commit());
    assertQ(req("q", "*:*"), "//*[@numFound='" + i + "']");
  }

  private static List<List<String[]>> createBlocks() {
    List<List<String[]>> blocks = new ArrayList<>();
    for (String parent : abcdef) {
      List<String[]> block = createChildrenBlock(parent);
      block.add(new String[] {"parent_s", parent});
      blocks.add(block);
    }
    Collections.shuffle(blocks, random());
    return blocks;
  }

  private static List<String[]> createChildrenBlock(String parent) {
    List<String[]> block = new ArrayList<>();
    for (String child : klm) {
      block
          .add(new String[] {"child_s", child, "parentchild_s", parent + child, "childparent_s", parent});
    }
    Collections.shuffle(block, random());
    addGrandChildren(block);
    return block;
  }
  
  private static void addGrandChildren(List<String[]> block) {
    List<String> grandChildren = new ArrayList<>(xyz);
    // add grandchildren after children
    for (ListIterator<String[]> iter = block.listIterator(); iter.hasNext();) {
      String[] child = iter.next();
      assert child[0]=="child_s" && child[2]=="parentchild_s": Arrays.toString(child);
      String child_s = child[1];
      String parentchild_s = child[3];
      int grandChildPos = 0;
      boolean lastLoopButStillHasGrCh = !iter.hasNext()
          && !grandChildren.isEmpty();
      while (!grandChildren.isEmpty()
          && ((grandChildPos = random().nextInt(grandChildren.size() * 2)) < grandChildren
              .size() || lastLoopButStillHasGrCh)) {
        grandChildPos = grandChildPos >= grandChildren.size() ? 0
            : grandChildPos;
        iter.add(new String[] {"grand_s", grandChildren.remove(grandChildPos),
            "grand_child_s", child_s, "grand_parentchild_s", parentchild_s});
      }
    }
    // and reverse after that
    Collections.reverse(block);
  }
  
  @Test
  public void testFull() throws IOException, Exception {
    String childb = "{!parent which=\"parent_s:[* TO *]\"}child_s:l";
    assertQ(req("q", childb), sixParents);
  }
  
  private static final String sixParents[] = new String[] {
      "//*[@numFound='6']", "//doc/arr[@name=\"parent_s\"]/str='a'",
      "//doc/arr[@name=\"parent_s\"]/str='b'",
      "//doc/arr[@name=\"parent_s\"]/str='c'",
      "//doc/arr[@name=\"parent_s\"]/str='d'",
      "//doc/arr[@name=\"parent_s\"]/str='e'",
      "//doc/arr[@name=\"parent_s\"]/str='f'"};
  
  @Test
  public void testJustParentsFilter() throws IOException {
    assertQ(req("q", "{!parent which=\"parent_s:[* TO *]\"}"), sixParents);
  }
  
  @Test
  public void testJustParentsFilterInChild() throws IOException {
    assertQ(req("q", "{!child of=\"parent_s:[* TO *]\"}",
          "fq", "childparent_s:"+abcdef[random().nextInt(abcdef.length)],
        "indent","on"), 
        "//*[@numFound='"+klm.length+"']", //for any parent we have all three children
        "//doc/arr[@name='child_s']/str='"+klm[0]+"'",
        "//doc/arr[@name='child_s']/str='"+klm[1]+"'",
        "//doc/arr[@name='child_s']/str='"+klm[2]+"'"
        );
    assert klm.length==3 : "change asserts pls "+klm;
  }
  
  private final static String beParents[] = new String[] {"//*[@numFound='2']",
      "//doc/arr[@name=\"parent_s\"]/str='b'",
      "//doc/arr[@name=\"parent_s\"]/str='e'"};
  
  @Test
  public void testIntersectBqBjq() {
    
    assertQ(
        req("q", "+parent_s:(e b) +_query_:\"{!parent which=$pq v=$chq}\"",
            "chq", "child_s:l", "pq", "parent_s:[* TO *]"), beParents);
    assertQ(
        req("fq", "{!parent which=$pq v=$chq}\"", "q", "parent_s:(e b)", "chq",
            "child_s:l", "pq", "parent_s:[* TO *]"), beParents);
    
    assertQ(
        req("q", "*:*", "fq", "{!parent which=$pq v=$chq}\"", "fq",
            "parent_s:(e b)", "chq", "child_s:l", "pq", "parent_s:[* TO *]"),
        beParents);
  }

  public void testScoreNoneScoringForParent() throws Exception {
    assertQ("score=none yields 0.0 score",
        req("q", "{!parent which=\"parent_s:[* TO *]\" "+(
            rarely()? "":(rarely()? "score=None":"score=none")
            )+"}child_s:l","fl","score"),
        "//*[@numFound='6']",
        "(//float[@name='score'])["+(random().nextInt(6)+1)+"]=0.0");
  }

  public void testWrongScoreExceptionForParent() throws Exception {
    final String aMode = ScoreMode.values()[random().nextInt(ScoreMode.values().length)].name();
    final String wrongMode = rarely()? "":(rarely()? " ":
      rarely()? aMode.substring(1):aMode.toUpperCase(Locale.ROOT));
    assertQEx("wrong score mode", 
        req("q", "{!parent which=\"parent_s:[* TO *]\" score="+wrongMode+"}child_s:l","fl","score")
        , SolrException.ErrorCode.BAD_REQUEST.code);
  }

  public void testScoresForParent() throws Exception{
    final ArrayList<ScoreMode> noNone = new ArrayList<>(Arrays.asList(ScoreMode.values()));
    noNone.remove(ScoreMode.None);
    final String notNoneMode = (noNone.get(random().nextInt(noNone.size()))).name();
    
    String leastScore = getLeastScore("child_s:l");
    assertTrue(leastScore+" > 0.0", Float.parseFloat(leastScore)>0.0);
    final String notNoneLower = usually() ? notNoneMode: notNoneMode.toLowerCase(Locale.ROOT);
    
    assertQ(req("q", "{!parent which=\"parent_s:[* TO *]\" score="+notNoneLower+"}child_s:l","fl","score"),
        "//*[@numFound='6']","(//float[@name='score'])["+(random().nextInt(6)+1)+"]>='"+leastScore+"'");
  }
  
  public void testScoresForChild() throws Exception{ 
    String leastScore = getLeastScore("parent_s:a");
      assertTrue(leastScore+" > 0.0", Float.parseFloat(leastScore)>0.0);
      assertQ(
          req("q", "{!child of=\"parent_s:[* TO *]\"}parent_s:a","fl","score"), 
          "//*[@numFound='6']","(//float[@name='score'])["+(random().nextInt(6)+1)+"]>='"+leastScore+"'");
  }
  
  private String getLeastScore(String query) throws Exception {
    final String resp = h.query(req("q",query, "sort","score asc", "fl","score"));
    return (String) BaseTestHarness.
        evaluateXPath(resp,"(//float[@name='score'])[1]/text()", 
            XPathConstants.STRING);
  }

  @Test
  public void testFq() {
    assertQ(
        req("q", "{!parent which=$pq v=$chq}", "fq", "parent_s:(e b)", "chq",
            "child_s:l", "pq", "parent_s:[* TO *]"// ,"debugQuery","on"
        ), beParents);
    
    boolean qfq = random().nextBoolean();
    assertQ(
        req(qfq ? "q" : "fq", "parent_s:(a e b)", (!qfq) ? "q" : "fq",
            "{!parent which=$pq v=$chq}", "chq", "parentchild_s:(bm ek cl)",
            "pq", "parent_s:[* TO *]"), beParents);
    
  }
  
  @Test
  public void testIntersectParentBqChildBq() throws IOException {
    
    assertQ(
        req("q", "+parent_s:(a e b) +_query_:\"{!parent which=$pq v=$chq}\"",
            "chq", "parentchild_s:(bm ek cl)", "pq", "parent_s:[* TO *]"),
        beParents);
  }
  
  @Test
  public void testGrandChildren() throws IOException {
    assertQ(
        req("q", "{!parent which=$parentfilter v=$children}", "children",
            "{!parent which=$childrenfilter v=$grandchildren}",
            "grandchildren", "grand_s:" + "x", "parentfilter",
            "parent_s:[* TO *]", "childrenfilter", "child_s:[* TO *]"),
        sixParents);
    // int loops = atLeast(1);
    String grandChildren = xyz.get(random().nextInt(xyz.size()));
    assertQ(
        req("q", "+parent_s:(a e b) +_query_:\"{!parent which=$pq v=$chq}\"",
            "chq", "{!parent which=$childfilter v=$grandchq}", "grandchq",
            "+grand_s:" + grandChildren + " +grand_parentchild_s:(b* e* c*)",
            "pq", "parent_s:[* TO *]", "childfilter", "child_s:[* TO *]"),
        beParents);
  }
  
  @Test
  public void testChildrenParser() {
    assertQ(
        req("q", "{!child of=\"parent_s:[* TO *]\"}parent_s:a", "fq",
            "NOT grand_s:[* TO *]"), "//*[@numFound='3']",
        "//doc/arr[@name=\"child_s\"]/str='k'",
        "//doc/arr[@name=\"child_s\"]/str='l'",
        "//doc/arr[@name=\"child_s\"]/str='m'");
    assertQ(
        req("q", "{!child of=\"parent_s:[* TO *]\"}parent_s:b", "fq",
            "-parentchild_s:bm", "fq", "-grand_s:*"), "//*[@numFound='2']",
        "//doc/arr[@name=\"child_s\"]/str='k'",
        "//doc/arr[@name=\"child_s\"]/str='l'");
  }

  @Test
  public void testCacheHit() throws IOException {

    MetricsMap parentFilterCache = (MetricsMap)((SolrMetricManager.GaugeWrapper<?>)h.getCore().getCoreMetricManager().getRegistry()
        .getMetrics().get("CACHE.searcher.perSegFilter")).getGauge();
    MetricsMap filterCache = (MetricsMap)((SolrMetricManager.GaugeWrapper<?>)h.getCore().getCoreMetricManager().getRegistry()
        .getMetrics().get("CACHE.searcher.filterCache")).getGauge();

    Map<String,Object> parentsBefore = parentFilterCache.getValue();

    Map<String,Object> filtersBefore = filterCache.getValue();

    // it should be weird enough to be uniq
    String parentFilter = "parent_s:([a TO c] [d TO f])";

    assertQ("search by parent filter",
        req("q", "{!parent which=\"" + parentFilter + "\"}"),
        "//*[@numFound='6']");

    assertQ("filter by parent filter",
        req("q", "*:*", "fq", "{!parent which=\"" + parentFilter + "\"}"),
        "//*[@numFound='6']");

    assertEquals("didn't hit fqCache yet ", 0L,
        delta("hits", filterCache.getValue(), filtersBefore));

    assertQ(
        "filter by join",
        req("q", "*:*", "fq", "{!parent which=\"" + parentFilter
            + "\"}child_s:l"), "//*[@numFound='6']");

    assertEquals("in cache mode every request lookups", 3,
        delta("lookups", parentFilterCache.getValue(), parentsBefore));
    assertEquals("last two lookups causes hits", 2,
        delta("hits", parentFilterCache.getValue(), parentsBefore));
    assertEquals("the first lookup gets insert", 1,
        delta("inserts", parentFilterCache.getValue(), parentsBefore));


    assertEquals("true join query was not in fqCache", 0L,
        delta("hits", filterCache.getValue(), filtersBefore));
    assertEquals("true join query is cached in fqCache", 1L,
        delta("inserts", filterCache.getValue(), filtersBefore));
  }
  
  private long delta(String key, Map<String,Object> a, Map<String,Object> b) {
    return (Long) a.get(key) - (Long) b.get(key);
  }

  
  @Test
  public void nullInit() {
    new BlockJoinParentQParserPlugin().init(null);
  }

  private final static String eParent[] = new String[]{"//*[@numFound='1']",
      "//doc/arr[@name=\"parent_s\"]/str='e'"};

  @Test
  public void testToParentFilters() {
    assertQ(
        req("fq", "{!parent filters=$child.fq which=$pq v=$chq}\"",
            "q", "parent_s:(e b)",
            "child.fq", "+childparent_s:e +child_s:l",
            "chq", "child_s:[* TO *]",
            "pq", "parent_s:[* TO *]"), eParent);

    assertQ(
        req("fq", "{!parent filters=$child.fq which=$pq v=$chq}\"",
            "q", "parent_s:(e b)",
            "child.fq", "childparent_s:e",
            "child.fq", "child_s:l",
            "chq", "child_s:[* TO *]",
            "pq", "parent_s:[* TO *]"), eParent);
  }

  @Test
  public void testToChildFilters() {
    assertQ(
        req("fq", "{!child of=$pq filters=$parent.fq  v=$pq}\"",
            "q", "child_s:(l m)",
            "parent.fq", "+parent_s:(d c)",
            "pq", "parent_s:[* TO *]"),
        "//*[@numFound='4']",
        "//doc/arr[@name=\"parentchild_s\"]/str='dl'",
        "//doc/arr[@name=\"parentchild_s\"]/str='dm'",
        "//doc/arr[@name=\"parentchild_s\"]/str='cl'",
        "//doc/arr[@name=\"parentchild_s\"]/str='cm'"
    );

    assertQ(
        req("fq", "{!child of=$pq filters=$parent.fq  v=$pq}\"",
            "q", "child_s:(l m)",
            "parent.fq", "+parent_s:(d c)",
            "parent.fq", "+parent_s:(c a)",
            "pq", "parent_s:[* TO *]"),
        "//*[@numFound='2']",
        "//doc/arr[@name=\"parentchild_s\"]/str='cl'",
        "//doc/arr[@name=\"parentchild_s\"]/str='cm'"
    );
  }

  private final static String elChild[] = new String[]{"//*[@numFound='1']",
      "//doc[" + 
          "arr[@name=\"child_s\"]/str='l' and arr[@name=\"childparent_s\"]/str='e']"};


  @Test
  public void testFilters() {
    assertQ(
        req("q", "{!filters param=$child.fq v=$gchq}",
            "child.fq", "childparent_s:e",
            "child.fq", "child_s:l",
            "gchq", "child_s:[* TO *]"), elChild);

    assertQ(
        req("q", "{!filters param=$child.fq excludeTags=firstTag v=$gchq}",
            "child.fq", "{!tag=zeroTag,firstTag}childparent_s:e",
            "child.fq", "{!tag=secondTag}child_s:l",
            "gchq", "child_s:[* TO *]"), "//*[@numFound='6']");

    assertQ(
        req("q", "{!filters param=$child.fq excludeTags=secondTag v=$gchq}",
            "child.fq", "{!tag=firstTag}childparent_s:e",
            "child.fq", "{!tag=secondTag}child_s:l",
            "gchq", "child_s:[* TO *]"), "//*[@numFound='3']");

    assertQ(req("q",
             random().nextBoolean() ? "{!filters param=$child.fq excludeTags=firstTag,secondTag v=$gchq}" :
               random().nextBoolean() ? "{!filters param=$thereAreNoLikeThese v=$gchq}" :
                 "{!filters v=$gchq}" ,
            "child.fq", "{!tag=firstTag}childparent_s:e",
            "child.fq", "{!tag=secondTag}child_s:l",
            "gchq", "child_s:[* TO *]"), "//*[@numFound='18']");
    
    assertQEx("expecting exception on weird param",
        req("q", "{!filters v=$gchq param=}\"" ,
            "gchq", "child_s:[* TO *]"
       ),ErrorCode.BAD_REQUEST);
    
    assertQ( // omit main query
        req("q", "{!filters param=$child.fq}",
            "child.fq", "{!tag=firstTag}childparent_s:(e f)",
            "child.fq", "{!tag=secondTag}child_s:l"), "//*[@numFound='2']");
    
    assertQ( // all excluded, matching all 
        req("q", "{!filters param=$child.fq excludeTags=firstTag,secondTag}",
            "child.fq", "{!tag=firstTag}childparent_s:(e f)",
            "child.fq", "{!tag=secondTag}child_s:l"), "//*[@numFound='42']");
    
    assertQ(req("q", // excluding top level
            "{!filters param=$child.fq excludeTags=bot,top v=$gchq}" ,
       "child.fq", "{!tag=secondTag}child_s:l", // 6 ls remains
       "gchq", "{!tag=top}childparent_s:e"), "//*[@numFound='6']");
    
    assertQ(req("q", // top and filter are excluded, got all results
        "{!filters excludeTags=bot,secondTag,top v=$gchq}" ,
         "child.fq", "{!tag=secondTag}child_s:l",
         "gchq", "{!tag=top}childparent_s:e"), "//*[@numFound='42']");
  }
  
  @Test
  public void testFiltersCache() throws SyntaxError, IOException {
    final String [] elFilterQuery = new String[] {"q", "{!filters param=$child.fq v=$gchq}",
        "child.fq", "childparent_s:e",
        "child.fq", "child_s:l",
        "gchq", "child_s:[* TO *]"};
    assertQ("precondition: single doc match", 
         req(elFilterQuery), elChild);
    final Query query;
    try(final SolrQueryRequest req = req(elFilterQuery)) {
      QParser parser = QParser.getParser(req.getParams().get("q"), null, req);
      query = parser.getQuery();
      final TopDocs topDocs = req.getSearcher().search(query, 10);
      assertEquals(1, topDocs.totalHits.value);
    }
    assertU(adoc("id", "12275", 
        "child_s", "l", "childparent_s", "e"));
    assertU(commit());

    assertQ("here we rely on autowarming for cathing cache leak",  //cache=false
          req(elFilterQuery), "//*[@numFound='2']");

    try(final SolrQueryRequest req = req()) {
        final int count = req.getSearcher().count(query);
        assertEquals("expecting new doc is visible to old query", 2, count);
    }
  }

  @After
  public void cleanAfterTestFiltersCache(){
    assertU("should be noop", delI("12275"));
    assertU("most of the time", commit());
  }
}

