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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.SolrCache;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

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
      List<XmlDoc> updBlock = new ArrayList<XmlDoc>();
      
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
    /*
     * dump docs well System.out.println(h.query(req("q","*:*",
     * "sort","_docid_ asc", "fl",
     * "parent_s,child_s,parentchild_s,grand_s,grand_child_s,grand_parentchild_s"
     * , "wt","csv", "rows","1000"))); /
     */
  }

  private static int id=0;
  private static List<List<String[]>> createBlocks() {
    List<List<String[]>> blocks = new ArrayList<List<String[]>>();
    for (String parent : abcdef) {
      List<String[]> block = createChildrenBlock(parent);
      block.add(new String[] {"parent_s", parent});
      blocks.add(block);
    }
    Collections.shuffle(blocks, random());
    return blocks;
  }

  private static List<String[]> createChildrenBlock(String parent) {
    List<String[]> block = new ArrayList<String[]>();
    for (String child : klm) {
      block
          .add(new String[] {"child_s", child, "parentchild_s", parent + child});
    }
    Collections.shuffle(block, random());
    addGrandChildren(block);
    return block;
  }
  
  private static void addGrandChildren(List<String[]> block) {
    List<String> grandChildren = new ArrayList<String>(xyz);
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

    SolrCache parentFilterCache = (SolrCache) h.getCore().getInfoRegistry()
        .get("perSegFilter");

    SolrCache filterCache = (SolrCache) h.getCore().getInfoRegistry()
        .get("filterCache");

    NamedList parentsBefore = parentFilterCache.getStatistics();

    NamedList filtersBefore = filterCache.getStatistics();

    // it should be weird enough to be uniq
    String parentFilter = "parent_s:([a TO c] [d TO f])";

    assertQ("search by parent filter",
        req("q", "{!parent which=\"" + parentFilter + "\"}"),
        "//*[@numFound='6']");

    assertQ("filter by parent filter",
        req("q", "*:*", "fq", "{!parent which=\"" + parentFilter + "\"}"),
        "//*[@numFound='6']");

    assertEquals("didn't hit fqCache yet ", 0L,
        delta("hits", filterCache.getStatistics(), filtersBefore));

    assertQ(
        "filter by join",
        req("q", "*:*", "fq", "{!parent which=\"" + parentFilter
            + "\"}child_s:l"), "//*[@numFound='6']");

    assertEquals("in cache mode every request lookups", 3,
        delta("lookups", parentFilterCache.getStatistics(), parentsBefore));
    assertEquals("last two lookups causes hits", 2,
        delta("hits", parentFilterCache.getStatistics(), parentsBefore));
    assertEquals("the first lookup gets insert", 1,
        delta("inserts", parentFilterCache.getStatistics(), parentsBefore));


    assertEquals("true join query is cached in fqCache", 1L,
        delta("lookups", filterCache.getStatistics(), filtersBefore));
  }
  
  private long delta(String key, NamedList a, NamedList b) {
    return (Long) a.get(key) - (Long) b.get(key);
  }

  
  @Test
  public void nullInit() {
    new BlockJoinParentQParserPlugin().init(null);
  }
  
}

