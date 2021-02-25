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
package org.apache.solr.handler.admin;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.Version;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.index.NoMergePolicyFactory;
import org.apache.solr.util.RefCounted;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for SegmentsInfoRequestHandler. Plugin entry, returning data of created segment.
 */
public class SegmentsInfoRequestHandlerTest extends SolrTestCaseJ4 {
  private static final int DOC_COUNT = 5;
  
  private static final int DEL_COUNT = 1;
  
  private static final int NUM_SEGMENTS = 2;

  private static int initialRefCount;
  
  @BeforeClass
  public static void beforeClass() throws Exception {

    // we need a consistent segmentation to ensure we don't get a random
    // merge that reduces the total num docs in all segments, or the number of deletes
    //
    systemSetPropertySolrTestsMergePolicyFactory(NoMergePolicyFactory.class.getName());
    // Also prevent flushes
    System.setProperty("solr.tests.maxBufferedDocs", "1000");
    System.setProperty("solr.tests.ramBufferSizeMB", "5000");
    
    System.setProperty("enable.update.log", "false"); // no _version_ in our schema
    initCore("solrconfig.xml", "schema12.xml"); // segments API shouldn't depend on _version_ or ulog
    
    // build up an index with at least 2 segments and some deletes
    for (int i = 0; i < DOC_COUNT; i++) {
      assertU(adoc("id","SOLR100" + i, "name","Apache Solr:" + i));
    }
    for (int i = 0; i < DEL_COUNT; i++) {
      assertU(delI("SOLR100" + i));
    }
    assertU(commit());
    for (int i = 0; i < DOC_COUNT; i++) {
      assertU(adoc("id","SOLR200" + i, "name","Apache Solr:" + i));
    }
    assertU(commit());
    h.getCore().withSearcher((searcher) -> {
      int numSegments = SegmentInfos.readLatestCommit(searcher.getIndexReader().directory()).size();
      // if this is not NUM_SEGMENTS, there was some unexpected flush or merge
      assertEquals("Unexpected number of segment in the index: " + numSegments, 
          NUM_SEGMENTS, numSegments);
      return null;
    });
    // see SOLR-14431
    RefCounted<IndexWriter> iwRef = h.getCore().getSolrCoreState().getIndexWriter(h.getCore());
    initialRefCount = iwRef.getRefcount();
    iwRef.decref();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    RefCounted<IndexWriter> iwRef = h.getCore().getSolrCoreState().getIndexWriter(h.getCore());
    int finalRefCount = iwRef.getRefcount();
    iwRef.decref();
    assertEquals("IW refcount mismatch", initialRefCount, finalRefCount);
    systemClearPropertySolrTestsMergePolicyFactory();
    System.clearProperty("solr.tests.maxBufferedDocs");
    System.clearProperty("solr.tests.ramBufferSizeMB");
  }

  @Test
  public void testSegmentInfos() {   
    assertQ("Unexpected number of segments returned",
        req("qt","/admin/segments"),
        NUM_SEGMENTS + "=count(//lst[@name='segments']/lst)");
  }

  @Test
  public void testSegmentInfosVersion() {
    assertQ("Unexpected number of segments returned",
        req("qt","/admin/segments"),
        NUM_SEGMENTS + "=count(//lst[@name='segments']/lst/str[@name='version'][.='" + Version.LATEST + "'])");
  }
  
  @Test
  public void testSegmentNames() throws IOException {
    String[] segmentNamePatterns = new String[NUM_SEGMENTS];
    h.getCore().withSearcher((searcher) -> {
      int i = 0;
      for (SegmentCommitInfo sInfo : SegmentInfos.readLatestCommit(searcher.getIndexReader().directory())) {
        assertTrue("Unexpected number of segment in the index: " + i, i < NUM_SEGMENTS);
        segmentNamePatterns[i] = "//lst[@name='segments']/lst/str[@name='name'][.='" + sInfo.info.name + "']";
        i++;
      }
      
      return null;
    });
    assertQ("Unexpected segment names returned",
        req("qt","/admin/segments"),
        segmentNamePatterns);
  }
  
  @Test
  public void testSegmentInfosData() {
    assertQ("Unexpected document counts in result",
        req("qt","/admin/segments"),
          //#Document
          (DOC_COUNT*2)+"=sum(//lst[@name='segments']/lst/int[@name='size'])",
          //#Deletes
          DEL_COUNT+"=sum(//lst[@name='segments']/lst/int[@name='delCount'])");
  }

  @Test
  public void testCoreInfo() {
    assertQ("Missing core info",
        req("qt", "/admin/segments", "coreInfo", "true"),
        "boolean(//lst[@name='info']/lst[@name='core'])");
  }

  @Test
  public void testFieldInfo() throws Exception {
    String[] segmentNamePatterns = new String[NUM_SEGMENTS];
    h.getCore().withSearcher((searcher) -> {
      int i = 0;
      for (SegmentCommitInfo sInfo : SegmentInfos.readLatestCommit(searcher.getIndexReader().directory())) {
        assertTrue("Unexpected number of segment in the index: " + i, i < NUM_SEGMENTS);
        segmentNamePatterns[i] = "boolean(//lst[@name='segments']/lst[@name='" + sInfo.info.name + "']/lst[@name='fields']/lst[@name='id']/str[@name='flags'])";
        i++;
      }

      return null;
    });
    assertQ("Unexpected field infos returned",
        req("qt","/admin/segments", "fieldInfo", "true"),
        segmentNamePatterns);
  }


}
