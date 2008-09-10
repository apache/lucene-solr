package org.apache.lucene.search;

/**
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

import java.util.ConcurrentModificationException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 * Test Hits searches with interleaved deletions.
 * 
 * See {@link http://issues.apache.org/jira/browse/LUCENE-1096}.
 * @deprecated Hits will be removed in Lucene 3.0
 */
public class TestSearchHitsWithDeletions extends TestCase {

  private static boolean VERBOSE = false;  
  private static final String TEXT_FIELD = "text";
  private static final int N = 16100;

  private static Directory directory;

  public void setUp() throws Exception {
    // Create an index writer.
    directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    for (int i=0; i<N; i++) {
      writer.addDocument(createDocument(i));
    }
    writer.optimize();
    writer.close();
  }

  /**
   * Deletions during search should not alter previously retrieved hits.
   */
  public void testSearchHitsDeleteAll() throws Exception {
    doTestSearchHitsDeleteEvery(1,false);
  }
  
  /**
   * Deletions during search should not alter previously retrieved hits.
   */
  public void testSearchHitsDeleteEvery2ndHit() throws Exception {
    doTestSearchHitsDeleteEvery(2,false);
  }

  /**
   * Deletions during search should not alter previously retrieved hits.
   */
  public void testSearchHitsDeleteEvery4thHit() throws Exception {
    doTestSearchHitsDeleteEvery(4,false);
  }

  /**
   * Deletions during search should not alter previously retrieved hits.
   */
  public void testSearchHitsDeleteEvery8thHit() throws Exception {
    doTestSearchHitsDeleteEvery(8,false);
  }

  /**
   * Deletions during search should not alter previously retrieved hits.
   */
  public void testSearchHitsDeleteEvery90thHit() throws Exception {
    doTestSearchHitsDeleteEvery(90,false);
  }

  /**
   * Deletions during search should not alter previously retrieved hits,
   * and deletions that affect total number of hits should throw the 
   * correct exception when trying to fetch "too many".
   */
  public void testSearchHitsDeleteEvery8thHitAndInAdvance() throws Exception {
    doTestSearchHitsDeleteEvery(8,true);
  }

  /**
   * Verify that ok also with no deletions at all.
   */
  public void testSearchHitsNoDeletes() throws Exception {
    doTestSearchHitsDeleteEvery(N+100,false);
  }

  /**
   * Deletions that affect total number of hits should throw the 
   * correct exception when trying to fetch "too many".
   */
  public void testSearchHitsDeleteInAdvance() throws Exception {
    doTestSearchHitsDeleteEvery(N+100,true);
  }

  /**
   * Intermittent deletions during search, should not alter previously retrieved hits.
   * (Using a debugger to verify that the check in Hits is performed only  
   */
  public void testSearchHitsDeleteIntermittent() throws Exception {
    doTestSearchHitsDeleteEvery(-1,false);
  }

  
  private void doTestSearchHitsDeleteEvery(int k, boolean deleteInFront) throws Exception {
    boolean intermittent = k<0;
    log("Test search hits with "+(intermittent ? "intermittent deletions." : "deletions of every "+k+" hit."));
    IndexSearcher searcher = new IndexSearcher(directory);
    IndexReader reader = searcher.getIndexReader();
    Query q = new TermQuery(new Term(TEXT_FIELD,"text")); // matching all docs
    Hits hits = searcher.search(q);
    log("Got "+hits.length()+" results");
    assertEquals("must match all "+N+" docs, not only "+hits.length()+" docs!",N,hits.length());
    if (deleteInFront) {
      log("deleting hits that was not yet retrieved!");
      reader.deleteDocument(reader.maxDoc()-1);
      reader.deleteDocument(reader.maxDoc()-2);
      reader.deleteDocument(reader.maxDoc()-3);
    }
    try {
      for (int i = 0; i < hits.length(); i++) {
        int id = hits.id(i);
        assertEquals("Hit "+i+" has doc id "+hits.id(i)+" instead of "+i,i,hits.id(i));
        if ((intermittent && (i==50 || i==250 || i==950)) || //100-yes, 200-no, 400-yes, 800-no, 1600-yes 
            (!intermittent && (k<2 || (i>0 && i%k==0)))) {
          Document doc = hits.doc(id);
          log("Deleting hit "+i+" - doc "+doc+" with id "+id);
          reader.deleteDocument(id);
        }
        if (intermittent) {
          // check internal behavior of Hits (go 50 ahead of getMoreDocs points because the deletions cause to use more of the available hits)
          if (i==150 || i==450 || i==1650) {
            assertTrue("Hit "+i+": hits should have checked for deletions in last call to getMoreDocs()",hits.debugCheckedForDeletions);
          } else if (i==50 || i==250 || i==850) {
            assertFalse("Hit "+i+": hits should have NOT checked for deletions in last call to getMoreDocs()",hits.debugCheckedForDeletions);
          }
        }
      }
    } catch (ConcurrentModificationException e) {
      // this is the only valid exception, and only when deletng in front.
      assertTrue(e.getMessage()+" not expected unless deleting hits that were not yet seen!",deleteInFront);
    }
    searcher.close();
  }

  private static Document createDocument(int id) {
    Document doc = new Document();
    doc.add(new Field(TEXT_FIELD, "text of document"+id, Field.Store.YES, Field.Index.ANALYZED));
    return doc;
  }

  private static void log (String s) {
    if (VERBOSE) {
      System.out.println(s);
    }
  }
}
