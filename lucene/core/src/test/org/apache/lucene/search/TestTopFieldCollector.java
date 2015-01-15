package org.apache.lucene.search;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestTopFieldCollector extends LuceneTestCase {
  private IndexSearcher is;
  private IndexReader ir;
  private Directory dir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      iw.addDocument(doc);
    }
    ir = iw.getReader();
    iw.close();
    is = newSearcher(ir);
  }
  
  @Override
  public void tearDown() throws Exception {
    ir.close();
    dir.close();
    super.tearDown();
  }
  
  public void testSortWithoutFillFields() throws Exception {
    
    // There was previously a bug in TopFieldCollector when fillFields was set
    // to false - the same doc and score was set in ScoreDoc[] array. This test
    // asserts that if fillFields is false, the documents are set properly. It
    // does not use Searcher's default search methods (with Sort) since all set
    // fillFields to true.
    Sort[] sort = new Sort[] { new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, false,
          false, false);
      
      is.search(q, tdc);
      
      ScoreDoc[] sd = tdc.topDocs().scoreDocs;
      for(int j = 1; j < sd.length; j++) {
        assertTrue(sd[j].doc != sd[j - 1].doc);
      }
      
    }
  }

  public void testSortWithoutScoreTracking() throws Exception {

    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true, false,
          false);
      
      is.search(q, tdc);
      
      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for(int j = 0; j < sd.length; j++) {
        assertTrue(Float.isNaN(sd[j].score));
      }
      assertTrue(Float.isNaN(td.getMaxScore()));
    }
  }
  
  public void testSortWithScoreNoMaxScoreTracking() throws Exception {
    
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true, true,
          false);
      
      is.search(q, tdc);
      
      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for(int j = 0; j < sd.length; j++) {
        assertTrue(!Float.isNaN(sd[j].score));
      }
      assertTrue(Float.isNaN(td.getMaxScore()));
    }
  }
  
  // MultiComparatorScoringNoMaxScoreCollector
  public void testSortWithScoreNoMaxScoreTrackingMulti() throws Exception {
    
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC, SortField.FIELD_SCORE) };
    for(int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true, true,
          false);

      is.search(q, tdc);
      
      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for(int j = 0; j < sd.length; j++) {
        assertTrue(!Float.isNaN(sd[j].score));
      }
      assertTrue(Float.isNaN(td.getMaxScore()));
    }
  }
  
  public void testSortWithScoreAndMaxScoreTracking() throws Exception {
    
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      Query q = new MatchAllDocsQuery();
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true, true,
          true);
      
      is.search(q, tdc);
      
      TopDocs td = tdc.topDocs();
      ScoreDoc[] sd = td.scoreDocs;
      for(int j = 0; j < sd.length; j++) {
        assertTrue(!Float.isNaN(sd[j].score));
      }
      assertTrue(!Float.isNaN(td.getMaxScore()));
    }
  }

  public void testSortWithScoreAndMaxScoreTrackingNoResults() throws Exception {
    
    // Two Sort criteria to instantiate the multi/single comparators.
    Sort[] sort = new Sort[] {new Sort(SortField.FIELD_DOC), new Sort() };
    for(int i = 0; i < sort.length; i++) {
      TopDocsCollector<Entry> tdc = TopFieldCollector.create(sort[i], 10, true, true, true);
      TopDocs td = tdc.topDocs();
      assertEquals(0, td.totalHits);
      assertTrue(Float.isNaN(td.getMaxScore()));
    }
  }  
}
