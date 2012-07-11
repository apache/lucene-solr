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
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestTopScoreDocCollector extends LuceneTestCase {

  public void testOutOfOrderCollection() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 10; i++) {
      writer.addDocument(new Document());
    }
    
    boolean[] inOrder = new boolean[] { false, true };
    String[] actualTSDCClass = new String[] {
        "OutOfOrderTopScoreDocCollector", 
        "InOrderTopScoreDocCollector" 
    };
    
    BooleanQuery bq = new BooleanQuery();
    // Add a Query with SHOULD, since bw.scorer() returns BooleanScorer2
    // which delegates to BS if there are no mandatory clauses.
    bq.add(new MatchAllDocsQuery(), Occur.SHOULD);
    // Set minNrShouldMatch to 1 so that BQ will not optimize rewrite to return
    // the clause instead of BQ.
    bq.setMinimumNumberShouldMatch(1);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    for (int i = 0; i < inOrder.length; i++) {
      TopDocsCollector<ScoreDoc> tdc = TopScoreDocCollector.create(3, inOrder[i]);
      assertEquals("org.apache.lucene.search.TopScoreDocCollector$" + actualTSDCClass[i], tdc.getClass().getName());
      
      searcher.search(new MatchAllDocsQuery(), tdc);
      
      ScoreDoc[] sd = tdc.topDocs().scoreDocs;
      assertEquals(3, sd.length);
      for (int j = 0; j < sd.length; j++) {
        assertEquals("expected doc Id " + j + " found " + sd[j].doc, j, sd[j].doc);
      }
    }
    writer.close();
    reader.close();
    dir.close();
  }
  
}
