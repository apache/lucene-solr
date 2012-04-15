package org.apache.lucene.search.spans;

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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests for {@link SpanMultiTermQueryWrapper}, wrapping a few MultiTermQueries.
 */
public class TestSpanMultiTermQueryWrapper extends LuceneTestCase {
  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
    Document doc = new Document();
    Field field = newField("field", "", TextField.TYPE_UNSTORED);
    doc.add(field);
    
    field.setStringValue("quick brown fox");
    iw.addDocument(doc);
    field.setStringValue("jumps over lazy broun dog");
    iw.addDocument(doc);
    field.setStringValue("jumps over extremely very lazy broxn dog");
    iw.addDocument(doc);
    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  public void testWildcard() throws Exception {
    WildcardQuery wq = new WildcardQuery(new Term("field", "bro?n"));
    SpanQuery swq = new SpanMultiTermQueryWrapper<WildcardQuery>(wq);
    // will only match quick brown fox
    SpanFirstQuery sfq = new SpanFirstQuery(swq, 2);
    assertEquals(1, searcher.search(sfq, 10).totalHits);
  }
  
  public void testPrefix() throws Exception {
    WildcardQuery wq = new WildcardQuery(new Term("field", "extrem*"));
    SpanQuery swq = new SpanMultiTermQueryWrapper<WildcardQuery>(wq);
    // will only match "jumps over extremely very lazy broxn dog"
    SpanFirstQuery sfq = new SpanFirstQuery(swq, 3);
    assertEquals(1, searcher.search(sfq, 10).totalHits);
  }
  
  public void testFuzzy() throws Exception {
    FuzzyQuery fq = new FuzzyQuery(new Term("field", "broan"));
    SpanQuery sfq = new SpanMultiTermQueryWrapper<FuzzyQuery>(fq);
    // will not match quick brown fox
    SpanPositionRangeQuery sprq = new SpanPositionRangeQuery(sfq, 3, 6);
    assertEquals(2, searcher.search(sprq, 10).totalHits);
  }
  
  public void testFuzzy2() throws Exception {
    // maximum of 1 term expansion
    FuzzyQuery fq = new FuzzyQuery(new Term("field", "broan"), 1f, 0, 1);
    SpanQuery sfq = new SpanMultiTermQueryWrapper<FuzzyQuery>(fq);
    // will only match jumps over lazy broun dog
    SpanPositionRangeQuery sprq = new SpanPositionRangeQuery(sfq, 0, 100);
    assertEquals(1, searcher.search(sprq, 10).totalHits);
  }
}
