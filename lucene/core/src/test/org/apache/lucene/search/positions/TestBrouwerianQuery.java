package org.apache.lucene.search.positions;
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
import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestBrouwerianQuery extends LuceneTestCase {
  
  private static final void addDocs(RandomIndexWriter writer) throws CorruptIndexException, IOException {
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "The quick brown fox jumps over the lazy dog",
              TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "The quick brown duck jumps over the lazy dog with the quick brown fox",
              TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
  }
  
  public void testBrouwerianBooleanQuery() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    addDocs(writer);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    writer.close();
    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "the")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "quick")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "jumps")), Occur.MUST));
    BooleanQuery sub = new BooleanQuery();
    sub.add(new BooleanClause(new TermQuery(new Term("field", "fox")), Occur.MUST));
    BrouwerianQuery q = new BrouwerianQuery(query, sub);
    TopDocs search = searcher.search(q, 10);
    ScoreDoc[] scoreDocs = search.scoreDocs;
    assertEquals(1, search.totalHits);
    assertEquals(1, scoreDocs[0].doc);
    
    reader.close();
    directory.close();
  }
  
  public void testBrouwerianBooleanQueryExcludedDoesNotExist() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    addDocs(writer);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    writer.close();
    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "the")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "quick")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "jumps")), Occur.MUST));
    BooleanQuery sub = new BooleanQuery();
    sub.add(new BooleanClause(new TermQuery(new Term("field", "blox")), Occur.MUST));
    BrouwerianQuery q = new BrouwerianQuery(query, sub);
    TopDocs search = searcher.search(q, 10);
    ScoreDoc[] scoreDocs = search.scoreDocs;
    assertEquals(2, search.totalHits);
    assertEquals(0, scoreDocs[0].doc);
    assertEquals(1, scoreDocs[1].doc);

    
    reader.close();
    directory.close();
  }
  
  public void testPositions() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    addDocs(writer);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    writer.close();
    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "the")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "quick")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "jumps")), Occur.MUST));
    BooleanQuery sub = new BooleanQuery();
    sub.add(new BooleanClause(new TermQuery(new Term("field", "fox")), Occur.MUST));
    BrouwerianQuery q = new BrouwerianQuery(query, sub);
    Weight weight = q.createWeight(searcher);
    IndexReaderContext topReaderContext = searcher.getTopReaderContext();
    List<AtomicReaderContext> leaves = topReaderContext.leaves();
    assertEquals(1, leaves.size());
    for (AtomicReaderContext atomicReaderContext : leaves) {
      Scorer scorer = weight.scorer(atomicReaderContext, true, true, atomicReaderContext.reader().getLiveDocs());
        int nextDoc = scorer.nextDoc();
        assertEquals(1, nextDoc);
        PositionIntervalIterator positions = scorer.positions(false, false, false);
        assertEquals(1, positions.advanceTo(nextDoc));
        PositionInterval interval = null;
        int[] start = new int[] {0, 1, 4};
        int[] end = new int[] {4, 6, 11};
        for (int j = 0; j < end.length; j++) {
          interval = positions.next();
          assertNotNull("" + j, interval);
          assertEquals(start[j], interval.begin);
          assertEquals(end[j], interval.end);
        }
        assertNull(positions.next());
        assertEquals(Scorer.NO_MORE_DOCS, scorer.nextDoc());
    reader.close();
    directory.close();
  }
  
  }
}
