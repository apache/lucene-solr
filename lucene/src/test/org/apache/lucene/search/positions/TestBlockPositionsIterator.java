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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
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
import org.apache.lucene.search.Weight.ScorerContext;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionIntervalFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ReaderUtil;

public class TestBlockPositionsIterator extends LuceneTestCase {
  
  private static final void addDocs(RandomIndexWriter writer) throws CorruptIndexException, IOException {
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
              + " like it cold, Some like it in the pot nine days old! Pease porridge hot! Pease porridge cold!",
          Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "Pease porridge cold! Pease porridge hot! Pease porridge in the pot nine days old! Some like it cold, some"
              + " like it hot, Some like it in the pot nine days old! Pease porridge cold! Pease porridge hot!",
          Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
  }
  public void testExactPhraseBooleanConjunction() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    addDocs(writer);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    writer.close();
    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "pease")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "porridge")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "hot!")), Occur.MUST));
    {
      PositionFilterQuery filter = new PositionFilterQuery(query, new BlockPositionIteratorFilter());
      TopDocs search = searcher.search(filter, 10);
      ScoreDoc[] scoreDocs = search.scoreDocs;
      assertEquals(2, search.totalHits);
      assertEquals(0, scoreDocs[0].doc);
      assertEquals(1, scoreDocs[1].doc);
    }
    query.add(new BooleanClause(new TermQuery(new Term("field", "pease")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "porridge")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "cold!")), Occur.MUST));
    
    {
      PositionFilterQuery filter = new PositionFilterQuery(query, new BlockPositionIteratorFilter());
      TopDocs search = searcher.search(filter, 10);
      ScoreDoc[] scoreDocs = search.scoreDocs;
      assertEquals(1, search.totalHits);
      assertEquals(0, scoreDocs[0].doc);
    }
    
    query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "pease")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "hot!")), Occur.MUST));
    {
      PositionFilterQuery filter = new PositionFilterQuery(query, new BlockPositionIteratorFilter());
      TopDocs search = searcher.search(filter, 10);
      assertEquals(0, search.totalHits);
    }
    searcher.close();
    reader.close();
    directory.close();
  }
  
  public void testBlockPositionIterator() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    addDocs(writer);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    writer.close();
    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "pease")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "porridge")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "hot!")), Occur.MUST));
    
    Weight weight = query.createWeight(searcher);
    ReaderContext topReaderContext = searcher.getTopReaderContext();
    AtomicReaderContext[] leaves = ReaderUtil.leaves(topReaderContext);
    assertEquals(1, leaves.length);
    for (int i = 0; i < leaves.length; i++) {
      Scorer scorer = weight.scorer(leaves[0], ScorerContext.def().topScorer(true).needsPositions(true));
      {
        int nextDoc = scorer.nextDoc();
        assertEquals(0, nextDoc);
        PositionIntervalIterator positions = new BlockPositionIterator(scorer.positions());
        PositionInterval interval = null;
        int[] start = new int[] {0, 31};
        int[] end = new int[] {2, 33};
        // {start}term{end} - end is pos+1 
        // {0}Pease porridge hot!{0} Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
        // like it cold, Some like it in the pot nine days old! {1}Pease porridge hot!{1} Pease porridge cold!",
        for (int j = 0; j < end.length; j++) {
          interval = positions.next();
          assertNotNull(interval);
          assertEquals(start[j], interval.begin);
          assertEquals(end[j], interval.end);
        }
        assertNull(positions.next());
      }
      {
        int nextDoc = scorer.nextDoc();
        assertEquals(1, nextDoc);
        PositionIntervalIterator positions =  new BlockPositionIterator(scorer.positions());
        PositionInterval interval = null;
        int[] start = new int[] {3, 34};
        int[] end = new int[] {5, 36};
        // {start}term{end} - end is pos+1
        // Pease porridge cold! {0}Pease porridge hot!{0} Pease porridge in the pot nine days old! Some like it cold, some
        // like it hot, Some like it in the pot nine days old! Pease porridge cold! {1}Pease porridge hot{1}!
        for (int j = 0; j < end.length; j++) {
          interval = positions.next();
          assertNotNull(interval);
          assertEquals(j + "", start[j], interval.begin);
          assertEquals(j+ "", end[j], interval.end);
        }
        assertNull(positions.next());
      }
    }
    

    searcher.close();
    reader.close();
    directory.close();
  }
  
  
  public static class BlockPositionIteratorFilter implements PositionIntervalFilter {

    @Override
    public PositionIntervalIterator filter(PositionIntervalIterator iter) {
      return new BlockPositionIterator(iter);
    }
    
  }
}
