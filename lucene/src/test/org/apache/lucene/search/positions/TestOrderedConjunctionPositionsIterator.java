package org.apache.lucene.search.positions;

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
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

public class TestOrderedConjunctionPositionsIterator extends LuceneTestCase {
  
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
    
    {
      Document doc = new Document();
      doc.add(newField(
          "field",
          "Pease porridge cold! Pease porridge hot! Pease porridge in the pot nine days old!",
          Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
  }
  public void testConjunctionPositionsBooleanQuery() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    addDocs(writer);
    
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    writer.close();
    BooleanQuery query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "hot!")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "pease")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "porridge")), Occur.MUST));
    {
      PositionFilterQuery filter = new PositionFilterQuery(query, new OrderedConjunctionPositionIteratorFilter());
      TopDocs search = searcher.search(filter, 10);
      ScoreDoc[] scoreDocs = search.scoreDocs;
      assertEquals(3, search.totalHits);
      assertEquals(2, scoreDocs[0].doc);
      assertEquals(0, scoreDocs[1].doc);
      assertEquals(1, scoreDocs[2].doc);
    }
    
    query = new BooleanQuery();
    query.add(new BooleanClause(new TermQuery(new Term("field", "old!")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "pease")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "porridge")), Occur.MUST));
    query.add(new BooleanClause(new TermQuery(new Term("field", "cold!")), Occur.MUST));

    {
      PositionFilterQuery filter = new PositionFilterQuery(query, new OrderedConjunctionPositionIteratorFilter());
      TopDocs search = searcher.search(filter, 10);
      ScoreDoc[] scoreDocs = search.scoreDocs;
      assertEquals(2, search.totalHits);
      assertEquals(0, scoreDocs[0].doc);
      assertEquals(1, scoreDocs[1].doc);
    }
    
    searcher.close();
    reader.close();
    directory.close();
  }
  
  public void testConjuctionPositionIterator() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    addDocs(writer);
    
    IndexReader reader = writer.getReader();
    writer.optimize();
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
        PositionIntervalIterator positions = new OrderedConjunctionPositionIterator(scorer.positions());
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
        PositionIntervalIterator positions = new OrderedConjunctionPositionIterator(scorer.positions());
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
  
  public static class OrderedConjunctionPositionIteratorFilter implements PositionIntervalFilter {

    @Override
    public PositionIntervalIterator filter(PositionIntervalIterator iter) {
      return new OrderedConjunctionPositionIterator(iter);
    }
    
  }
}
