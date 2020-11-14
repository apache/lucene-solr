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
package org.apache.lucene.index;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.ExitableDirectoryReader.ExitingReaderException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Test that uses a default/lucene Implementation of {@link QueryTimeout}
 * to exit out long running queries that take too long to iterate over Terms.
 */
public class TestExitableDirectoryReader extends LuceneTestCase {
  private static class TestReader extends FilterLeafReader {

    private static class TestTerms extends FilterTerms {
      TestTerms(Terms in) {
        super(in);
      }

      @Override
      public TermsEnum iterator() throws IOException {
        return new TestTermsEnum(super.iterator());
      }
    }

    private static class TestTermsEnum extends FilterTermsEnum {
      public TestTermsEnum(TermsEnum in) {
        super(in);
      }

      /**
       * Sleep between iterations to timeout things.
       */
      @Override
      public BytesRef next() throws IOException {
        try {
          // Sleep for 100ms before each .next() call.
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
        return in.next();
      }
    }

    public TestReader(LeafReader reader) throws IOException {
      super(reader);
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = super.terms(field);
      return terms==null ? null : new TestTerms(terms);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return in.getReaderCacheHelper();
    }
  }

  /**
   * Tests timing out of TermsEnum iterations
   * @throws Exception on error
   */
  public void testExitableFilterTermsIndexReader() throws Exception {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

    Document d1 = new Document();
    d1.add(newTextField("default", "one two", Field.Store.YES));
    writer.addDocument(d1);

    Document d2 = new Document();
    d2.add(newTextField("default", "one three", Field.Store.YES));
    writer.addDocument(d2);

    Document d3 = new Document();
    d3.add(newTextField("default", "ones two four", Field.Store.YES));
    writer.addDocument(d3);

    writer.forceMerge(1);
    writer.commit();
    writer.close();

    DirectoryReader directoryReader;
    DirectoryReader exitableDirectoryReader;
    IndexReader reader;
    IndexSearcher searcher;

    Query query = new PrefixQuery(new Term("default", "o"));

    // Set a fairly high timeout value (infinite) and expect the query to complete in that time frame.
    // Not checking the validity of the result, all we are bothered about in this test is the timing out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, infiniteQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();

    // Set a really low timeout value (immediate) and expect an Exception
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, immediateQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    IndexSearcher slowSearcher = new IndexSearcher(reader);
    expectThrows(ExitingReaderException.class, () -> {
      slowSearcher.search(query, 10);
    });
    reader.close();
   
    // Set maximum time out and expect the query to complete. 
    // Not checking the validity of the result, all we are bothered about in this test is the timing out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, infiniteQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();

    // Set a negative time allowed and expect the query to complete (should disable timeouts)
    // Not checking the validity of the result, all we are bothered about in this test is the timing out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, disabledQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();

    directory.close();
  }

  /**
   * Tests time out check sampling of TermsEnum iterations
   *
   * @throws Exception on error
   */
  public void testExitableTermsEnumSampleTimeoutCheck() throws Exception {
    try (Directory directory = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())))) {
        for (int i = 0; i < 50; i++) {
          Document d1 = new Document();
          d1.add(newTextField("default", "term" + i, Field.Store.YES));
          writer.addDocument(d1);
        }

        writer.forceMerge(1);
        writer.commit();

        DirectoryReader directoryReader;
        DirectoryReader exitableDirectoryReader;
        IndexReader reader;
        IndexSearcher searcher;

        Query query = new PrefixQuery(new Term("default", "term"));

        // Set a fairly high timeout value (infinite) and expect the query to complete in that time frame.
        // Not checking the validity of the result, but checking the sampling kicks in to reduce the number of timeout check
        CountingQueryTimeout queryTimeout = new CountingQueryTimeout();
        directoryReader = DirectoryReader.open(directory);
        exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, queryTimeout);
        reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
        searcher = new IndexSearcher(reader);
        searcher.search(query, 300);
        reader.close();
        // The number of sampled query time out check here depends on two factors:
        // 1. ExitableDirectoryReader.ExitableTermsEnum.NUM_CALLS_PER_TIMEOUT_CHECK
        // 2. MultiTermQueryConstantScoreWrapper.BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD
        assertEquals(5, queryTimeout.getShouldExitCallCount());
      }
    }
  }

  /**
   * Tests timing out of PointValues queries
   *
   * @throws Exception on error
   */
  public void testExitablePointValuesIndexReader() throws Exception {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

    Document d1 = new Document();
    d1.add(new IntPoint("default", 10));
    writer.addDocument(d1);

    Document d2 = new Document();
    d2.add(new IntPoint("default", 100));
    writer.addDocument(d2);

    Document d3 = new Document();
    d3.add(new IntPoint("default", 1000));
    writer.addDocument(d3);

    writer.forceMerge(1);
    writer.commit();
    writer.close();

    DirectoryReader directoryReader;
    DirectoryReader exitableDirectoryReader;
    IndexReader reader;
    IndexSearcher searcher;

    Query query = IntPoint.newRangeQuery("default", 10, 20);

    // Set a fairly high timeout value (infinite) and expect the query to complete in that time frame.
    // Not checking the validity of the result, all we are bothered about in this test is the timing out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, infiniteQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();

    // Set a really low timeout value (immediate) and expect an Exception
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, immediateQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    IndexSearcher slowSearcher = new IndexSearcher(reader);
    expectThrows(ExitingReaderException.class, () -> {
      slowSearcher.search(query, 10);
    });
    reader.close();

    // Set maximum time out and expect the query to complete.
    // Not checking the validity of the result, all we are bothered about in this test is the timing out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, infiniteQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();

    // Set a negative time allowed and expect the query to complete (should disable timeouts)
    // Not checking the validity of the result, all we are bothered about in this test is the timing out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, disabledQueryTimeout());
    reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();

    directory.close();
  }

  private static QueryTimeout disabledQueryTimeout() {
    return new QueryTimeout() {

      @Override
      public boolean shouldExit() {
        return false;
      }

      @Override
      public boolean isTimeoutEnabled() {
        return false;
      }
    };
  }

  private static QueryTimeout infiniteQueryTimeout() {
    return new QueryTimeout() {

      @Override
      public boolean shouldExit() {
        return false;
      }

      @Override
      public boolean isTimeoutEnabled() {
        return true;
      }
    };
  }

  private static class CountingQueryTimeout implements QueryTimeout {
    private int counter = 0;

    @Override
    public boolean shouldExit() {
      counter++;
      return false;
    }

    @Override
    public boolean isTimeoutEnabled() {
      return true;
    }

    public int getShouldExitCallCount() {
      return counter;
    }
  }

  private static QueryTimeout immediateQueryTimeout() {
    return new QueryTimeout() {

      @Override
      public boolean shouldExit() {
        return true;
      }

      @Override
      public boolean isTimeoutEnabled() {
        return true;
      }
    };
  }
  
  @FunctionalInterface
  interface DvFactory {
    DocValuesIterator create(LeafReader leaf) throws IOException;
  }
  
  public void testDocValues() throws IOException {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(new MockAnalyzer(random())));

    Document d1 = new Document();
    addDVs(d1, 10);
    writer.addDocument(d1);

    Document d2 = new Document();
    addDVs(d2, 100);
    writer.addDocument(d2);

    Document d3 = new Document();
    addDVs(d3, 1000);
    writer.addDocument(d3);

    writer.forceMerge(1);
    writer.commit();
    writer.close();

    DirectoryReader directoryReader;
    DirectoryReader exitableDirectoryReader;

    for (DvFactory dvFactory :   Arrays.<DvFactory>asList(
                 (r) -> r.getSortedDocValues("sorted"),    
                 (r) -> r.getSortedSetDocValues("sortedset"),
                 (r) -> r.getSortedNumericDocValues("sortednumeric"),
                 (r) -> r.getNumericDocValues("numeric"),
                 (r) -> r.getBinaryDocValues("binary") 
            ))
    {
      directoryReader = DirectoryReader.open(directory);
      exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, immediateQueryTimeout());
      
      {
        IndexReader  reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
      
        expectThrows(ExitingReaderException.class, () -> {
          LeafReader leaf = reader.leaves().get(0).reader();
          DocValuesIterator iter = dvFactory.create(leaf);
          scan(leaf, iter);
        });
        reader.close();
      }
  
      directoryReader = DirectoryReader.open(directory);
      exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, random().nextBoolean()? 
          infiniteQueryTimeout() : disabledQueryTimeout());
      {
        IndexReader reader = new TestReader(getOnlyLeafReader(exitableDirectoryReader));
        final LeafReader leaf = reader.leaves().get(0).reader();
        scan(leaf, dvFactory.create(leaf));
        assertNull(leaf.getNumericDocValues("absent"));
        assertNull(leaf.getBinaryDocValues("absent"));
        assertNull(leaf.getSortedDocValues("absent"));
        assertNull(leaf.getSortedNumericDocValues("absent"));
        assertNull(leaf.getSortedSetDocValues("absent"));
        
        reader.close();
      }
    }
    
    directory.close();
  
  }

  static private void scan(LeafReader leaf, DocValuesIterator iter ) throws IOException {
    for (iter.nextDoc(); iter.docID()!=DocIdSetIterator.NO_MORE_DOCS
         && iter.docID()<leaf.maxDoc();) {
      final int nextDocId = iter.docID()+1;
      if (random().nextBoolean() && nextDocId<leaf.maxDoc()) {
        if(random().nextBoolean()) {
          iter.advance(nextDocId);
        } else {
          iter.advanceExact(nextDocId);
        }
      } else { 
        iter.nextDoc();
      }
    }
  }
  private void addDVs(Document d1, int i) {
    d1.add(new NumericDocValuesField("numeric", i));
    d1.add(new BinaryDocValuesField("binary", new BytesRef(""+i)));
    d1.add(new SortedDocValuesField("sorted", new BytesRef(""+i)));
    d1.add(new SortedNumericDocValuesField("sortednumeric", i));
    d1.add(new SortedSetDocValuesField("sortedset", new BytesRef(""+i)));
  }
}

