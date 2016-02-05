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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.ExitableDirectoryReader.ExitingReaderException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

/**
 * Test that uses a default/lucene Implementation of {@link QueryTimeout}
 * to exit out long running queries that take too long to iterate over Terms.
 */
public class TestExitableDirectoryReader extends LuceneTestCase {
  private static class TestReader extends FilterLeafReader {

    private static class TestFields extends FilterFields {
      TestFields(Fields in) {
        super(in);
      }

      @Override
      public Terms terms(String field) throws IOException {
        return new TestTerms(super.terms(field));
      }
    }

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

    public TestReader(IndexReader reader) throws IOException {
      super(SlowCompositeReaderWrapper.wrap(reader));
    }

    @Override
    public Fields fields() throws IOException {
      return new TestFields(super.fields());
    }
  }

  /**
   * Tests timing out of TermsEnum iterations
   * @throws Exception on error
   */
  public void testExitableFilterIndexReader() throws Exception {
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

    writer.commit();
    writer.close();

    DirectoryReader directoryReader;
    DirectoryReader exitableDirectoryReader;
    IndexReader reader;
    IndexSearcher searcher;

    Query query = new PrefixQuery(new Term("default", "o"));

    // Set a fairly high timeout value (1 second) and expect the query to complete in that time frame.
    // Not checking the validity of the result, all we are bothered about in this test is the timing out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, new QueryTimeoutImpl(1000));
    reader = new TestReader(exitableDirectoryReader);
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();
    exitableDirectoryReader.close();


    // Set a really low timeout value (1 millisecond) and expect an Exception
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, new QueryTimeoutImpl(1));
    reader = new TestReader(exitableDirectoryReader);
    searcher = new IndexSearcher(reader);
    try {
      searcher.search(query, 10);
      fail("This query should have led to an ExitingReaderException!");
    } catch (ExitingReaderException ex) {
      // Do nothing, we expect this!
    } finally {
      reader.close();
      exitableDirectoryReader.close();
    }
   
    // Set maximum time out and expect the query to complete. 
    // Not checking the validity of the result, all we are bothered about in this test is the timing out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, new QueryTimeoutImpl(Long.MAX_VALUE));
    reader = new TestReader(exitableDirectoryReader);
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();
    exitableDirectoryReader.close();

    // Set a negative time allowed and expect the query to complete (should disable timeouts)
    // Not checking the validity of the result, all we are bothered about in this test is the timing out.
    directoryReader = DirectoryReader.open(directory);
    exitableDirectoryReader = new ExitableDirectoryReader(directoryReader, new QueryTimeoutImpl(-189034L));
    reader = new TestReader(exitableDirectoryReader);
    searcher = new IndexSearcher(reader);
    searcher.search(query, 10);
    reader.close();
    exitableDirectoryReader.close();

    directory.close();
  }
}

