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


import java.util.concurrent.RejectedExecutionException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestReaderClosed extends LuceneTestCase {
  private DirectoryReader reader;
  private Directory dir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, 
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false))
          .setMaxBufferedDocs(TestUtil.nextInt(random(), 50, 1000)));
    
    Document doc = new Document();
    Field field = newStringField("field", "", Field.Store.NO);
    doc.add(field);

    // we generate aweful prefixes: good for testing.
    // but for preflex codec, the test can be very slow, so use less iterations.
    int num = atLeast(10);
    for (int i = 0; i < num; i++) {
      field.setStringValue(TestUtil.randomUnicodeString(random(), 10));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = writer.getReader();
    writer.close();
  }
  
  public void test() throws Exception {
    assertTrue(reader.getRefCount() > 0);
    IndexSearcher searcher = newSearcher(reader);
    TermRangeQuery query = TermRangeQuery.newStringRange("field", "a", "z", true, true);
    searcher.search(query, 5);
    reader.close();
    try {
      searcher.search(query, 5);
    } catch (AlreadyClosedException ace) {
      // expected
    } catch (RejectedExecutionException ree) {
      // expected if the searcher has been created with threads since LuceneTestCase
      // closes the thread-pool in a reader close listener
    }
  }

  // LUCENE-3800
  public void testReaderChaining() throws Exception {
    assertTrue(reader.getRefCount() > 0);
    LeafReader wrappedReader = new ParallelLeafReader(getOnlyLeafReader(reader));

    // We wrap with a OwnCacheKeyMultiReader so that closing the underlying reader
    // does not terminate the threadpool (if that index searcher uses one)
    IndexSearcher searcher = newSearcher(new OwnCacheKeyMultiReader(wrappedReader));

    TermRangeQuery query = TermRangeQuery.newStringRange("field", "a", "z", true, true);
    searcher.search(query, 5);
    reader.close(); // close original child reader
    try {
      searcher.search(query, 5);
    } catch (Exception e) {
      AlreadyClosedException ace = null;
      for (Throwable t = e; t != null; t = t.getCause()) {
        if (t instanceof AlreadyClosedException) {
          ace = (AlreadyClosedException) t;
        }
      }
      if (ace == null) {
        throw new AssertionError("Query failed, but not due to an AlreadyClosedException", e);
      }
      assertEquals(
        "this IndexReader cannot be used anymore as one of its child readers was closed",
        ace.getMessage()
      );
    } finally {
      // close executor: in case of wrap-wrap-wrapping
      searcher.getIndexReader().close();
    }
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }
}
