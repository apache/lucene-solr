package org.apache.lucene.index;

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


import org.apache.lucene.util.LuceneTestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.IOException;

public class TestFilterIndexReader extends LuceneTestCase {

  private static class TestReader extends FilterIndexReader {

     /** Filter that only permits terms containing 'e'.*/
    private static class TestTermEnum extends FilterTermEnum {
      public TestTermEnum(TermEnum termEnum) {
        super(termEnum);
      }

      /** Scan for terms containing the letter 'e'.*/
      @Override
      public boolean next() throws IOException {
        while (in.next()) {
          if (in.term().text().indexOf('e') != -1)
            return true;
        }
        return false;
      }
    }
    
    /** Filter that only returns odd numbered documents. */
    private static class TestTermPositions extends FilterTermPositions {
      public TestTermPositions(TermPositions in) {
        super(in);
      }

      /** Scan for odd numbered documents. */
      @Override
      public boolean next() throws IOException {
        while (in.next()) {
          if ((in.doc() % 2) == 1)
            return true;
        }
        return false;
      }
    }
    
    public TestReader(IndexReader reader) {
      super(reader);
    }

    /** Filter terms with TestTermEnum. */
    @Override
    public TermEnum terms() throws IOException {
      return new TestTermEnum(in.terms());
    }

    /** Filter positions with TestTermPositions. */
    @Override
    public TermPositions termPositions() throws IOException {
      return new TestTermPositions(in.termPositions());
    }
  }


  /** Main for running test case by itself. */
  public static void main(String args[]) {
    TestRunner.run (new TestSuite(TestIndexReader.class));
  }
    
  /**
   * Tests the IndexReader.getFieldNames implementation
   * @throws Exception on error
   */
  public void testFilterIndexReader() throws Exception {
    Directory directory = newDirectory();
    IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));

    Document d1 = new Document();
    d1.add(newField("default","one two", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(d1);

    Document d2 = new Document();
    d2.add(newField("default","one three", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(d2);

    Document d3 = new Document();
    d3.add(newField("default","two four", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(d3);

    writer.close();

    IndexReader reader = new TestReader(IndexReader.open(directory, true));
    TermEnum terms = reader.terms();
    while (terms.next()) {
      assertTrue(terms.term().text().indexOf('e') != -1);
    }
    terms.close();
    
    TermPositions positions = reader.termPositions(new Term("default", "one"));
    while (positions.next()) {
      assertTrue((positions.doc() % 2) == 1);
    }

    int NUM_DOCS = 3;

    TermDocs td = reader.termDocs(null);
    for(int i=0;i<NUM_DOCS;i++) {
      assertTrue(td.next());
      assertEquals(i, td.doc());
      assertEquals(1, td.freq());
    }
    td.close();
    reader.close();
    directory.close();
  }
}
