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

import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
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
    public TermEnum terms() throws IOException {
      return new TestTermEnum(in.terms());
    }

    /** Filter positions with TestTermPositions. */
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
    RAMDirectory directory = new MockRAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true,
                                         IndexWriter.MaxFieldLength.LIMITED);

    Document d1 = new Document();
    d1.add(new Field("default","one two", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(d1);

    Document d2 = new Document();
    d2.add(new Field("default","one three", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(d2);

    Document d3 = new Document();
    d3.add(new Field("default","two four", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(d3);

    writer.close();

    IndexReader reader = new TestReader(IndexReader.open(directory));

    assertTrue(reader.isOptimized());
    
    TermEnum terms = reader.terms();
    while (terms.next()) {
      assertTrue(terms.term().text().indexOf('e') != -1);
    }
    terms.close();
    
    TermPositions positions = reader.termPositions(new Term("default", "one"));
    while (positions.next()) {
      assertTrue((positions.doc() % 2) == 1);
    }

    reader.close();
    directory.close();
  }
}
