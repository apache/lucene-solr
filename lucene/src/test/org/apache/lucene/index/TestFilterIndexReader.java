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
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Bits;

import java.io.IOException;

public class TestFilterIndexReader extends LuceneTestCase {

  private static class TestReader extends FilterIndexReader {

    /** Filter that only permits terms containing 'e'.*/
    private static class TestFields extends FilterFields {
      TestFields(Fields in) {
        super(in);
      }
      public FieldsEnum iterator() throws IOException {
        return new TestFieldsEnum(super.iterator());
      }
      public Terms terms(String field) throws IOException {
        return new TestTerms(super.terms(field));
      }
    }

    private static class TestTerms extends FilterTerms {
      TestTerms(Terms in) {
        super(in);
      }

      public TermsEnum iterator() throws IOException {
        return new TestTermsEnum(super.iterator());
      }
    }

    private static class TestFieldsEnum extends FilterFieldsEnum {
      TestFieldsEnum(FieldsEnum in) {
        super(in);
      }

      public TermsEnum terms() throws IOException {
        return new TestTermsEnum(super.terms());
      }
    }

    private static class TestTermsEnum extends FilterTermsEnum {
      public TestTermsEnum(TermsEnum in) {
        super(in);
      }

      /** Scan for terms containing the letter 'e'.*/
      @Override
      public BytesRef next() throws IOException {
        BytesRef text;
        while ((text = in.next()) != null) {
          if (text.utf8ToString().indexOf('e') != -1)
            return text;
        }
        return null;
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
        return new TestPositions(super.docsAndPositions(skipDocs, reuse == null ? null : ((FilterDocsAndPositionsEnum) reuse).in));
      }
    }

    /** Filter that only returns odd numbered documents. */
    private static class TestPositions extends FilterDocsAndPositionsEnum {
      public TestPositions(DocsAndPositionsEnum in) {
        super(in);
      }

      /** Scan for odd numbered documents. */
      @Override
      public int nextDoc() throws IOException {
        int doc;
        while ((doc = in.nextDoc()) != NO_MORE_DOCS) {
          if ((doc % 2) == 1)
            return doc;
        }
        return NO_MORE_DOCS;
      }
    }
    
    public TestReader(IndexReader reader) {
      super(reader);
    }

    @Override
    public Fields fields() throws IOException {
      return new TestFields(super.fields());
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
    IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer()));

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

    //IndexReader reader = new TestReader(IndexReader.open(directory, true));
    RAMDirectory target = new MockRAMDirectory();
    writer = new IndexWriter(target, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer()));
    IndexReader reader = new TestReader(IndexReader.open(directory, true));
    writer.addIndexes(reader);
    writer.close();
    reader.close();
    reader = IndexReader.open(target, true);
    

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
