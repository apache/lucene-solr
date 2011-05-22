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

import org.apache.lucene.store.Directory;
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
      @Override
      public FieldsEnum iterator() throws IOException {
        return new TestFieldsEnum(super.iterator());
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

    private static class TestFieldsEnum extends FilterFieldsEnum {
      TestFieldsEnum(FieldsEnum in) {
        super(in);
      }

      @Override
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
      super(new SlowMultiReaderWrapper(reader));
    }

    @Override
    public Fields fields() throws IOException {
      return new TestFields(super.fields());
    }
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

    Directory target = newDirectory();
    writer = new IndexWriter(target, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    IndexReader reader = new TestReader(IndexReader.open(directory, true));
    writer.addIndexes(reader);
    writer.close();
    reader.close();
    reader = IndexReader.open(target, true);
    
    TermsEnum terms = MultiFields.getTerms(reader, "default").iterator();
    while (terms.next() != null) {
      assertTrue(terms.term().utf8ToString().indexOf('e') != -1);
    }
    
    assertEquals(TermsEnum.SeekStatus.FOUND, terms.seek(new BytesRef("one")));
    
    DocsAndPositionsEnum positions = terms.docsAndPositions(MultiFields.getDeletedDocs(reader),
                                                            null);
    while (positions.nextDoc() != DocsEnum.NO_MORE_DOCS) {
      assertTrue((positions.docID() % 2) == 1);
    }

    reader.close();
    directory.close();
    target.close();
  }
}
