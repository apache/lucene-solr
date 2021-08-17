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


import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestIndexableField extends LuceneTestCase {

  private static class MyField implements IndexableField {

    private final int counter;
    private final IndexableFieldType fieldType = new IndexableFieldType() {
      @Override
      public boolean stored() {
        return (counter & 1) == 0 || (counter % 10) == 3;
      }

      @Override
      public boolean tokenized() {
        return true;
      }

      @Override
      public boolean storeTermVectors() {
        return indexOptions() != IndexOptions.NONE && counter % 2 == 1 && counter % 10 != 9;
      }

      @Override
      public boolean storeTermVectorOffsets() {
        return storeTermVectors() && counter % 10 != 9;
      }

      @Override
      public boolean storeTermVectorPositions() {
        return storeTermVectors() && counter % 10 != 9;
      }
      
      @Override
      public boolean storeTermVectorPayloads() {
        return storeTermVectors() && counter % 10 != 9;
      }

      @Override
      public boolean omitNorms() {
        return false;
      }

      @Override
      public IndexOptions indexOptions() {
        return counter%10 == 3 ? IndexOptions.NONE : IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
      }

      @Override
      public DocValuesType docValuesType() {
        return DocValuesType.NONE;
      }

      @Override
      public int pointDimensionCount() {
        return 0;
      }

      @Override
      public int pointIndexDimensionCount() {
        return 0;
      }

      @Override
      public int pointNumBytes() {
        return 0;
      }

      @Override
      public Map<String, String> getAttributes() {
        return null;
      }
    };

    public MyField(int counter) {
      this.counter = counter;
    }

    @Override
    public String name() {
      return "f" + counter;
    }

    @Override
    public BytesRef binaryValue() {
      if ((counter%10) == 3) {
        final byte[] bytes = new byte[10];
        for(int idx=0;idx<bytes.length;idx++) {
          bytes[idx] = (byte) (counter+idx);
        }
        return newBytesRef(bytes, 0, bytes.length);
      } else {
        return null;
      }
    }

    @Override
    public String stringValue() {
      final int fieldID = counter%10;
      if (fieldID != 3 && fieldID != 7) {
        return "text " + counter;
      } else {
        return null;
      }
    }

    @Override
    public Reader readerValue() {
      if (counter%10 == 7) {
        return new StringReader("text " + counter);
      } else {
        return null;
      }
    }

    @Override
    public Number numericValue() {
      return null;
    }

    @Override
    public IndexableFieldType fieldType() {
      return fieldType;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream previous) {
      return readerValue() != null ? analyzer.tokenStream(name(), readerValue()) :
        analyzer.tokenStream(name(), new StringReader(stringValue()));
    }
  }

  // Silly test showing how to index documents w/o using Lucene's core
  // Document nor Field class
  public void testArbitraryFields() throws Exception {

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final int NUM_DOCS = atLeast(27);
    if (VERBOSE) {
      System.out.println("TEST: " + NUM_DOCS + " docs");
    }
    final int[] fieldsPerDoc = new int[NUM_DOCS];
    int baseCount = 0;

    for(int docCount=0;docCount<NUM_DOCS;docCount++) {
      final int fieldCount = TestUtil.nextInt(random(), 1, 17);
      fieldsPerDoc[docCount] = fieldCount-1;

      final int finalDocCount = docCount;
      if (VERBOSE) {
        System.out.println("TEST: " + fieldCount + " fields in doc " + docCount);
      }

      final int finalBaseCount = baseCount;
      baseCount += fieldCount-1;

      Iterable<IndexableField> d = new Iterable<IndexableField>() {
        @Override
        public Iterator<IndexableField> iterator() {
          return new Iterator<IndexableField>() {
            int fieldUpto;

            @Override
            public boolean hasNext() {
              return fieldUpto < fieldCount;
            }

            @Override
            public IndexableField next() {
              assert fieldUpto < fieldCount;
              if (fieldUpto == 0) {
                fieldUpto = 1;
                return newStringField("id", ""+finalDocCount, Field.Store.YES);
              } else {
                return new MyField(finalBaseCount + (fieldUpto++-1));
              }
            }

            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }
          };
        }
        };
      w.addDocument(d);
    }

    final IndexReader r = w.getReader();
    w.close();

    final IndexSearcher s = newSearcher(r);
    int counter = 0;
    for(int id=0;id<NUM_DOCS;id++) {
      if (VERBOSE) {
        System.out.println("TEST: verify doc id=" + id + " (" + fieldsPerDoc[id] + " fields) counter=" + counter);
      }

      final TopDocs hits = s.search(new TermQuery(new Term("id", ""+id)), 1);
      assertEquals(1, hits.totalHits.value);
      final int docID = hits.scoreDocs[0].doc;
      final Document doc = s.doc(docID);
      final int endCounter = counter + fieldsPerDoc[id];
      while(counter < endCounter) {
        final String name = "f" + counter;
        final int fieldID = counter % 10;

        final boolean stored = (counter&1) == 0 || fieldID == 3;
        final boolean binary = fieldID == 3;
        final boolean indexed = fieldID != 3;

        final String stringValue;
        if (fieldID != 3 && fieldID != 9) {
          stringValue = "text " + counter;
        } else {
          stringValue = null;
        }

        // stored:
        if (stored) {
          IndexableField f = doc.getField(name);
          assertNotNull("doc " + id + " doesn't have field f" + counter, f);
          if (binary) {
            assertNotNull("doc " + id + " doesn't have field f" + counter, f);
            final BytesRef b = f.binaryValue();
            assertNotNull(b);
            assertEquals(10, b.length);
            for(int idx=0;idx<10;idx++) {
              assertEquals((byte) (idx+counter), b.bytes[b.offset+idx]);
            }
          } else {
            assert stringValue != null;
            assertEquals(stringValue, f.stringValue());
          }
        }
        
        if (indexed) {
          final boolean tv = counter % 2 == 1 && fieldID != 9;
          if (tv) {
            final Terms tfv = r.getTermVectors(docID).terms(name);
            assertNotNull(tfv);
            TermsEnum termsEnum = tfv.iterator();
            assertEquals(newBytesRef("" + counter), termsEnum.next());
            assertEquals(1, termsEnum.totalTermFreq());
            PostingsEnum dpEnum = termsEnum.postings(null, PostingsEnum.ALL);
            assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
            assertEquals(1, dpEnum.freq());
            assertEquals(1, dpEnum.nextPosition());

            assertEquals(newBytesRef("text"), termsEnum.next());
            assertEquals(1, termsEnum.totalTermFreq());
            dpEnum = termsEnum.postings(dpEnum, PostingsEnum.ALL);
            assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
            assertEquals(1, dpEnum.freq());
            assertEquals(0, dpEnum.nextPosition());

            assertNull(termsEnum.next());

            // TODO: offsets
            
          } else {
            Fields vectors = r.getTermVectors(docID);
            assertTrue(vectors == null || vectors.terms(name) == null);
          }

          BooleanQuery.Builder bq = new BooleanQuery.Builder();
          bq.add(new TermQuery(new Term("id", ""+id)), BooleanClause.Occur.MUST);
          bq.add(new TermQuery(new Term(name, "text")), BooleanClause.Occur.MUST);
          final TopDocs hits2 = s.search(bq.build(), 1);
          assertEquals(1, hits2.totalHits.value);
          assertEquals(docID, hits2.scoreDocs[0].doc);

          bq = new BooleanQuery.Builder();
          bq.add(new TermQuery(new Term("id", ""+id)), BooleanClause.Occur.MUST);
          bq.add(new TermQuery(new Term(name, ""+counter)), BooleanClause.Occur.MUST);
          final TopDocs hits3 = s.search(bq.build(), 1);
          assertEquals(1, hits3.totalHits.value);
          assertEquals(docID, hits3.scoreDocs[0].doc);
        }

        counter++;
      }
    }

    r.close();
    dir.close();
  }

  private static class CustomField implements IndexableField {
    @Override
    public BytesRef binaryValue() {
      return null;
    }

    @Override
    public String stringValue() {
      return "foobar";
    }

    @Override
    public Reader readerValue() {
      return null;
    }

    @Override
    public Number numericValue() {
      return null;
    }

    @Override
    public String name() {
      return "field";
    }

    @Override
    public TokenStream tokenStream(Analyzer a, TokenStream reuse) {
      return null;
    }

    @Override
    public IndexableFieldType fieldType() {
      FieldType ft = new FieldType(StoredField.TYPE);
      ft.setStoreTermVectors(true);
      ft.freeze();
      return ft;
    }
  }

  // LUCENE-5611
  public void testNotIndexedTermVectors() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    expectThrows(IllegalArgumentException.class, () -> w.addDocument(Collections.singletonList(new CustomField())));
    w.close();
    dir.close();
  }
}
