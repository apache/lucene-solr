package org.apache.lucene.search;

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
import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document2;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * 
 */
public class TestFieldValueFilter extends LuceneTestCase {

  public void testFieldValueFilterNoValue() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random())));
    int docs = atLeast(10);
    int[] docStates = buildIndex(writer, docs);
    int numDocsNoValue = 0;
    for (int i = 0; i < docStates.length; i++) {
      if (docStates[i] == 0) {
        numDocsNoValue++;
      }
    }

    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = newSearcher(reader);
    TopDocs search = searcher.search(new TermQuery(new Term("all", "test")),
        new FieldValueFilter("some", true), docs);
    assertEquals(search.totalHits, numDocsNoValue);
    
    ScoreDoc[] scoreDocs = search.scoreDocs;
    for (ScoreDoc scoreDoc : scoreDocs) {
      assertNull(reader.document(scoreDoc.doc).get("some"));
    }
    
    reader.close();
    directory.close();
  }
  
  public void testFieldValueFilter() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random())));
    int docs = atLeast(10);
    int[] docStates = buildIndex(writer, docs);
    int numDocsWithValue = 0;
    for (int i = 0; i < docStates.length; i++) {
      if (docStates[i] == 1) {
        numDocsWithValue++;
      }
    }
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = newSearcher(reader);
    Filter filter = new FieldValueFilter("some");
    TopDocs search = searcher.search(new TermQuery(new Term("all", "test")), filter, docs);
    assertEquals(search.totalHits, numDocsWithValue);
    
    ScoreDoc[] scoreDocs = search.scoreDocs;
    for (ScoreDoc scoreDoc : scoreDocs) {
      assertEquals("value", reader.document(scoreDoc.doc).get("some"));
    }
    
    reader.close();
    directory.close();
  }

  public void testOptimizations() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random())));
    final int docs = atLeast(10);
    buildIndex(writer, docs);
    IndexReader reader = DirectoryReader.open(directory);
    LeafReader leafReader = reader.leaves().get(0).reader();
    
    FilterLeafReader filterReader = new FilterLeafReader(leafReader) {
      @Override
      public Bits getDocsWithField(String field) throws IOException {
        switch (field) {
          case "with_matchall":
            return new Bits.MatchAllBits(maxDoc());
          case "with_matchno":
            return new Bits.MatchNoBits(maxDoc());
          case "with_bitset":
            BitSet b = random().nextBoolean() ? new SparseFixedBitSet(maxDoc()) : new FixedBitSet(maxDoc());
            b.set(random().nextInt(maxDoc()));
            return b;
        }
        return super.getDocsWithField(field);
      }
    };

    Filter filter = new FieldValueFilter("with_matchall", true);
    DocIdSet set = filter.getDocIdSet(filterReader.getContext(), null);
    assertNull(set);

    filter = new FieldValueFilter("with_matchno");
    set = filter.getDocIdSet(filterReader.getContext(), null);
    assertNull(set);

    filter = new FieldValueFilter("with_bitset");
    set = filter.getDocIdSet(filterReader.getContext(), null);
    assertTrue(set instanceof BitDocIdSet);

    reader.close();
    directory.close();
  }

  private int[] buildIndex(RandomIndexWriter writer, int docs)
      throws IOException {
    FieldTypes fieldTypes = writer.getFieldTypes();
    int[] docStates = new int[docs];
    for (int i = 0; i < docs; i++) {
      Document2 doc = writer.newDocument();
      if (random().nextBoolean()) {
        docStates[i] = 1;
        doc.addShortText("some", "value");
      }
      doc.addShortText("all", "test");
      doc.addUniqueInt("id", i);
      writer.addDocument(doc);
    }
    writer.commit();
    int numDeletes = random().nextInt(docs);
    for (int i = 0; i < numDeletes; i++) {
      int docID = random().nextInt(docs);
      writer.deleteDocuments(fieldTypes.newIntTerm("id", docID));
      docStates[docID] = 2;
    }
    writer.close();
    return docStates;
  }

}
