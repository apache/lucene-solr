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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestSortingCodecReader extends LuceneTestCase {

  public void testSortOnAddIndicesInt() throws IOException {
    Directory tmpDir = newDirectory();
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(tmpDir, iwc);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 18));
    w.addDocument(doc);
    // so we get more than one segment, so that forceMerge actually does merge, since we only get a sorted segment by merging:
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("foo", -1));
    w.addDocument(doc);
    w.commit();

    doc = new Document();
    doc.add(new NumericDocValuesField("foo", 7));
    w.addDocument(doc);
    w.commit();
    w.close();
    Sort indexSort = new Sort(new SortField("foo", SortField.Type.INT));

    iwc = new IndexWriterConfig(new MockAnalyzer(random())).setIndexSort(indexSort);
    w = new IndexWriter(dir, iwc);
    try (DirectoryReader reader = DirectoryReader.open(tmpDir)) {
      List<CodecReader> readers = new ArrayList<>();
      for (LeafReaderContext ctx : reader.leaves()) {
        readers.add(SortingCodecReader.wrap(SlowCodecReaderWrapper.wrap(ctx.reader()), indexSort));
      }
      w.addIndexes(readers.toArray(new CodecReader[0]));
    }
    DirectoryReader r = DirectoryReader.open(w);
    LeafReader leaf = getOnlyLeafReader(r);
    assertEquals(3, leaf.maxDoc());
    NumericDocValues values = leaf.getNumericDocValues("foo");
    assertEquals(0, values.nextDoc());
    assertEquals(-1, values.longValue());
    assertEquals(1, values.nextDoc());
    assertEquals(7, values.longValue());
    assertEquals(2, values.nextDoc());
    assertEquals(18, values.longValue());
    assertNotNull(leaf.getMetaData().getSort());
    IOUtils.close(r, w, dir, tmpDir);
  }

  public void testSortOnAddIndicesRandom() throws IOException {
    try (Directory dir = newDirectory()) {
      int numDocs = atLeast(200);
      int actualNumDocs = -1;
      try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          doc.add(new NumericDocValuesField("foo", random().nextInt(20)));
          doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
          doc.add(new LongPoint("id", i));

          FieldType ft = new FieldType(StringField.TYPE_NOT_STORED);
          ft.setStoreTermVectors(true);
          doc.add(new Field("term_vectors", "test" + i, ft));
          if (rarely() == false) {
            doc.add(new NumericDocValuesField("id", i));
          } else {
            doc.add(new NumericDocValuesField("alt_id", i));
          }
          iw.addDocument(doc);
          if (random().nextInt(5) == 0) {
            final int id = TestUtil.nextInt(random(), 0, i);
            iw.deleteDocuments(new Term("id", Integer.toString(id)));
          }
        }
        iw.commit();
        actualNumDocs = iw.getDocStats().numDocs;
      }
      Sort indexSort = new Sort(new SortField("id", SortField.Type.INT), new SortField("alt_id", SortField.Type.INT));
      try (Directory sortDir = newDirectory()) {
        try (IndexWriter writer = new IndexWriter(sortDir, newIndexWriterConfig().setIndexSort(indexSort))) {
          try (DirectoryReader reader = DirectoryReader.open(dir)) {
            List<CodecReader> readers = new ArrayList<>();
            for (LeafReaderContext ctx : reader.leaves()) {
              readers.add(SortingCodecReader.wrap(SlowCodecReaderWrapper.wrap(ctx.reader()), indexSort));
            }
            writer.addIndexes(readers.toArray(new CodecReader[0]));
          }
          try (DirectoryReader r = DirectoryReader.open(writer)) {
            LeafReader leaf = getOnlyLeafReader(r);
            assertEquals(actualNumDocs, leaf.maxDoc());
            NumericDocValues ids = leaf.getNumericDocValues("id");
            int prevId = -1;
            for (int i = 0; i < actualNumDocs; i++) {
              int idNext = ids.nextDoc();
              assertTrue(prevId < idNext);
              if (idNext == DocIdSetIterator.NO_MORE_DOCS) {
                ids = leaf.getNumericDocValues("alt_id");
                prevId = ids.nextDoc();
              } else {
                prevId = idNext;
              }
              if (idNext != DocIdSetIterator.NO_MORE_DOCS) {
                assertTrue(leaf.getTermVectors(idNext).terms("term_vectors").iterator().seekExact(new BytesRef("test" + ids.longValue())));
                assertEquals(Long.toString(ids.longValue()), leaf.document(idNext).get("id"));
                IndexSearcher searcher = new IndexSearcher(r);
                TopDocs result = searcher.search(LongPoint.newExactQuery("id", ids.longValue()), 1);
                assertEquals(1, result.totalHits.value);
                assertEquals(idNext, result.scoreDocs[0].doc);

                result = searcher.search(new TermQuery(new Term("id", "" + ids.longValue())), 1);
                assertEquals(1, result.totalHits.value);
                assertEquals(idNext, result.scoreDocs[0].doc);
              }
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, ids.nextDoc());
          }
        }
      }
    }
  }
}
