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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.ReferenceDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Ignore;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 *
 * Tests ReferenceDocValues integration into IndexWriter
 *
 */
public class TestReferenceDocValues extends LuceneTestCase {

  // TODO: specialize to graph (ie symmetric) / non-graph references
  public void testSymmetricReference() throws Exception {
    // create the graph with two nodes: 0-1
    try (Directory d = newDirectory()) {
      RandomIndexWriter w = new RandomIndexWriter(random(), d);

      add(w);
      add(w, 0);

      try (DirectoryReader r = w.getReader()) {
        w.close();
        SortedNumericDocValues values = DocValues.getSortedNumeric(getOnlyLeafReader(r), "field");
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.docValueCount());
        assertEquals(1, values.nextValue());
        assertEquals(1, values.nextDoc());
        assertEquals(1, values.docValueCount());
        assertEquals(0, values.nextValue());
      }
    }
  }

  public void testInvalidReferences() throws Exception {
    try (Directory d = newDirectory();
         RandomIndexWriter w = new RandomIndexWriter(random(), d)) {

      // self-reference
      expectThrows(IllegalArgumentException.class, () -> add(w, 0));

      // forward reference
      expectThrows(IllegalArgumentException.class, () -> add(w, 1));

      // negative reference
      expectThrows(IllegalArgumentException.class, () -> add(w, -1));
    }
  }

  public void testSortedIndex() throws Exception {
    // create the graph with two nodes: 0-1
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
    try (Directory d = newDirectory();
         RandomIndexWriter w = new RandomIndexWriter(random(), d, iwc)) {

      // docs inserted in reverse order of sortkey so they will get reordered
      add(w, "sortkey", 2);
      add(w, "sortkey", 1, 0);

      try (DirectoryReader r = w.getReader()) {
        SortedNumericDocValues values = DocValues.getSortedNumeric(getOnlyLeafReader(r), "field");
        NumericDocValues sortkey = DocValues.getNumeric(getOnlyLeafReader(r), "sortkey");
        // doc 0 refers to doc 1
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.docValueCount());
        assertEquals(1, values.nextValue());
        // and has sortkey = 1
        assertEquals(0, sortkey.nextDoc());
        assertEquals(1, sortkey.longValue());

        // doc 1 refers to doc 0
        assertEquals(1, values.nextDoc());
        assertEquals(1, values.docValueCount());
        assertEquals(0, values.nextValue());
        // and has sortkey = 2
        assertEquals(1, sortkey.nextDoc());
        assertEquals(2, sortkey.longValue());
      }
    }
  }

  @Ignore("only for asymmetric graph")
  public void testFlushDeletes() throws Exception {
    // create the graph with two nodes: 0-1
    // delete one document and verify that its reference is deleted
    try (Directory d = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig();
      iwc.setSoftDeletesField(null);
      RandomIndexWriter w = new RandomIndexWriter(random(), d);

      add(w, "id", "a");
      add(w, "id", "b", 0);
      w.deleteDocuments(new Term("id", "a"));

      try (DirectoryReader r = w.getReader()) {
        assertEquals(1, w.getDocStats().numDocs);
        w.close();
        SortedNumericDocValues values = DocValues.getSortedNumeric(getOnlyLeafReader(r), "field");
        // The deleted docs' values are still there!
        assertEquals(0, values.nextDoc());
        assertEquals(1, values.nextValue());
        // But references to it have been dropped
        assertEquals(NO_MORE_DOCS, values.nextDoc());
      }
    }
  }

  @Ignore("only for asymmetric graph")
  public void testMerge() throws Exception {
    // create two segments and merge, creating a disconnected graph
    try (Directory d = newDirectory();
         RandomIndexWriter w = new RandomIndexWriter(random(), d)) {

      add(w, "id", "a");
      add(w, "id", "b", 0);
      w.commit();

      add(w, "id", "c");
      add(w, "id", "d", 0);
      w.forceMerge(1);

      try (DirectoryReader r = w.getReader()) {
        assertEquals(4, w.getDocStats().maxDoc);
        assertEquals(4, w.getDocStats().numDocs);
        SortedNumericDocValues values = DocValues.getSortedNumeric(getOnlyLeafReader(r), "field");
        // extract one ref per doc
        int docid;
        Map<String, String> refs = new HashMap<>();
        while ((docid = values.nextDoc()) != NO_MORE_DOCS) {
          assertEquals(1, values.docValueCount());
          String id = r.document(0).get("id");
          refs.put(id, r.document(docid).get("id"));
        }
        assertEquals(4, refs.size());
        assertEquals("a", refs.get("b"));
        assertEquals("b", refs.get("a"));
        assertEquals("c", refs.get("d"));
        assertEquals("d", refs.get("c"));
      }
    }
  }

  @Ignore("only for asymmetric graph")
  public void testMergeDeletes() throws Exception {
    // create two segments, delete a document and merge. Verify that the reference to the deleted doc is dropped
    try (Directory d = newDirectory();
         RandomIndexWriter w = new RandomIndexWriter(random(), d)) {

      add(w, "id", "a");
      add(w, "id", "b", 0);
      w.commit();

      add(w, "id", "c");
      add(w, "id", "d", 0);
      w.deleteDocuments(new Term("id", "a"));
      w.forceMerge(1);

      try (DirectoryReader r = w.getReader()) {
        // after force merge, the deleted document is really gone
        assertEquals(3, w.getDocStats().maxDoc);
        assertEquals(3, w.getDocStats().numDocs);
        SortedNumericDocValues values = DocValues.getSortedNumeric(getOnlyLeafReader(r), "field");
        int docid;
        Map<String, String> refs = new HashMap<>();
        while ((docid = values.nextDoc()) != NO_MORE_DOCS) {
          assertEquals(1, values.docValueCount());
          String id = r.document(0).get("id");
          refs.put(id, r.document(docid).get("id"));
        }
        assertEquals(2, refs.size());
        assertNull(refs.get("b"));
        assertEquals("c", refs.get("d"));
        assertEquals("d", refs.get("c"));
      }
    }
  }

  @SuppressWarnings("try")
  public void testRandomGraph() throws Exception {
    int numDocs = atLeast(1000) + 2;
    List<Doc> expected = new ArrayList<>();
    IndexWriterConfig iwc = new IndexWriterConfig();
    // TODO: deletions; how about soft deletes?
    boolean sortedIndex = random().nextBoolean();
    if (sortedIndex) {
        iwc.setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)));
    }
    // we must write all documents that refer to each other in a single segment.
    // This restriction means this ReferenceDocValues API cannot be public; it is only for the use of
    // other classes that are aware of the segment state such as other DocValues writers.
    iwc.setMaxBufferedDocs(numDocs + 1);
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    try (Directory d = newDirectory();
         IndexWriter w = new IndexWriter(d, iwc)) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        Doc expectedDoc = new Doc(i, random().nextInt(1000));
        expected.add(expectedDoc);
        Set<Integer> refs = new HashSet<>();
        if (i % 2 == 0 && i > 0) {
          // Only even-numbered docs are in the graph, and every even-numbered doc is linked to at least one other
          int numRefs = 1 + random().nextInt(16); // more than 8 sometimes so we exercise growing an internal array
          for (int j = 0; j < numRefs; j++) {
            int value = random().nextInt(i / 2) * 2;
            refs.add(value);
            // also expect the symmetric ref
            expected.get(value).add(i);
            doc.add(new ReferenceDocValuesField("field", value));
          }
          expectedDoc.refs = new ArrayList<>(refs);
        }
        doc.add(new NumericDocValuesField("sortkey", expectedDoc.sortKey));
        // add the doc
        w.addDocument(doc);
      }
      if (sortedIndex) {
        Collections.sort(expected);
        int[] docmap = new int[expected.size()];
        int i = 0;
        for (Doc doc : expected) {
          docmap[doc.id] = i++;
        }
        for (Doc doc : expected) {
          // map the expected refs to their new ids
          doc.refs = doc.refs.stream().map(ref -> docmap[ref]).collect(Collectors.toList());
        }
      }
      for (Doc doc : expected) {
          Collections.sort(doc.refs);
      }
      /*
      for (int i = 0; i < numDocs && i < 20; i++){
        System.out.println(expected.get(i));
      }
      */
      try (DirectoryReader r = w.getReader()) {
        w.close();
        SortedNumericDocValues values = DocValues.getSortedNumeric(getOnlyLeafReader(r), "field");
        NumericDocValues sortkey = DocValues.getNumeric(getOnlyLeafReader(r), "sortkey");
        for (int i = 0; i < numDocs; i++) {
          assertEquals(i, sortkey.nextDoc());
          long sortValue = sortkey.longValue();
          Doc expectedDoc = expected.get(i);
          assertEquals(expectedDoc.sortKey, sortValue);
          int originalId = expectedDoc.id;
          List<Integer> actual = new ArrayList<>();
          if (originalId % 2 == 0) {
            assertEquals(i, values.nextDoc());
            for (int j = 0; j < values.docValueCount(); j++) {
              actual.add((int) values.nextValue());
            }
          }
          assertEquals("values for doc " + i, expectedDoc.refs, actual);
        }
        assertEquals(NO_MORE_DOCS, values.nextDoc());
      }
    }
  }

  private void add(RandomIndexWriter iw, int... refs) throws IOException {
    Document doc = new Document();
    for (int ref : refs) {
      doc.add(new ReferenceDocValuesField("field", ref));
    }
    iw.addDocument(doc);
  }

  private void add(RandomIndexWriter iw, String field, String value, int... refs) throws IOException  {
    Document doc = new Document();
    for (int ref : refs) {
      doc.add(new ReferenceDocValuesField("field", ref));
    }
    doc.add(new StringField(field, value, Field.Store.YES));
    iw.addDocument(doc);
  }

  private void add(RandomIndexWriter iw, String field, int value, int... refs) throws IOException  {
    Document doc = new Document();
    for (int ref : refs) {
      doc.add(new ReferenceDocValuesField("field", ref));
    }
    doc.add(new NumericDocValuesField(field, value));
    iw.addDocument(doc);
  }

  /**
   * Mock document for recording expected state of index
   */
  static class Doc implements Comparable<Doc> {
    int id;
    int sortKey;
    List<Integer> refs = new ArrayList<>();

    Doc(int id, int sortKey) {
      this.id = id;
      this.sortKey = sortKey;
    }

    void add(int ref) {
      if (refs.size() == 0 || refs.get(refs.size() - 1) != ref) {
        // values will be added in nondecreasing order; keep them unique
        refs.add(ref);
      }
    }

    @Override
    public int compareTo(Doc other) {
      int cmp = sortKey - other.sortKey;
      if (cmp == 0) {
        return id - other.id;
      } else {
        return cmp;
      }
    }

    @Override
    public String toString() {
      return "doc id=" + id + " sortkey=" + sortKey + " refs=" + refs;
    }
  }

}
