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
package org.apache.solr.search.grouping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.AllGroupHeadsCollector;
import org.apache.lucene.search.grouping.TermGroupSelector;
import org.apache.lucene.search.grouping.ValueSourceGroupSelector;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ToParentBlockJoinSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.SolrTestCase;

public class AllGroupHeadsCollectorTest extends SolrTestCase {

  public void testBasicBlockJoin() throws Exception {
    final String groupField = "author";
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random(),
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    DocValuesType valueType = DocValuesType.SORTED;

    // 0
    Document doc = new Document();
    addGroupField(doc, groupField, "author1", valueType);
    doc.add(newTextField("content", "random text", Field.Store.NO));
    doc.add(new NumericDocValuesField("id_1", 1));
    doc.add(new SortedDocValuesField("id_2", new BytesRef("1")));
    addParent(w, doc, new SortedSetDocValuesField("id_3", new BytesRef("10")),
                    new SortedSetDocValuesField("id_3", new BytesRef("11")));

    // 1
    doc = new Document();
    addGroupField(doc, groupField, "author1", valueType);
    doc.add(newTextField("content", "some more random text blob", Field.Store.NO));
    doc.add(new NumericDocValuesField("id_1", 2));
    doc.add(new SortedDocValuesField("id_2", new BytesRef("2")));
    addParent(w, doc, new SortedSetDocValuesField("id_3", new BytesRef("20")),
          new SortedSetDocValuesField("id_3", new BytesRef("21")));

    // 2
    doc = new Document();
    addGroupField(doc, groupField, "author1", valueType);
    doc.add(newTextField("content", "some more random textual data", Field.Store.NO));
    doc.add(new NumericDocValuesField("id_1", 3));
    doc.add(new SortedDocValuesField("id_2", new BytesRef("3")));
    addParent(w, doc, new SortedSetDocValuesField("id_3", new BytesRef("30")),
      new SortedSetDocValuesField("id_3", new BytesRef("31")));
    w.commit(); // To ensure a second segment

    // 3
    doc = new Document();
    addGroupField(doc, groupField, "author2", valueType);
    doc.add(newTextField("content", "some random text", Field.Store.NO));
    doc.add(new NumericDocValuesField("id_1", 4));
    doc.add(new SortedDocValuesField("id_2", new BytesRef("4")));
    addParent(w, doc, new SortedSetDocValuesField("id_3", new BytesRef("40")),
         new SortedSetDocValuesField("id_3", new BytesRef("41")));

    // 4
    doc = new Document();
    addGroupField(doc, groupField, "author3", valueType);
    doc.add(newTextField("content", "some more random text", Field.Store.NO));
    doc.add(new NumericDocValuesField("id_1", 5));
    doc.add(new SortedDocValuesField("id_2", new BytesRef("5")));
    addParent(w, doc, new SortedSetDocValuesField("id_3", new BytesRef("50")),
                 new SortedSetDocValuesField("id_3", new BytesRef("51")));

    // 5
    doc = new Document();
    addGroupField(doc, groupField, "author3", valueType);
    doc.add(newTextField("content", "random blob", Field.Store.NO));
    doc.add(new NumericDocValuesField("id_1", 6));
    doc.add(new SortedDocValuesField("id_2", new BytesRef("6")));
    addParent(w, doc, new SortedSetDocValuesField("id_3", new BytesRef("60")),
              new SortedSetDocValuesField("id_3", new BytesRef("61")));

    // 6 -- no author field
    doc = new Document();
    doc.add(newTextField("content", "random word stuck in alot of other text", Field.Store.NO));
    doc.add(new NumericDocValuesField("id_1", 6));
    doc.add(new SortedDocValuesField("id_2", new BytesRef("6")));
    addParent(w, doc, new SortedSetDocValuesField("id_3", new BytesRef("60")),
            new SortedSetDocValuesField("id_3", new BytesRef("61")));

    // 7 -- no author field
    doc = new Document();
    doc.add(newTextField("content", "random word stuck in alot of other text", Field.Store.NO));
    doc.add(new NumericDocValuesField("id_1", 7));
    doc.add(new SortedDocValuesField("id_2", new BytesRef("7")));
    addParent(w, doc, new SortedSetDocValuesField("id_3", new BytesRef("70")),
           new SortedSetDocValuesField("id_3", new BytesRef("71")));

    IndexReader reader = w.getReader();
    IndexSearcher indexSearcher = newSearcher(reader);

    w.close();
    int maxDoc = reader.maxDoc();

    final QueryBitSetProducer parentFilter = new QueryBitSetProducer(new TermQuery(new Term("type","parent")));
    final QueryBitSetProducer childFilter = new QueryBitSetProducer(new TermQuery(new Term("type","child")));
    Sort sortWithinGroup2 = new Sort(new ToParentBlockJoinSortField("id_3", Type.STRING, true, 
        parentFilter, childFilter));
    AllGroupHeadsCollector<?> allGroupHeadsCollector = createRandomCollector(groupField, sortWithinGroup2);
    indexSearcher.search(new TermQuery(new Term("content", "random")), allGroupHeadsCollector);
    assertTrue(arrayContains(shiftForBlocks(2, 3, 5, 7), allGroupHeadsCollector.retrieveGroupHeads()));
    assertTrue(openBitSetContains(shiftForBlocks(2, 3, 5, 7), allGroupHeadsCollector.retrieveGroupHeads(maxDoc), maxDoc));

    Sort sortWithinGroup3 = new Sort(new ToParentBlockJoinSortField("id_3", Type.STRING, false, 
        parentFilter, childFilter));
    allGroupHeadsCollector = createRandomCollector(groupField, sortWithinGroup3);
    indexSearcher.search(new TermQuery(new Term("content", "random")), allGroupHeadsCollector);
    // 7 b/c higher doc id wins, even if order of field is in not in reverse.
    assertTrue(arrayContains(shiftForBlocks(0, 3, 4, 6), allGroupHeadsCollector.retrieveGroupHeads()));
    assertTrue(openBitSetContains(shiftForBlocks(0, 3, 4, 6), allGroupHeadsCollector.retrieveGroupHeads(maxDoc), maxDoc));
    indexSearcher.getIndexReader().close();
    dir.close();
  }

  /** converts flat docnums from lucene/grouping AllGroupHeadsCollectorTest.testBasic() to parent docNums (zero based) with two child docs */
  private int[] shiftForBlocks(int ... plainDocNums) {
    return Arrays.stream(plainDocNums).map((docNum)-> docNum*3+2).toArray();
  }

  private void addParent(RandomIndexWriter w, Document parentDoc, Field ... childFields) throws IOException {
    final List<Iterable<? extends IndexableField>> block = new ArrayList<>();
    for (Field childField : childFields) {
      final Document child = new Document();
      child.add(childField);
      child.add(new StringField("type", "child", Store.NO));
      block.add(child);
    }
    parentDoc.add(new StringField("type", "parent", Store.NO));
    block.add(parentDoc);
    w.addDocuments(block);
  }

  private boolean arrayContains(int[] expected, int[] actual) {
    Arrays.sort(actual); // in some cases the actual docs aren't sorted by docid. This method expects that.
    if (expected.length != actual.length) {
      return false;
    }

    for (int e : expected) {
      boolean found = false;
      for (int a : actual) {
        if (e == a) {
          found = true;
          break;
        }
      }

      if (!found) {
        return false;
      }
    }

    return true;
  }

  private boolean openBitSetContains(int[] expectedDocs, Bits actual, int maxDoc) throws IOException {
    assert actual instanceof FixedBitSet;
    if (expectedDocs.length != ((FixedBitSet)actual).cardinality()) {
      return false;
    }

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int expectedDoc : expectedDocs) {
      expected.set(expectedDoc);
    }

    for (int docId = expected.nextSetBit(0); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docId + 1 >= expected.length() ? DocIdSetIterator.NO_MORE_DOCS : expected.nextSetBit(docId + 1)) {
      if (!actual.get(docId)) {
        return false;
      }
    }

    return true;
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  private AllGroupHeadsCollector<?> createRandomCollector(String groupField, Sort sortWithinGroup) {
    if (random().nextBoolean()) {
      ValueSource vs = new BytesRefFieldSource(groupField);
      return AllGroupHeadsCollector.newCollector(new ValueSourceGroupSelector(vs, new HashMap<>()), sortWithinGroup);
    } else {
      return AllGroupHeadsCollector.newCollector(new TermGroupSelector(groupField), sortWithinGroup);
    }
  }

  private void addGroupField(Document doc, String groupField, String value, DocValuesType valueType) {
    Field valuesField = null;
    switch(valueType) {
      case BINARY:
        valuesField = new BinaryDocValuesField(groupField, new BytesRef(value));
        break;
      case SORTED:
        valuesField = new SortedDocValuesField(groupField, new BytesRef(value));
        break;
      default:
        fail("unhandled type");
    }
    doc.add(valuesField);
  }
}
