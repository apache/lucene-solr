package org.apache.lucene.search.grouping;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowMultiReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.dv.DVAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.function.FunctionAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupHeadsCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

import java.io.IOException;
import java.util.*;

public class AllGroupHeadsCollectorTest extends LuceneTestCase {

  private static final Type[] vts = new Type[]{
      Type.BYTES_VAR_DEREF, Type.BYTES_VAR_STRAIGHT, Type.BYTES_VAR_SORTED
  };

  public void testBasic() throws Exception {
    final String groupField = "author";
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random,
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT,
            new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    boolean canUseIDV = !"Lucene3x".equals(w.w.getConfig().getCodec().getName());
    Type valueType = vts[random.nextInt(vts.length)];

    // 0
    Document doc = new Document();
    addGroupField(doc, groupField, "author1", canUseIDV, valueType);
    doc.add(newField("content", "random text", TextField.TYPE_STORED));
    doc.add(newField("id", "1", StringField.TYPE_STORED));
    w.addDocument(doc);

    // 1
    doc = new Document();
    addGroupField(doc, groupField, "author1", canUseIDV, valueType);
    doc.add(newField("content", "some more random text blob", TextField.TYPE_STORED));
    doc.add(newField("id", "2", StringField.TYPE_STORED));
    w.addDocument(doc);

    // 2
    doc = new Document();
    addGroupField(doc, groupField, "author1", canUseIDV, valueType);
    doc.add(newField("content", "some more random textual data", TextField.TYPE_STORED));
    doc.add(newField("id", "3", StringField.TYPE_STORED));
    w.addDocument(doc);
    w.commit(); // To ensure a second segment

    // 3
    doc = new Document();
    addGroupField(doc, groupField, "author2", canUseIDV, valueType);
    doc.add(newField("content", "some random text", TextField.TYPE_STORED));
    doc.add(newField("id", "4", StringField.TYPE_STORED));
    w.addDocument(doc);

    // 4
    doc = new Document();
    addGroupField(doc, groupField, "author3", canUseIDV, valueType);
    doc.add(newField("content", "some more random text", TextField.TYPE_STORED));
    doc.add(newField("id", "5", StringField.TYPE_STORED));
    w.addDocument(doc);

    // 5
    doc = new Document();
    addGroupField(doc, groupField, "author3", canUseIDV, valueType);
    doc.add(newField("content", "random blob", TextField.TYPE_STORED));
    doc.add(newField("id", "6", StringField.TYPE_STORED));
    w.addDocument(doc);

    // 6 -- no author field
    doc = new Document();
    doc.add(newField("content", "random word stuck in alot of other text", TextField.TYPE_STORED));
    doc.add(newField("id", "6", StringField.TYPE_STORED));
    w.addDocument(doc);

    // 7 -- no author field
    doc = new Document();
    doc.add(newField("content", "random word stuck in alot of other text", TextField.TYPE_STORED));
    doc.add(newField("id", "7", StringField.TYPE_STORED));
    w.addDocument(doc);

    IndexReader reader = w.getReader();
    IndexSearcher indexSearcher = new IndexSearcher(reader);
    if (SlowMultiReaderWrapper.class.isAssignableFrom(reader.getClass())) {
      canUseIDV = false;
    }

    w.close();
    int maxDoc = reader.maxDoc();

    Sort sortWithinGroup = new Sort(new SortField("id", SortField.Type.INT, true));
    AbstractAllGroupHeadsCollector c1 = createRandomCollector(groupField, sortWithinGroup, canUseIDV, valueType);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c1);
    assertTrue(arrayContains(new int[]{2, 3, 5, 7}, c1.retrieveGroupHeads()));
    assertTrue(openBitSetContains(new int[]{2, 3, 5, 7}, c1.retrieveGroupHeads(maxDoc), maxDoc));

    AbstractAllGroupHeadsCollector c2 = createRandomCollector(groupField, sortWithinGroup, canUseIDV, valueType);
    indexSearcher.search(new TermQuery(new Term("content", "some")), c2);
    assertTrue(arrayContains(new int[]{2, 3, 4}, c2.retrieveGroupHeads()));
    assertTrue(openBitSetContains(new int[]{2, 3, 4}, c2.retrieveGroupHeads(maxDoc), maxDoc));

    AbstractAllGroupHeadsCollector c3 = createRandomCollector(groupField, sortWithinGroup, canUseIDV, valueType);
    indexSearcher.search(new TermQuery(new Term("content", "blob")), c3);
    assertTrue(arrayContains(new int[]{1, 5}, c3.retrieveGroupHeads()));
    assertTrue(openBitSetContains(new int[]{1, 5}, c3.retrieveGroupHeads(maxDoc), maxDoc));

    // STRING sort type triggers different implementation
    Sort sortWithinGroup2 = new Sort(new SortField("id", SortField.Type.STRING, true));
    AbstractAllGroupHeadsCollector c4 = createRandomCollector(groupField, sortWithinGroup2, canUseIDV, valueType);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c4);
    assertTrue(arrayContains(new int[]{2, 3, 5, 7}, c4.retrieveGroupHeads()));
    assertTrue(openBitSetContains(new int[]{2, 3, 5, 7}, c4.retrieveGroupHeads(maxDoc), maxDoc));

    Sort sortWithinGroup3 = new Sort(new SortField("id", SortField.Type.STRING, false));
    AbstractAllGroupHeadsCollector c5 = createRandomCollector(groupField, sortWithinGroup3, canUseIDV, valueType);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c5);
    // 7 b/c higher doc id wins, even if order of field is in not in reverse.
    assertTrue(arrayContains(new int[]{0, 3, 4, 6}, c5.retrieveGroupHeads()));
    assertTrue(openBitSetContains(new int[]{0, 3, 4, 6}, c5.retrieveGroupHeads(maxDoc), maxDoc));

    indexSearcher.getIndexReader().close();
    dir.close();
  }

  public void testRandom() throws Exception {
    int numberOfRuns = _TestUtil.nextInt(random, 3, 6);
    for (int iter = 0; iter < numberOfRuns; iter++) {
      if (VERBOSE) {
        System.out.println(String.format("TEST: iter=%d total=%d", iter, numberOfRuns));
      }

      final int numDocs = _TestUtil.nextInt(random, 100, 1000) * RANDOM_MULTIPLIER;
      final int numGroups = _TestUtil.nextInt(random, 1, numDocs);

      if (VERBOSE) {
        System.out.println("TEST: numDocs=" + numDocs + " numGroups=" + numGroups);
      }

      final List<BytesRef> groups = new ArrayList<BytesRef>();
      for (int i = 0; i < numGroups; i++) {
        String randomValue;
        do {
          // B/c of DV based impl we can't see the difference between an empty string and a null value.
          // For that reason we don't generate empty string groups.
          randomValue = _TestUtil.randomRealisticUnicodeString(random);
        } while ("".equals(randomValue));
        groups.add(new BytesRef(randomValue));
      }
      final String[] contentStrings = new String[_TestUtil.nextInt(random, 2, 20)];
      if (VERBOSE) {
        System.out.println("TEST: create fake content");
      }
      for (int contentIDX = 0; contentIDX < contentStrings.length; contentIDX++) {
        final StringBuilder sb = new StringBuilder();
        sb.append("real").append(random.nextInt(3)).append(' ');
        final int fakeCount = random.nextInt(10);
        for (int fakeIDX = 0; fakeIDX < fakeCount; fakeIDX++) {
          sb.append("fake ");
        }
        contentStrings[contentIDX] = sb.toString();
        if (VERBOSE) {
          System.out.println("  content=" + sb.toString());
        }
      }

      Directory dir = newDirectory();
      RandomIndexWriter w = new RandomIndexWriter(
          random,
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT,
              new MockAnalyzer(random)));
      boolean preFlex = "Lucene3x".equals(w.w.getConfig().getCodec().getName());
      boolean canUseIDV = !preFlex;
      Type valueType = vts[random.nextInt(vts.length)];

      Document doc = new Document();
      Document docNoGroup = new Document();
      Field group = newField("group", "", StringField.TYPE_UNSTORED);
      doc.add(group);
      DocValuesField valuesField = null;
      if (canUseIDV) {
        valuesField = new DocValuesField("group", valueType);
        doc.add(valuesField);
      }
      Field sort1 = newField("sort1", "", StringField.TYPE_UNSTORED);
      doc.add(sort1);
      docNoGroup.add(sort1);
      Field sort2 = newField("sort2", "", StringField.TYPE_UNSTORED);
      doc.add(sort2);
      docNoGroup.add(sort2);
      Field sort3 = newField("sort3", "", StringField.TYPE_UNSTORED);
      doc.add(sort3);
      docNoGroup.add(sort3);
      Field content = newField("content", "", TextField.TYPE_UNSTORED);
      doc.add(content);
      docNoGroup.add(content);
      NumericField id = new NumericField("id");
      doc.add(id);
      docNoGroup.add(id);
      final GroupDoc[] groupDocs = new GroupDoc[numDocs];
      for (int i = 0; i < numDocs; i++) {
        final BytesRef groupValue;
        if (random.nextInt(24) == 17) {
          // So we test the "doc doesn't have the group'd
          // field" case:
          groupValue = null;
        } else {
          groupValue = groups.get(random.nextInt(groups.size()));
        }

        final GroupDoc groupDoc = new GroupDoc(
            i,
            groupValue,
            groups.get(random.nextInt(groups.size())),
            groups.get(random.nextInt(groups.size())),
            new BytesRef(String.format("%05d", i)),
            contentStrings[random.nextInt(contentStrings.length)]
        );

        if (VERBOSE) {
          System.out.println("  doc content=" + groupDoc.content + " id=" + i + " group=" + (groupDoc.group == null ? "null" : groupDoc.group.utf8ToString()) + " sort1=" + groupDoc.sort1.utf8ToString() + " sort2=" + groupDoc.sort2.utf8ToString() + " sort3=" + groupDoc.sort3.utf8ToString());
        }

        groupDocs[i] = groupDoc;
        if (groupDoc.group != null) {
          group.setValue(groupDoc.group.utf8ToString());
          if (canUseIDV) {
            valuesField.setBytes(new BytesRef(groupDoc.group.utf8ToString()));
          }
        }
        sort1.setValue(groupDoc.sort1.utf8ToString());
        sort2.setValue(groupDoc.sort2.utf8ToString());
        sort3.setValue(groupDoc.sort3.utf8ToString());
        content.setValue(groupDoc.content);
        id.setIntValue(groupDoc.id);
        if (groupDoc.group == null) {
          w.addDocument(docNoGroup);
        } else {
          w.addDocument(doc);
        }
      }

      final IndexReader r = w.getReader();
      w.close();

      // NOTE: intentional but temporary field cache insanity!
      final int[] docIdToFieldId = FieldCache.DEFAULT.getInts(r, "id", false);
      final int[] fieldIdToDocID = new int[numDocs];
      for (int i = 0; i < docIdToFieldId.length; i++) {
        int fieldId = docIdToFieldId[i];
        fieldIdToDocID[fieldId] = i;
      }

      try {
        final IndexSearcher s = newSearcher(r);
        if (SlowMultiReaderWrapper.class.isAssignableFrom(s.getIndexReader().getClass())) {
          canUseIDV = false;
        } else {
          canUseIDV = !preFlex;
        }

        for (int contentID = 0; contentID < 3; contentID++) {
          final ScoreDoc[] hits = s.search(new TermQuery(new Term("content", "real" + contentID)), numDocs).scoreDocs;
          for (ScoreDoc hit : hits) {
            final GroupDoc gd = groupDocs[docIdToFieldId[hit.doc]];
            assertTrue(gd.score == 0.0);
            gd.score = hit.score;
            int docId = gd.id;
            assertEquals(docId, docIdToFieldId[hit.doc]);
          }
        }

        for (GroupDoc gd : groupDocs) {
          assertTrue(gd.score != 0.0);
        }

        for (int searchIter = 0; searchIter < 100; searchIter++) {

          if (VERBOSE) {
            System.out.println("TEST: searchIter=" + searchIter);
          }

          final String searchTerm = "real" + random.nextInt(3);
          boolean sortByScoreOnly = random.nextBoolean();
          Sort sortWithinGroup = getRandomSort(sortByScoreOnly);
          AbstractAllGroupHeadsCollector allGroupHeadsCollector = createRandomCollector("group", sortWithinGroup, canUseIDV, valueType);
          s.search(new TermQuery(new Term("content", searchTerm)), allGroupHeadsCollector);
          int[] expectedGroupHeads = createExpectedGroupHeads(searchTerm, groupDocs, sortWithinGroup, sortByScoreOnly, fieldIdToDocID);
          int[] actualGroupHeads = allGroupHeadsCollector.retrieveGroupHeads();
          // The actual group heads contains Lucene ids. Need to change them into our id value.
          for (int i = 0; i < actualGroupHeads.length; i++) {
            actualGroupHeads[i] = docIdToFieldId[actualGroupHeads[i]];
          }
          // Allows us the easily iterate and assert the actual and expected results.
          Arrays.sort(expectedGroupHeads);
          Arrays.sort(actualGroupHeads);

          if (VERBOSE) {
            System.out.println("Collector: " + allGroupHeadsCollector.getClass().getSimpleName());
            System.out.println("Sort within group: " + sortWithinGroup);
            System.out.println("Num group: " + numGroups);
            System.out.println("Num doc: " + numDocs);
            System.out.println("\n=== Expected: \n");
            for (int expectedDocId : expectedGroupHeads) {
              GroupDoc expectedGroupDoc = groupDocs[expectedDocId];
              String expectedGroup = expectedGroupDoc.group == null ? null : expectedGroupDoc.group.utf8ToString();
              System.out.println(
                  String.format(
                      "Group:%10s score%5f Sort1:%10s Sort2:%10s Sort3:%10s doc:%5d",
                      expectedGroup, expectedGroupDoc.score, expectedGroupDoc.sort1.utf8ToString(),
                      expectedGroupDoc.sort2.utf8ToString(), expectedGroupDoc.sort3.utf8ToString(), expectedDocId
                  )
              );
            }
            System.out.println("\n=== Actual: \n");
            for (int actualDocId : actualGroupHeads) {
              GroupDoc actualGroupDoc = groupDocs[actualDocId];
              String actualGroup = actualGroupDoc.group == null ? null : actualGroupDoc.group.utf8ToString();
              System.out.println(
                  String.format(
                      "Group:%10s score%5f Sort1:%10s Sort2:%10s Sort3:%10s doc:%5d",
                      actualGroup, actualGroupDoc.score, actualGroupDoc.sort1.utf8ToString(),
                      actualGroupDoc.sort2.utf8ToString(), actualGroupDoc.sort3.utf8ToString(), actualDocId
                  )
              );
            }
            System.out.println("\n===================================================================================");
          }

          assertEquals(expectedGroupHeads.length, actualGroupHeads.length);
          for (int i = 0; i < expectedGroupHeads.length; i++) {
            assertEquals(expectedGroupHeads[i], actualGroupHeads[i]);
          }
        }
      } finally {
        FieldCache.DEFAULT.purge(r);
      }

      r.close();
      dir.close();
    }
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
        }
      }

      if (!found) {
        return false;
      }
    }

    return true;
  }

  private boolean openBitSetContains(int[] expectedDocs, FixedBitSet actual, int maxDoc) throws IOException {
    if (expectedDocs.length != actual.cardinality()) {
      return false;
    }

    FixedBitSet expected = new FixedBitSet(maxDoc);
    for (int expectedDoc : expectedDocs) {
      expected.set(expectedDoc);
    }

    int docId;
    DocIdSetIterator iterator = expected.iterator();
    while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      if (!actual.get(docId)) {
        return false;
      }
    }

    return true;
  }

  private int[] createExpectedGroupHeads(String searchTerm, GroupDoc[] groupDocs, Sort docSort, boolean sortByScoreOnly, int[] fieldIdToDocID) throws IOException {
    Map<BytesRef, List<GroupDoc>> groupHeads = new HashMap<BytesRef, List<GroupDoc>>();
    for (GroupDoc groupDoc : groupDocs) {
      if (!groupDoc.content.startsWith(searchTerm)) {
        continue;
      }

      if (!groupHeads.containsKey(groupDoc.group)) {
        List<GroupDoc> list = new ArrayList<GroupDoc>();
        list.add(groupDoc);
        groupHeads.put(groupDoc.group, list);
        continue;
      }
      groupHeads.get(groupDoc.group).add(groupDoc);
    }

    int[] allGroupHeads = new int[groupHeads.size()];
    int i = 0;
    for (BytesRef groupValue : groupHeads.keySet()) {
      List<GroupDoc> docs = groupHeads.get(groupValue);
      Collections.sort(docs, getComparator(docSort, sortByScoreOnly, fieldIdToDocID));
      allGroupHeads[i++] = docs.get(0).id;
    }

    return allGroupHeads;
  }

  private Sort getRandomSort(boolean scoreOnly) {
    final List<SortField> sortFields = new ArrayList<SortField>();
    if (random.nextInt(7) == 2 || scoreOnly) {
      sortFields.add(SortField.FIELD_SCORE);
    } else {
      if (random.nextBoolean()) {
        if (random.nextBoolean()) {
          sortFields.add(new SortField("sort1", SortField.Type.STRING, random.nextBoolean()));
        } else {
          sortFields.add(new SortField("sort2", SortField.Type.STRING, random.nextBoolean()));
        }
      } else if (random.nextBoolean()) {
        sortFields.add(new SortField("sort1", SortField.Type.STRING, random.nextBoolean()));
        sortFields.add(new SortField("sort2", SortField.Type.STRING, random.nextBoolean()));
      }
    }
    // Break ties:
    if (random.nextBoolean() && !scoreOnly) {
      sortFields.add(new SortField("sort3", SortField.Type.STRING));
    } else if (!scoreOnly) {
      sortFields.add(new SortField("id", SortField.Type.INT));
    }
    return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
  }

  private Comparator<GroupDoc> getComparator(Sort sort, final boolean sortByScoreOnly, final int[] fieldIdToDocID) {
    final SortField[] sortFields = sort.getSort();
    return new Comparator<GroupDoc>() {
      @Override
      public int compare(GroupDoc d1, GroupDoc d2) {
        for (SortField sf : sortFields) {
          final int cmp;
          if (sf.getType() == SortField.Type.SCORE) {
            if (d1.score > d2.score) {
              cmp = -1;
            } else if (d1.score < d2.score) {
              cmp = 1;
            } else {
              cmp = sortByScoreOnly ? fieldIdToDocID[d1.id] - fieldIdToDocID[d2.id] : 0;
            }
          } else if (sf.getField().equals("sort1")) {
            cmp = d1.sort1.compareTo(d2.sort1);
          } else if (sf.getField().equals("sort2")) {
            cmp = d1.sort2.compareTo(d2.sort2);
          } else if (sf.getField().equals("sort3")) {
            cmp = d1.sort3.compareTo(d2.sort3);
          } else {
            assertEquals(sf.getField(), "id");
            cmp = d1.id - d2.id;
          }
          if (cmp != 0) {
            return sf.getReverse() ? -cmp : cmp;
          }
        }
        // Our sort always fully tie breaks:
        fail();
        return 0;
      }
    };
  }

  private AbstractAllGroupHeadsCollector createRandomCollector(String groupField, Sort sortWithinGroup, boolean canUseIDV, Type valueType) throws IOException {
    AbstractAllGroupHeadsCollector collector;
    if (random.nextBoolean()) {
      ValueSource vs = new BytesRefFieldSource(groupField);
      collector =  new FunctionAllGroupHeadsCollector(vs, new HashMap(), sortWithinGroup);
    } else if (canUseIDV && random.nextBoolean()) {
      boolean diskResident = random.nextBoolean();
      collector =  DVAllGroupHeadsCollector.create(groupField, sortWithinGroup, valueType, diskResident);
    } else {
      collector =  TermAllGroupHeadsCollector.create(groupField, sortWithinGroup);
    }

    if (VERBOSE) {
      System.out.println("Selected implementation: " + collector.getClass().getSimpleName());
    }

    return collector;
  }

  private void addGroupField(Document doc, String groupField, String value, boolean canUseIDV, Type valueType) {
    doc.add(new Field(groupField, value, TextField.TYPE_STORED));
    if (canUseIDV) {
      DocValuesField valuesField = new DocValuesField(groupField, valueType);
      valuesField.setBytes(new BytesRef(value));
      doc.add(valuesField);
    }
  }

  private static class GroupDoc {
    final int id;
    final BytesRef group;
    final BytesRef sort1;
    final BytesRef sort2;
    final BytesRef sort3;
    // content must be "realN ..."
    final String content;
    float score;

    public GroupDoc(int id, BytesRef group, BytesRef sort1, BytesRef sort2, BytesRef sort3, String content) {
      this.id = id;
      this.group = group;
      this.sort1 = sort1;
      this.sort2 = sort2;
      this.sort3 = sort3;
      this.content = content;
    }

  }

}
