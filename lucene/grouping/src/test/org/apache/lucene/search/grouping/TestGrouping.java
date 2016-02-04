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
package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.function.FunctionAllGroupsCollector;
import org.apache.lucene.search.grouping.function.FunctionFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.function.FunctionSecondPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermSecondPassGroupingCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;

// TODO
//   - should test relevance sort too
//   - test null
//   - test ties
//   - test compound sort

public class TestGrouping extends LuceneTestCase {

  public void testBasic() throws Exception {

    String groupField = "author";

    FieldType customType = new FieldType();
    customType.setStored(true);

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
                               random(),
                               dir,
                               newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    // 0
    Document doc = new Document();
    addGroupField(doc, groupField, "author1");
    doc.add(new TextField("content", "random text", Field.Store.YES));
    doc.add(new Field("id", "1", customType));
    w.addDocument(doc);

    // 1
    doc = new Document();
    addGroupField(doc, groupField, "author1");
    doc.add(new TextField("content", "some more random text", Field.Store.YES));
    doc.add(new Field("id", "2", customType));
    w.addDocument(doc);

    // 2
    doc = new Document();
    addGroupField(doc, groupField, "author1");
    doc.add(new TextField("content", "some more random textual data", Field.Store.YES));
    doc.add(new Field("id", "3", customType));
    w.addDocument(doc);

    // 3
    doc = new Document();
    addGroupField(doc, groupField, "author2");
    doc.add(new TextField("content", "some random text", Field.Store.YES));
    doc.add(new Field("id", "4", customType));
    w.addDocument(doc);

    // 4
    doc = new Document();
    addGroupField(doc, groupField, "author3");
    doc.add(new TextField("content", "some more random text", Field.Store.YES));
    doc.add(new Field("id", "5", customType));
    w.addDocument(doc);

    // 5
    doc = new Document();
    addGroupField(doc, groupField, "author3");
    doc.add(new TextField("content", "random", Field.Store.YES));
    doc.add(new Field("id", "6", customType));
    w.addDocument(doc);

    // 6 -- no author field
    doc = new Document();
    doc.add(new TextField("content", "random word stuck in alot of other text", Field.Store.YES));
    doc.add(new Field("id", "6", customType));
    w.addDocument(doc);

    IndexSearcher indexSearcher = newSearcher(w.getReader());
    w.close();

    final Sort groupSort = Sort.RELEVANCE;

    final AbstractFirstPassGroupingCollector<?> c1 = createRandomFirstPassCollector(groupField, groupSort, 10);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c1);

    final AbstractSecondPassGroupingCollector<?> c2 = createSecondPassCollector(c1, groupField, groupSort, Sort.RELEVANCE, 0, 5, true, true, true);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c2);

    final TopGroups<?> groups = c2.getTopGroups(0);
    assertFalse(Float.isNaN(groups.maxScore));

    assertEquals(7, groups.totalHitCount);
    assertEquals(7, groups.totalGroupedHitCount);
    assertEquals(4, groups.groups.length);

    // relevance order: 5, 0, 3, 4, 1, 2, 6

    // the later a document is added the higher this docId
    // value
    GroupDocs<?> group = groups.groups[0];
    compareGroupValue("author3", group);
    assertEquals(2, group.scoreDocs.length);
    assertEquals(5, group.scoreDocs[0].doc);
    assertEquals(4, group.scoreDocs[1].doc);
    assertTrue(group.scoreDocs[0].score > group.scoreDocs[1].score);

    group = groups.groups[1];
    compareGroupValue("author1", group);
    assertEquals(3, group.scoreDocs.length);
    assertEquals(0, group.scoreDocs[0].doc);
    assertEquals(1, group.scoreDocs[1].doc);
    assertEquals(2, group.scoreDocs[2].doc);
    assertTrue(group.scoreDocs[0].score >= group.scoreDocs[1].score);
    assertTrue(group.scoreDocs[1].score >= group.scoreDocs[2].score);

    group = groups.groups[2];
    compareGroupValue("author2", group);
    assertEquals(1, group.scoreDocs.length);
    assertEquals(3, group.scoreDocs[0].doc);

    group = groups.groups[3];
    compareGroupValue(null, group);
    assertEquals(1, group.scoreDocs.length);
    assertEquals(6, group.scoreDocs[0].doc);

    indexSearcher.getIndexReader().close();
    dir.close();
  }

  private void addGroupField(Document doc, String groupField, String value) {
    doc.add(new SortedDocValuesField(groupField, new BytesRef(value)));
  }

  private AbstractFirstPassGroupingCollector<?> createRandomFirstPassCollector(String groupField, Sort groupSort, int topDocs) throws IOException {
    AbstractFirstPassGroupingCollector<?> selected;
    if (random().nextBoolean()) {
      ValueSource vs = new BytesRefFieldSource(groupField);
      selected = new FunctionFirstPassGroupingCollector(vs, new HashMap<>(), groupSort, topDocs);
    } else {
      selected = new TermFirstPassGroupingCollector(groupField, groupSort, topDocs);
    }
    if (VERBOSE) {
      System.out.println("Selected implementation: " + selected.getClass().getName());
    }
    return selected;
  }

  private AbstractFirstPassGroupingCollector<?> createFirstPassCollector(String groupField, Sort groupSort, int topDocs, AbstractFirstPassGroupingCollector<?> firstPassGroupingCollector) throws IOException {
    if (TermFirstPassGroupingCollector.class.isAssignableFrom(firstPassGroupingCollector.getClass())) {
      ValueSource vs = new BytesRefFieldSource(groupField);
      return new FunctionFirstPassGroupingCollector(vs, new HashMap<>(), groupSort, topDocs);
    } else {
      return new TermFirstPassGroupingCollector(groupField, groupSort, topDocs);
    }
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  private <T> AbstractSecondPassGroupingCollector<T> createSecondPassCollector(AbstractFirstPassGroupingCollector firstPassGroupingCollector,
                                                                        String groupField,
                                                                        Sort groupSort,
                                                                        Sort sortWithinGroup,
                                                                        int groupOffset,
                                                                        int maxDocsPerGroup,
                                                                        boolean getScores,
                                                                        boolean getMaxScores,
                                                                        boolean fillSortFields) throws IOException {

    if (TermFirstPassGroupingCollector.class.isAssignableFrom(firstPassGroupingCollector.getClass())) {
      Collection<SearchGroup<BytesRef>> searchGroups = firstPassGroupingCollector.getTopGroups(groupOffset, fillSortFields);
      return (AbstractSecondPassGroupingCollector) new TermSecondPassGroupingCollector(groupField, searchGroups, groupSort, sortWithinGroup, maxDocsPerGroup , getScores, getMaxScores, fillSortFields);
    } else {
      ValueSource vs = new BytesRefFieldSource(groupField);
      Collection<SearchGroup<MutableValue>> searchGroups = firstPassGroupingCollector.getTopGroups(groupOffset, fillSortFields);
      return (AbstractSecondPassGroupingCollector) new FunctionSecondPassGroupingCollector(searchGroups, groupSort, sortWithinGroup, maxDocsPerGroup, getScores, getMaxScores, fillSortFields, vs, new HashMap());
    }
  }

  // Basically converts searchGroups from MutableValue to BytesRef if grouping by ValueSource
  @SuppressWarnings("unchecked")
  private AbstractSecondPassGroupingCollector<?> createSecondPassCollector(AbstractFirstPassGroupingCollector<?> firstPassGroupingCollector,
                                                                        String groupField,
                                                                        Collection<SearchGroup<BytesRef>> searchGroups,
                                                                        Sort groupSort,
                                                                        Sort sortWithinGroup,
                                                                        int maxDocsPerGroup,
                                                                        boolean getScores,
                                                                        boolean getMaxScores,
                                                                        boolean fillSortFields) throws IOException {
    if (firstPassGroupingCollector.getClass().isAssignableFrom(TermFirstPassGroupingCollector.class)) {
      return new TermSecondPassGroupingCollector(groupField, searchGroups, groupSort, sortWithinGroup, maxDocsPerGroup , getScores, getMaxScores, fillSortFields);
    } else {
      ValueSource vs = new BytesRefFieldSource(groupField);
      List<SearchGroup<MutableValue>> mvalSearchGroups = new ArrayList<>(searchGroups.size());
      for (SearchGroup<BytesRef> mergedTopGroup : searchGroups) {
        SearchGroup<MutableValue> sg = new SearchGroup<>();
        MutableValueStr groupValue = new MutableValueStr();
        if (mergedTopGroup.groupValue != null) {
          groupValue.value.copyBytes(mergedTopGroup.groupValue);
        } else {
          groupValue.exists = false;
        }
        sg.groupValue = groupValue;
        sg.sortValues = mergedTopGroup.sortValues;
        mvalSearchGroups.add(sg);
      }

      return new FunctionSecondPassGroupingCollector(mvalSearchGroups, groupSort, sortWithinGroup, maxDocsPerGroup, getScores, getMaxScores, fillSortFields, vs, new HashMap<>());
    }
  }

  private AbstractAllGroupsCollector<?> createAllGroupsCollector(AbstractFirstPassGroupingCollector<?> firstPassGroupingCollector,
                                                              String groupField) {
    if (firstPassGroupingCollector.getClass().isAssignableFrom(TermFirstPassGroupingCollector.class)) {
      return new TermAllGroupsCollector(groupField);
    } else {
      ValueSource vs = new BytesRefFieldSource(groupField);
      return new FunctionAllGroupsCollector(vs, new HashMap<>());
    }
  }

  private void compareGroupValue(String expected, GroupDocs<?> group) {
    if (expected == null) {
      if (group.groupValue == null) {
        return;
      } else if (group.groupValue.getClass().isAssignableFrom(MutableValueStr.class)) {
        return;
      } else if (((BytesRef) group.groupValue).length == 0) {
        return;
      }
      fail();
    }

    if (group.groupValue.getClass().isAssignableFrom(BytesRef.class)) {
      assertEquals(new BytesRef(expected), group.groupValue);
    } else if (group.groupValue.getClass().isAssignableFrom(MutableValueStr.class)) {
      MutableValueStr v = new MutableValueStr();
      v.value.copyChars(expected);
      assertEquals(v, group.groupValue);
    } else {
      fail();
    }
  }

  private Collection<SearchGroup<BytesRef>> getSearchGroups(AbstractFirstPassGroupingCollector<?> c, int groupOffset, boolean fillFields) {
    if (TermFirstPassGroupingCollector.class.isAssignableFrom(c.getClass())) {
      return ((TermFirstPassGroupingCollector) c).getTopGroups(groupOffset, fillFields);
    } else if (FunctionFirstPassGroupingCollector.class.isAssignableFrom(c.getClass())) {
      Collection<SearchGroup<MutableValue>> mutableValueGroups = ((FunctionFirstPassGroupingCollector) c).getTopGroups(groupOffset, fillFields);
      if (mutableValueGroups == null) {
        return null;
      }

      List<SearchGroup<BytesRef>> groups = new ArrayList<>(mutableValueGroups.size());
      for (SearchGroup<MutableValue> mutableValueGroup : mutableValueGroups) {
        SearchGroup<BytesRef> sg = new SearchGroup<>();
        sg.groupValue = mutableValueGroup.groupValue.exists() ? ((MutableValueStr) mutableValueGroup.groupValue).value.get() : null;
        sg.sortValues = mutableValueGroup.sortValues;
        groups.add(sg);
      }
      return groups;
    }
    fail();
    return null;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private TopGroups<BytesRef> getTopGroups(AbstractSecondPassGroupingCollector c, int withinGroupOffset) {
    if (c.getClass().isAssignableFrom(TermSecondPassGroupingCollector.class)) {
      return ((TermSecondPassGroupingCollector) c).getTopGroups(withinGroupOffset);
    } else if (c.getClass().isAssignableFrom(FunctionSecondPassGroupingCollector.class)) {
      TopGroups<MutableValue> mvalTopGroups = ((FunctionSecondPassGroupingCollector) c).getTopGroups(withinGroupOffset);
      List<GroupDocs<BytesRef>> groups = new ArrayList<>(mvalTopGroups.groups.length);
      for (GroupDocs<MutableValue> mvalGd : mvalTopGroups.groups) {
        BytesRef groupValue = mvalGd.groupValue.exists() ? ((MutableValueStr) mvalGd.groupValue).value.get() : null;
        groups.add(new GroupDocs<>(Float.NaN, mvalGd.maxScore, mvalGd.totalHits, mvalGd.scoreDocs, groupValue, mvalGd.groupSortValues));
      }
      return new TopGroups<>(mvalTopGroups.groupSort, mvalTopGroups.withinGroupSort, mvalTopGroups.totalHitCount, mvalTopGroups.totalGroupedHitCount, groups.toArray(new GroupDocs[groups.size()]), Float.NaN);
    }
    fail();
    return null;
  }

  private static class GroupDoc {
    final int id;
    final BytesRef group;
    final BytesRef sort1;
    final BytesRef sort2;
    // content must be "realN ..."
    final String content;
    float score;
    float score2;

    public GroupDoc(int id, BytesRef group, BytesRef sort1, BytesRef sort2, String content) {
      this.id = id;
      this.group = group;
      this.sort1 = sort1;
      this.sort2 = sort2;
      this.content = content;
    }
  }

  private Sort getRandomSort() {
    final List<SortField> sortFields = new ArrayList<>();
    if (random().nextInt(7) == 2) {
      sortFields.add(SortField.FIELD_SCORE);
    } else {
      if (random().nextBoolean()) {
        if (random().nextBoolean()) {
          sortFields.add(new SortField("sort1", SortField.Type.STRING, random().nextBoolean()));
        } else {
          sortFields.add(new SortField("sort2", SortField.Type.STRING, random().nextBoolean()));
        }
      } else if (random().nextBoolean()) {
        sortFields.add(new SortField("sort1", SortField.Type.STRING, random().nextBoolean()));
        sortFields.add(new SortField("sort2", SortField.Type.STRING, random().nextBoolean()));
      }
    }
    // Break ties:
    sortFields.add(new SortField("id", SortField.Type.INT));
    return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
  }

  private Comparator<GroupDoc> getComparator(Sort sort) {
    final SortField[] sortFields = sort.getSort();
    return new Comparator<GroupDoc>() {
      @Override
      public int compare(GroupDoc d1, GroupDoc d2) {
        for(SortField sf : sortFields) {
          final int cmp;
          if (sf.getType() == SortField.Type.SCORE) {
            if (d1.score > d2.score) {
              cmp = -1;
            } else if (d1.score < d2.score) {
              cmp = 1;
            } else {
              cmp = 0;
            }
          } else if (sf.getField().equals("sort1")) {
            cmp = d1.sort1.compareTo(d2.sort1);
          } else if (sf.getField().equals("sort2")) {
            cmp = d1.sort2.compareTo(d2.sort2);
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

  @SuppressWarnings({"unchecked","rawtypes"})
  private Comparable<?>[] fillFields(GroupDoc d, Sort sort) {
    final SortField[] sortFields = sort.getSort();
    final Comparable<?>[] fields = new Comparable[sortFields.length];
    for(int fieldIDX=0;fieldIDX<sortFields.length;fieldIDX++) {
      final Comparable<?> c;
      final SortField sf = sortFields[fieldIDX];
      if (sf.getType() == SortField.Type.SCORE) {
        c = d.score;
      } else if (sf.getField().equals("sort1")) {
        c = d.sort1;
      } else if (sf.getField().equals("sort2")) {
        c = d.sort2;
      } else {
        assertEquals("id", sf.getField());
        c = d.id;
      }
      fields[fieldIDX] = c;
    }
    return fields;
  }

  private String groupToString(BytesRef b) {
    if (b == null) {
      return "null";
    } else {
      return b.utf8ToString();
    }
  }

  private TopGroups<BytesRef> slowGrouping(GroupDoc[] groupDocs,
                                           String searchTerm,
                                           boolean fillFields,
                                           boolean getScores,
                                           boolean getMaxScores,
                                           boolean doAllGroups,
                                           Sort groupSort,
                                           Sort docSort,
                                           int topNGroups,
                                           int docsPerGroup,
                                           int groupOffset,
                                           int docOffset) {

    final Comparator<GroupDoc> groupSortComp = getComparator(groupSort);

    Arrays.sort(groupDocs, groupSortComp);
    final HashMap<BytesRef,List<GroupDoc>> groups = new HashMap<>();
    final List<BytesRef> sortedGroups = new ArrayList<>();
    final List<Comparable<?>[]> sortedGroupFields = new ArrayList<>();

    int totalHitCount = 0;
    Set<BytesRef> knownGroups = new HashSet<>();

    //System.out.println("TEST: slowGrouping");
    for(GroupDoc d : groupDocs) {
      // TODO: would be better to filter by searchTerm before sorting!
      if (!d.content.startsWith(searchTerm)) {
        continue;
      }
      totalHitCount++;
      //System.out.println("  match id=" + d.id + " score=" + d.score);

      if (doAllGroups) {
        if (!knownGroups.contains(d.group)) {
          knownGroups.add(d.group);
          //System.out.println("    add group=" + groupToString(d.group));
        }
      }

      List<GroupDoc> l = groups.get(d.group);
      if (l == null) {
        //System.out.println("    add sortedGroup=" + groupToString(d.group));
        sortedGroups.add(d.group);
        if (fillFields) {
          sortedGroupFields.add(fillFields(d, groupSort));
        }
        l = new ArrayList<>();
        groups.put(d.group, l);
      }
      l.add(d);
    }

    if (groupOffset >= sortedGroups.size()) {
      // slice is out of bounds
      return null;
    }

    final int limit = Math.min(groupOffset + topNGroups, groups.size());

    final Comparator<GroupDoc> docSortComp = getComparator(docSort);
    @SuppressWarnings({"unchecked","rawtypes"})
    final GroupDocs<BytesRef>[] result = new GroupDocs[limit-groupOffset];
    int totalGroupedHitCount = 0;
    for(int idx=groupOffset;idx < limit;idx++) {
      final BytesRef group = sortedGroups.get(idx);
      final List<GroupDoc> docs = groups.get(group);
      totalGroupedHitCount += docs.size();
      Collections.sort(docs, docSortComp);
      final ScoreDoc[] hits;
      if (docs.size() > docOffset) {
        final int docIDXLimit = Math.min(docOffset + docsPerGroup, docs.size());
        hits = new ScoreDoc[docIDXLimit - docOffset];
        for(int docIDX=docOffset; docIDX < docIDXLimit; docIDX++) {
          final GroupDoc d = docs.get(docIDX);
          final FieldDoc fd;
          if (fillFields) {
            fd = new FieldDoc(d.id, getScores ? d.score : Float.NaN, fillFields(d, docSort));
          } else {
            fd = new FieldDoc(d.id, getScores ? d.score : Float.NaN);
          }
          hits[docIDX-docOffset] = fd;
        }
      } else  {
        hits = new ScoreDoc[0];
      }

      result[idx-groupOffset] = new GroupDocs<>(Float.NaN,
                                                        0.0f,
                                                        docs.size(),
                                                        hits,
                                                        group,
                                                        fillFields ? sortedGroupFields.get(idx) : null);
    }

    if (doAllGroups) {
      return new TopGroups<>(
        new TopGroups<>(groupSort.getSort(), docSort.getSort(), totalHitCount, totalGroupedHitCount, result, Float.NaN),
          knownGroups.size()
      );
    } else {
      return new TopGroups<>(groupSort.getSort(), docSort.getSort(), totalHitCount, totalGroupedHitCount, result, Float.NaN);
    }
  }

  private DirectoryReader getDocBlockReader(Directory dir, GroupDoc[] groupDocs) throws IOException {
    // Coalesce by group, but in random order:
    Collections.shuffle(Arrays.asList(groupDocs), random());
    final Map<BytesRef,List<GroupDoc>> groupMap = new HashMap<>();
    final List<BytesRef> groupValues = new ArrayList<>();

    for(GroupDoc groupDoc : groupDocs) {
      if (!groupMap.containsKey(groupDoc.group)) {
        groupValues.add(groupDoc.group);
        groupMap.put(groupDoc.group, new ArrayList<GroupDoc>());
      }
      groupMap.get(groupDoc.group).add(groupDoc);
    }

    RandomIndexWriter w = new RandomIndexWriter(
                                                random(),
                                                dir,
                                                newIndexWriterConfig(new MockAnalyzer(random())));

    final List<List<Document>> updateDocs = new ArrayList<>();

    FieldType groupEndType = new FieldType(StringField.TYPE_NOT_STORED);
    groupEndType.setIndexOptions(IndexOptions.DOCS);
    groupEndType.setOmitNorms(true);

    //System.out.println("TEST: index groups");
    for(BytesRef group : groupValues) {
      final List<Document> docs = new ArrayList<>();
      //System.out.println("TEST:   group=" + (group == null ? "null" : group.utf8ToString()));
      for(GroupDoc groupValue : groupMap.get(group)) {
        Document doc = new Document();
        docs.add(doc);
        if (groupValue.group != null) {
          doc.add(newStringField("group", groupValue.group.utf8ToString(), Field.Store.YES));
          doc.add(new SortedDocValuesField("group", BytesRef.deepCopyOf(groupValue.group)));
        }
        doc.add(newStringField("sort1", groupValue.sort1.utf8ToString(), Field.Store.NO));
        doc.add(new SortedDocValuesField("sort1", BytesRef.deepCopyOf(groupValue.sort1)));
        doc.add(newStringField("sort2", groupValue.sort2.utf8ToString(), Field.Store.NO));
        doc.add(new SortedDocValuesField("sort2", BytesRef.deepCopyOf(groupValue.sort2)));
        doc.add(new IntField("id", groupValue.id, Field.Store.NO));
        doc.add(new NumericDocValuesField("id", groupValue.id));
        doc.add(newTextField("content", groupValue.content, Field.Store.NO));
        //System.out.println("TEST:     doc content=" + groupValue.content + " group=" + (groupValue.group == null ? "null" : groupValue.group.utf8ToString()) + " sort1=" + groupValue.sort1.utf8ToString() + " id=" + groupValue.id);
      }
      // So we can pull filter marking last doc in block:
      final Field groupEnd = newField("groupend", "x", groupEndType);
      docs.get(docs.size()-1).add(groupEnd);
      // Add as a doc block:
      w.addDocuments(docs);
      if (group != null && random().nextInt(7) == 4) {
        updateDocs.add(docs);
      }
    }

    for(List<Document> docs : updateDocs) {
      // Just replaces docs w/ same docs:
      w.updateDocuments(new Term("group", docs.get(0).get("group")), docs);
    }

    final DirectoryReader r = w.getReader();
    w.close();

    return r;
  }

  private static class ShardState {

    public final ShardSearcher[] subSearchers;
    public final int[] docStarts;

    public ShardState(IndexSearcher s) {
      final IndexReaderContext ctx = s.getTopReaderContext();
      final List<LeafReaderContext> leaves = ctx.leaves();
      subSearchers = new ShardSearcher[leaves.size()];
      for(int searcherIDX=0;searcherIDX<subSearchers.length;searcherIDX++) {
        subSearchers[searcherIDX] = new ShardSearcher(leaves.get(searcherIDX), ctx);
      }

      docStarts = new int[subSearchers.length];
      for(int subIDX=0;subIDX<docStarts.length;subIDX++) {
        docStarts[subIDX] = leaves.get(subIDX).docBase;
        //System.out.println("docStarts[" + subIDX + "]=" + docStarts[subIDX]);
      }
    }
  }

  public void testRandom() throws Exception {
    int numberOfRuns = TestUtil.nextInt(random(), 3, 6);
    for (int iter=0; iter<numberOfRuns; iter++) {
      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }

      final int numDocs = TestUtil.nextInt(random(), 100, 1000) * RANDOM_MULTIPLIER;
      //final int numDocs = _TestUtil.nextInt(random, 5, 20);

      final int numGroups = TestUtil.nextInt(random(), 1, numDocs);

      if (VERBOSE) {
        System.out.println("TEST: numDocs=" + numDocs + " numGroups=" + numGroups);
      }

      final List<BytesRef> groups = new ArrayList<>();
      for(int i=0;i<numGroups;i++) {
        String randomValue;
        do {
          // B/c of DV based impl we can't see the difference between an empty string and a null value.
          // For that reason we don't generate empty string
          // groups.
          randomValue = TestUtil.randomRealisticUnicodeString(random());
          //randomValue = TestUtil.randomSimpleString(random());
        } while ("".equals(randomValue));

        groups.add(new BytesRef(randomValue));
      }
      final String[] contentStrings = new String[TestUtil.nextInt(random(), 2, 20)];
      if (VERBOSE) {
        System.out.println("TEST: create fake content");
      }
      for(int contentIDX=0;contentIDX<contentStrings.length;contentIDX++) {
        final StringBuilder sb = new StringBuilder();
        sb.append("real").append(random().nextInt(3)).append(' ');
        final int fakeCount = random().nextInt(10);
        for(int fakeIDX=0;fakeIDX<fakeCount;fakeIDX++) {
          sb.append("fake ");
        }
        contentStrings[contentIDX] = sb.toString();
        if (VERBOSE) {
          System.out.println("  content=" + sb.toString());
        }
      }

      Directory dir = newDirectory();
      RandomIndexWriter w = new RandomIndexWriter(
                                                  random(),
                                                  dir,
                                                  newIndexWriterConfig(new MockAnalyzer(random())));
      Document doc = new Document();
      Document docNoGroup = new Document();
      Field idvGroupField = new SortedDocValuesField("group", new BytesRef());
      doc.add(idvGroupField);
      docNoGroup.add(idvGroupField);

      Field group = newStringField("group", "", Field.Store.NO);
      doc.add(group);
      Field sort1 = new SortedDocValuesField("sort1", new BytesRef());
      doc.add(sort1);
      docNoGroup.add(sort1);
      Field sort2 = new SortedDocValuesField("sort2", new BytesRef());
      doc.add(sort2);
      docNoGroup.add(sort2);
      Field content = newTextField("content", "", Field.Store.NO);
      doc.add(content);
      docNoGroup.add(content);
      IntField id = new IntField("id", 0, Field.Store.NO);
      doc.add(id);
      NumericDocValuesField idDV = new NumericDocValuesField("id", 0);
      doc.add(idDV);
      docNoGroup.add(id);
      docNoGroup.add(idDV);
      final GroupDoc[] groupDocs = new GroupDoc[numDocs];
      for(int i=0;i<numDocs;i++) {
        final BytesRef groupValue;
        if (random().nextInt(24) == 17) {
          // So we test the "doc doesn't have the group'd
          // field" case:
          groupValue = null;
        } else {
          groupValue = groups.get(random().nextInt(groups.size()));
        }
        final GroupDoc groupDoc = new GroupDoc(i,
                                               groupValue,
                                               groups.get(random().nextInt(groups.size())),
                                               groups.get(random().nextInt(groups.size())),
                                               contentStrings[random().nextInt(contentStrings.length)]);
        if (VERBOSE) {
          System.out.println("  doc content=" + groupDoc.content + " id=" + i + " group=" + (groupDoc.group == null ? "null" : groupDoc.group.utf8ToString()) + " sort1=" + groupDoc.sort1.utf8ToString() + " sort2=" + groupDoc.sort2.utf8ToString());
        }

        groupDocs[i] = groupDoc;
        if (groupDoc.group != null) {
          group.setStringValue(groupDoc.group.utf8ToString());
          idvGroupField.setBytesValue(BytesRef.deepCopyOf(groupDoc.group));
        } else {
          // TODO: not true
          // Must explicitly set empty string, else eg if
          // the segment has all docs missing the field then
          // we get null back instead of empty BytesRef:
          idvGroupField.setBytesValue(new BytesRef());
        }
        sort1.setBytesValue(BytesRef.deepCopyOf(groupDoc.sort1));
        sort2.setBytesValue(BytesRef.deepCopyOf(groupDoc.sort2));
        content.setStringValue(groupDoc.content);
        id.setIntValue(groupDoc.id);
        idDV.setLongValue(groupDoc.id);
        if (groupDoc.group == null) {
          w.addDocument(docNoGroup);
        } else {
          w.addDocument(doc);
        }
      }

      final GroupDoc[] groupDocsByID = new GroupDoc[groupDocs.length];
      System.arraycopy(groupDocs, 0, groupDocsByID, 0, groupDocs.length);

      final DirectoryReader r = w.getReader();
      w.close();

      final NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");
      DirectoryReader rBlocks = null;
      Directory dirBlocks = null;

      final IndexSearcher s = newSearcher(r);
      if (VERBOSE) {
        System.out.println("\nTEST: searcher=" + s);
      }
      
      final ShardState shards = new ShardState(s);
      
      Set<Integer> seenIDs = new HashSet<>();
      for(int contentID=0;contentID<3;contentID++) {
        final ScoreDoc[] hits = s.search(new TermQuery(new Term("content", "real"+contentID)), numDocs).scoreDocs;
        for(ScoreDoc hit : hits) {
          int idValue = (int) docIDToID.get(hit.doc);

          final GroupDoc gd = groupDocs[idValue];
          seenIDs.add(idValue);
          assertTrue(gd.score == 0.0);
          gd.score = hit.score;
          assertEquals(gd.id, idValue);
        }
      }
      
      // make sure all groups were seen across the hits
      assertEquals(groupDocs.length, seenIDs.size());

      for(GroupDoc gd : groupDocs) {
        assertFalse(Float.isNaN(gd.score));
        assertFalse(Float.isInfinite(gd.score));
        assertTrue(gd.score >= 0.0);
      }
      
      // Build 2nd index, where docs are added in blocks by
      // group, so we can use single pass collector
      dirBlocks = newDirectory();
      rBlocks = getDocBlockReader(dirBlocks, groupDocs);
      final Query lastDocInBlock = new TermQuery(new Term("groupend", "x"));
      final NumericDocValues docIDToIDBlocks = MultiDocValues.getNumericValues(rBlocks, "id");
      assertNotNull(docIDToIDBlocks);
      
      final IndexSearcher sBlocks = newSearcher(rBlocks);
      final ShardState shardsBlocks = new ShardState(sBlocks);
      
      // ReaderBlocks only increases maxDoc() vs reader, which
      // means a monotonic shift in scores, so we can
      // reliably remap them w/ Map:
      final Map<String,Map<Float,Float>> scoreMap = new HashMap<>();
      
      // Tricky: must separately set .score2, because the doc
      // block index was created with possible deletions!
      //System.out.println("fixup score2");
      for(int contentID=0;contentID<3;contentID++) {
        //System.out.println("  term=real" + contentID);
        final Map<Float,Float> termScoreMap = new HashMap<>();
        scoreMap.put("real"+contentID, termScoreMap);
        //System.out.println("term=real" + contentID + " dfold=" + s.docFreq(new Term("content", "real"+contentID)) +
        //" dfnew=" + sBlocks.docFreq(new Term("content", "real"+contentID)));
        final ScoreDoc[] hits = sBlocks.search(new TermQuery(new Term("content", "real"+contentID)), numDocs).scoreDocs;
        for(ScoreDoc hit : hits) {
          final GroupDoc gd = groupDocsByID[(int) docIDToIDBlocks.get(hit.doc)];
          assertTrue(gd.score2 == 0.0);
          gd.score2 = hit.score;
          assertEquals(gd.id, docIDToIDBlocks.get(hit.doc));
          //System.out.println("    score=" + gd.score + " score2=" + hit.score + " id=" + docIDToIDBlocks.get(hit.doc));
          termScoreMap.put(gd.score, gd.score2);
        }
      }
      
      for(int searchIter=0;searchIter<100;searchIter++) {
        
        if (VERBOSE) {
          System.out.println("\nTEST: searchIter=" + searchIter);
        }
        
        final String searchTerm = "real" + random().nextInt(3);
        final boolean fillFields = random().nextBoolean();
        boolean getScores = random().nextBoolean();
        final boolean getMaxScores = random().nextBoolean();
        final Sort groupSort = getRandomSort();
        //final Sort groupSort = new Sort(new SortField[] {new SortField("sort1", SortField.STRING), new SortField("id", SortField.INT)});
        final Sort docSort = getRandomSort();
        
        getScores |= (groupSort.needsScores() || docSort.needsScores());
        
        final int topNGroups = TestUtil.nextInt(random(), 1, 30);
        //final int topNGroups = 10;
        final int docsPerGroup = TestUtil.nextInt(random(), 1, 50);
        
        final int groupOffset = TestUtil.nextInt(random(), 0, (topNGroups - 1) / 2);
        //final int groupOffset = 0;
        
        final int docOffset = TestUtil.nextInt(random(), 0, docsPerGroup - 1);
        //final int docOffset = 0;

        final boolean doCache = random().nextBoolean();
        final boolean doAllGroups = random().nextBoolean();
        if (VERBOSE) {
          System.out.println("TEST: groupSort=" + groupSort + " docSort=" + docSort + " searchTerm=" + searchTerm + " dF=" + r.docFreq(new Term("content", searchTerm))  +" dFBlock=" + rBlocks.docFreq(new Term("content", searchTerm)) + " topNGroups=" + topNGroups + " groupOffset=" + groupOffset + " docOffset=" + docOffset + " doCache=" + doCache + " docsPerGroup=" + docsPerGroup + " doAllGroups=" + doAllGroups + " getScores=" + getScores + " getMaxScores=" + getMaxScores);
        }
        
        String groupField = "group";
        if (VERBOSE) {
          System.out.println("  groupField=" + groupField);
        }
        final AbstractFirstPassGroupingCollector<?> c1 = createRandomFirstPassCollector(groupField, groupSort, groupOffset+topNGroups);
        final CachingCollector cCache;
        final Collector c;
        
        final AbstractAllGroupsCollector<?> allGroupsCollector;
        if (doAllGroups) {
          allGroupsCollector = createAllGroupsCollector(c1, groupField);
        } else {
          allGroupsCollector = null;
        }
        
        final boolean useWrappingCollector = random().nextBoolean();
        
        if (doCache) {
          final double maxCacheMB = random().nextDouble();
          if (VERBOSE) {
            System.out.println("TEST: maxCacheMB=" + maxCacheMB);
          }
          
          if (useWrappingCollector) {
            if (doAllGroups) {
              cCache = CachingCollector.create(c1, true, maxCacheMB);
              c = MultiCollector.wrap(cCache, allGroupsCollector);
            } else {
              c = cCache = CachingCollector.create(c1, true, maxCacheMB);
            }
          } else {
            // Collect only into cache, then replay multiple times:
            c = cCache = CachingCollector.create(true, maxCacheMB);
          }
        } else {
          cCache = null;
          if (doAllGroups) {
            c = MultiCollector.wrap(c1, allGroupsCollector);
          } else {
            c = c1;
          }
        }
        
        // Search top reader:
        final Query query = new TermQuery(new Term("content", searchTerm));
        
        s.search(query, c);
        
        if (doCache && !useWrappingCollector) {
          if (cCache.isCached()) {
            // Replay for first-pass grouping
            cCache.replay(c1);
            if (doAllGroups) {
              // Replay for all groups:
              cCache.replay(allGroupsCollector);
            }
          } else {
            // Replay by re-running search:
            s.search(query, c1);
            if (doAllGroups) {
              s.search(query, allGroupsCollector);
            }
          }
        }
        
        // Get 1st pass top groups
        final Collection<SearchGroup<BytesRef>> topGroups = getSearchGroups(c1, groupOffset, fillFields);
        final TopGroups<BytesRef> groupsResult;
        if (VERBOSE) {
          System.out.println("TEST: first pass topGroups");
          if (topGroups == null) {
            System.out.println("  null");
          } else {
            for (SearchGroup<BytesRef> searchGroup : topGroups) {
              System.out.println("  " + (searchGroup.groupValue == null ? "null" : searchGroup.groupValue) + ": " + Arrays.deepToString(searchGroup.sortValues));
            }
          }
        }
        
        // Get 1st pass top groups using shards
        
        final TopGroups<BytesRef> topGroupsShards = searchShards(s, shards.subSearchers, query, groupSort, docSort,
            groupOffset, topNGroups, docOffset, docsPerGroup, getScores, getMaxScores, true, false);
        final AbstractSecondPassGroupingCollector<?> c2;
        if (topGroups != null) {
          
          if (VERBOSE) {
            System.out.println("TEST: topGroups");
            for (SearchGroup<BytesRef> searchGroup : topGroups) {
              System.out.println("  " + (searchGroup.groupValue == null ? "null" : searchGroup.groupValue.utf8ToString()) + ": " + Arrays.deepToString(searchGroup.sortValues));
            }
          }
          
          c2 = createSecondPassCollector(c1, groupField, groupSort, docSort, groupOffset, docOffset + docsPerGroup, getScores, getMaxScores, fillFields);
          if (doCache) {
            if (cCache.isCached()) {
              if (VERBOSE) {
                System.out.println("TEST: cache is intact");
              }
              cCache.replay(c2);
            } else {
              if (VERBOSE) {
                System.out.println("TEST: cache was too large");
              }
              s.search(query, c2);
            }
          } else {
            s.search(query, c2);
          }
          
          if (doAllGroups) {
            TopGroups<BytesRef> tempTopGroups = getTopGroups(c2, docOffset);
            groupsResult = new TopGroups<>(tempTopGroups, allGroupsCollector.getGroupCount());
          } else {
            groupsResult = getTopGroups(c2, docOffset);
          }
        } else {
          c2 = null;
          groupsResult = null;
          if (VERBOSE) {
            System.out.println("TEST:   no results");
          }
        }
        
        final TopGroups<BytesRef> expectedGroups = slowGrouping(groupDocs, searchTerm, fillFields, getScores, getMaxScores, doAllGroups, groupSort, docSort, topNGroups, docsPerGroup, groupOffset, docOffset);
        
        if (VERBOSE) {
          if (expectedGroups == null) {
            System.out.println("TEST: no expected groups");
          } else {
            System.out.println("TEST: expected groups totalGroupedHitCount=" + expectedGroups.totalGroupedHitCount);
            for(GroupDocs<BytesRef> gd : expectedGroups.groups) {
              System.out.println("  group=" + (gd.groupValue == null ? "null" : gd.groupValue) + " totalHits=" + gd.totalHits + " scoreDocs.len=" + gd.scoreDocs.length);
              for(ScoreDoc sd : gd.scoreDocs) {
                System.out.println("    id=" + sd.doc + " score=" + sd.score);
              }
            }
          }
          
          if (groupsResult == null) {
            System.out.println("TEST: no matched groups");
          } else {
            System.out.println("TEST: matched groups totalGroupedHitCount=" + groupsResult.totalGroupedHitCount);
            for(GroupDocs<BytesRef> gd : groupsResult.groups) {
              System.out.println("  group=" + (gd.groupValue == null ? "null" : gd.groupValue) + " totalHits=" + gd.totalHits);
              for(ScoreDoc sd : gd.scoreDocs) {
                System.out.println("    id=" + docIDToID.get(sd.doc) + " score=" + sd.score);
              }
            }
            
            if (searchIter == 14) {
              for(int docIDX=0;docIDX<s.getIndexReader().maxDoc();docIDX++) {
                System.out.println("ID=" + docIDToID.get(docIDX) + " explain=" + s.explain(query, docIDX));
              }
            }
          }
          
          if (topGroupsShards == null) {
            System.out.println("TEST: no matched-merged groups");
          } else {
            System.out.println("TEST: matched-merged groups totalGroupedHitCount=" + topGroupsShards.totalGroupedHitCount);
            for(GroupDocs<BytesRef> gd : topGroupsShards.groups) {
              System.out.println("  group=" + (gd.groupValue == null ? "null" : gd.groupValue) + " totalHits=" + gd.totalHits);
              for(ScoreDoc sd : gd.scoreDocs) {
                System.out.println("    id=" + docIDToID.get(sd.doc) + " score=" + sd.score);
              }
            }
          }
        }
        
        assertEquals(docIDToID, expectedGroups, groupsResult, true, true, true, getScores, true);
        
        // Confirm merged shards match:
        assertEquals(docIDToID, expectedGroups, topGroupsShards, true, false, fillFields, getScores, true);
        if (topGroupsShards != null) {
          verifyShards(shards.docStarts, topGroupsShards);
        }
        
        final boolean needsScores = getScores || getMaxScores || docSort == null;
        final BlockGroupingCollector c3 = new BlockGroupingCollector(groupSort, groupOffset+topNGroups, needsScores, sBlocks.createNormalizedWeight(lastDocInBlock, false));
        final TermAllGroupsCollector allGroupsCollector2;
        final Collector c4;
        if (doAllGroups) {
          // NOTE: must be "group" and not "group_dv"
          // (groupField) because we didn't index doc
          // values in the block index:
          allGroupsCollector2 = new TermAllGroupsCollector("group");
          c4 = MultiCollector.wrap(c3, allGroupsCollector2);
        } else {
          allGroupsCollector2 = null;
          c4 = c3;
        }
        // Get block grouping result:
        sBlocks.search(query, c4);
        @SuppressWarnings({"unchecked","rawtypes"})
        final TopGroups<BytesRef> tempTopGroupsBlocks = (TopGroups<BytesRef>) c3.getTopGroups(docSort, groupOffset, docOffset, docOffset+docsPerGroup, fillFields);
        final TopGroups<BytesRef> groupsResultBlocks;
        if (doAllGroups && tempTopGroupsBlocks != null) {
          assertEquals((int) tempTopGroupsBlocks.totalGroupCount, allGroupsCollector2.getGroupCount());
          groupsResultBlocks = new TopGroups<>(tempTopGroupsBlocks, allGroupsCollector2.getGroupCount());
        } else {
          groupsResultBlocks = tempTopGroupsBlocks;
        }
        
        if (VERBOSE) {
          if (groupsResultBlocks == null) {
            System.out.println("TEST: no block groups");
          } else {
            System.out.println("TEST: block groups totalGroupedHitCount=" + groupsResultBlocks.totalGroupedHitCount);
            boolean first = true;
            for(GroupDocs<BytesRef> gd : groupsResultBlocks.groups) {
              System.out.println("  group=" + (gd.groupValue == null ? "null" : gd.groupValue.utf8ToString()) + " totalHits=" + gd.totalHits);
              for(ScoreDoc sd : gd.scoreDocs) {
                System.out.println("    id=" + docIDToIDBlocks.get(sd.doc) + " score=" + sd.score);
                if (first) {
                  System.out.println("explain: " + sBlocks.explain(query, sd.doc));
                  first = false;
                }
              }
            }
          }
        }
        
        // Get shard'd block grouping result:
        final TopGroups<BytesRef> topGroupsBlockShards = searchShards(sBlocks, shardsBlocks.subSearchers, query,
            groupSort, docSort, groupOffset, topNGroups, docOffset, docsPerGroup, getScores, getMaxScores, false, false);
        
        if (expectedGroups != null) {
          // Fixup scores for reader2
          for (GroupDocs<?> groupDocsHits : expectedGroups.groups) {
            for(ScoreDoc hit : groupDocsHits.scoreDocs) {
              final GroupDoc gd = groupDocsByID[hit.doc];
              assertEquals(gd.id, hit.doc);
              //System.out.println("fixup score " + hit.score + " to " + gd.score2 + " vs " + gd.score);
              hit.score = gd.score2;
            }
          }
          
          final SortField[] sortFields = groupSort.getSort();
          final Map<Float,Float> termScoreMap = scoreMap.get(searchTerm);
          for(int groupSortIDX=0;groupSortIDX<sortFields.length;groupSortIDX++) {
            if (sortFields[groupSortIDX].getType() == SortField.Type.SCORE) {
              for (GroupDocs<?> groupDocsHits : expectedGroups.groups) {
                if (groupDocsHits.groupSortValues != null) {
                  //System.out.println("remap " + groupDocsHits.groupSortValues[groupSortIDX] + " to " + termScoreMap.get(groupDocsHits.groupSortValues[groupSortIDX]));
                  groupDocsHits.groupSortValues[groupSortIDX] = termScoreMap.get(groupDocsHits.groupSortValues[groupSortIDX]);
                  assertNotNull(groupDocsHits.groupSortValues[groupSortIDX]);
                }
              }
            }
          }
          
          final SortField[] docSortFields = docSort.getSort();
          for(int docSortIDX=0;docSortIDX<docSortFields.length;docSortIDX++) {
            if (docSortFields[docSortIDX].getType() == SortField.Type.SCORE) {
              for (GroupDocs<?> groupDocsHits : expectedGroups.groups) {
                for(ScoreDoc _hit : groupDocsHits.scoreDocs) {
                  FieldDoc hit = (FieldDoc) _hit;
                  if (hit.fields != null) {
                    hit.fields[docSortIDX] = termScoreMap.get(hit.fields[docSortIDX]);
                    assertNotNull(hit.fields[docSortIDX]);
                  }
                }
              }
            }
          }
        }
        
        assertEquals(docIDToIDBlocks, expectedGroups, groupsResultBlocks, false, true, true, getScores, false);
        assertEquals(docIDToIDBlocks, expectedGroups, topGroupsBlockShards, false, false, fillFields, getScores, false);
      }
      
      r.close();
      dir.close();
      
      rBlocks.close();
      dirBlocks.close();
    }
  }

  private void verifyShards(int[] docStarts, TopGroups<BytesRef> topGroups) {
    for(GroupDocs<?> group : topGroups.groups) {
      for(int hitIDX=0;hitIDX<group.scoreDocs.length;hitIDX++) {
        final ScoreDoc sd = group.scoreDocs[hitIDX];
        assertEquals("doc=" + sd.doc + " wrong shard",
                     ReaderUtil.subIndex(sd.doc, docStarts),
                     sd.shardIndex);
      }
    }
  }

  private TopGroups<BytesRef> searchShards(IndexSearcher topSearcher, ShardSearcher[] subSearchers, Query query, Sort groupSort, Sort docSort, int groupOffset, int topNGroups, int docOffset,
                                           int topNDocs, boolean getScores, boolean getMaxScores, boolean canUseIDV, boolean preFlex) throws Exception {

    // TODO: swap in caching, all groups collector hereassertEquals(expected.totalHitCount, actual.totalHitCount);
    // too...
    if (VERBOSE) {
      System.out.println("TEST: " + subSearchers.length + " shards: " + Arrays.toString(subSearchers) + " canUseIDV=" + canUseIDV);
    }
    // Run 1st pass collector to get top groups per shard
    final Weight w = topSearcher.createNormalizedWeight(query, getScores);
    final List<Collection<SearchGroup<BytesRef>>> shardGroups = new ArrayList<>();
    List<AbstractFirstPassGroupingCollector<?>> firstPassGroupingCollectors = new ArrayList<>();
    AbstractFirstPassGroupingCollector<?> firstPassCollector = null;
    boolean shardsCanUseIDV;
    if (canUseIDV) {
      if (SlowCompositeReaderWrapper.class.isAssignableFrom(subSearchers[0].getIndexReader().getClass())) {
        shardsCanUseIDV = false;
      } else {
        shardsCanUseIDV = !preFlex;
      }
    } else {
      shardsCanUseIDV = false;
    }

    String groupField = "group";

    for(int shardIDX=0;shardIDX<subSearchers.length;shardIDX++) {

      // First shard determines whether we use IDV or not;
      // all other shards match that:
      if (firstPassCollector == null) {
        firstPassCollector = createRandomFirstPassCollector(groupField, groupSort, groupOffset + topNGroups);
      } else {
        firstPassCollector = createFirstPassCollector(groupField, groupSort, groupOffset + topNGroups, firstPassCollector);
      }
      if (VERBOSE) {
        System.out.println("  shard=" + shardIDX + " groupField=" + groupField);
        System.out.println("    1st pass collector=" + firstPassCollector);
      }
      firstPassGroupingCollectors.add(firstPassCollector);
      subSearchers[shardIDX].search(w, firstPassCollector);
      final Collection<SearchGroup<BytesRef>> topGroups = getSearchGroups(firstPassCollector, 0, true);
      if (topGroups != null) {
        if (VERBOSE) {
          System.out.println("  shard " + shardIDX + " s=" + subSearchers[shardIDX] + " totalGroupedHitCount=?" + " " + topGroups.size() + " groups:");
          for(SearchGroup<BytesRef> group : topGroups) {
            System.out.println("    " + groupToString(group.groupValue) + " groupSort=" + Arrays.toString(group.sortValues));
          }
        }
        shardGroups.add(topGroups);
      }
    }

    final Collection<SearchGroup<BytesRef>> mergedTopGroups = SearchGroup.merge(shardGroups, groupOffset, topNGroups, groupSort);
    if (VERBOSE) {
      System.out.println(" top groups merged:");
      if (mergedTopGroups == null) {
        System.out.println("    null");
      } else {
        System.out.println("    " + mergedTopGroups.size() + " top groups:");
        for(SearchGroup<BytesRef> group : mergedTopGroups) {
          System.out.println("    [" + groupToString(group.groupValue) + "] groupSort=" + Arrays.toString(group.sortValues));
        }
      }
    }

    if (mergedTopGroups != null) {
      // Now 2nd pass:
      @SuppressWarnings({"unchecked","rawtypes"})
      final TopGroups<BytesRef>[] shardTopGroups = new TopGroups[subSearchers.length];
      for(int shardIDX=0;shardIDX<subSearchers.length;shardIDX++) {
        final AbstractSecondPassGroupingCollector<?> secondPassCollector = createSecondPassCollector(firstPassGroupingCollectors.get(shardIDX),
            groupField, mergedTopGroups, groupSort, docSort, docOffset + topNDocs, getScores, getMaxScores, true);
        subSearchers[shardIDX].search(w, secondPassCollector);
        shardTopGroups[shardIDX] = getTopGroups(secondPassCollector, 0);
        if (VERBOSE) {
          System.out.println(" " + shardTopGroups[shardIDX].groups.length + " shard[" + shardIDX + "] groups:");
          for(GroupDocs<BytesRef> group : shardTopGroups[shardIDX].groups) {
            System.out.println("    [" + groupToString(group.groupValue) + "] groupSort=" + Arrays.toString(group.groupSortValues) + " numDocs=" + group.scoreDocs.length);
          }
        }
      }

      TopGroups<BytesRef> mergedGroups = TopGroups.merge(shardTopGroups, groupSort, docSort, docOffset, topNDocs, TopGroups.ScoreMergeMode.None);
      if (VERBOSE) {
        System.out.println(" " + mergedGroups.groups.length + " merged groups:");
        for(GroupDocs<BytesRef> group : mergedGroups.groups) {
          System.out.println("    [" + groupToString(group.groupValue) + "] groupSort=" + Arrays.toString(group.groupSortValues)  + " numDocs=" + group.scoreDocs.length);
        }
      }
      return mergedGroups;
    } else {
      return null;
    }
  }

  private void assertEquals(NumericDocValues docIDtoID, TopGroups<BytesRef> expected, TopGroups<BytesRef> actual, boolean verifyGroupValues, boolean verifyTotalGroupCount, boolean verifySortValues, boolean testScores, boolean idvBasedImplsUsed) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);

    assertEquals("expected.groups.length != actual.groups.length", expected.groups.length, actual.groups.length);
    assertEquals("expected.totalHitCount != actual.totalHitCount", expected.totalHitCount, actual.totalHitCount);
    assertEquals("expected.totalGroupedHitCount != actual.totalGroupedHitCount", expected.totalGroupedHitCount, actual.totalGroupedHitCount);
    if (expected.totalGroupCount != null && verifyTotalGroupCount) {
      assertEquals("expected.totalGroupCount != actual.totalGroupCount", expected.totalGroupCount, actual.totalGroupCount);
    }

    for(int groupIDX=0;groupIDX<expected.groups.length;groupIDX++) {
      if (VERBOSE) {
        System.out.println("  check groupIDX=" + groupIDX);
      }
      final GroupDocs<BytesRef> expectedGroup = expected.groups[groupIDX];
      final GroupDocs<BytesRef> actualGroup = actual.groups[groupIDX];
      if (verifyGroupValues) {
        if (idvBasedImplsUsed) {
          if (actualGroup.groupValue.length == 0) {
            assertNull(expectedGroup.groupValue);
          } else {
            assertEquals(expectedGroup.groupValue, actualGroup.groupValue);
          }
        } else {
          assertEquals(expectedGroup.groupValue, actualGroup.groupValue);
        }

      }
      if (verifySortValues) {
        assertArrayEquals(expectedGroup.groupSortValues, actualGroup.groupSortValues);
      }

      // TODO
      // assertEquals(expectedGroup.maxScore, actualGroup.maxScore);
      assertEquals(expectedGroup.totalHits, actualGroup.totalHits);

      final ScoreDoc[] expectedFDs = expectedGroup.scoreDocs;
      final ScoreDoc[] actualFDs = actualGroup.scoreDocs;

      assertEquals(expectedFDs.length, actualFDs.length);
      for(int docIDX=0;docIDX<expectedFDs.length;docIDX++) {
        final FieldDoc expectedFD = (FieldDoc) expectedFDs[docIDX];
        final FieldDoc actualFD = (FieldDoc) actualFDs[docIDX];
        //System.out.println("  actual doc=" + docIDtoID.get(actualFD.doc) + " score=" + actualFD.score);
        assertEquals(expectedFD.doc, docIDtoID.get(actualFD.doc));
        if (testScores) {
          assertEquals(expectedFD.score, actualFD.score, 0.1);
        } else {
          // TODO: too anal for now
          //assertEquals(Float.NaN, actualFD.score);
        }
        if (verifySortValues) {
          assertArrayEquals(expectedFD.fields, actualFD.fields);
        }
      }
    }
  }

  private static class ShardSearcher extends IndexSearcher {
    private final List<LeafReaderContext> ctx;

    public ShardSearcher(LeafReaderContext ctx, IndexReaderContext parent) {
      super(parent);
      this.ctx = Collections.singletonList(ctx);
    }

    public void search(Weight weight, Collector collector) throws IOException {
      search(ctx, weight, collector);
    }

    @Override
    public String toString() {
      return "ShardSearcher(" + ctx.get(0).reader() + ")";
    }
  }

  private static class ValueHolder<V> {

    V value;

    private ValueHolder(V value) {
      this.value = value;
    }
  }
}
