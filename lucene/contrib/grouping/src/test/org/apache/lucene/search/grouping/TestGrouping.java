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

package org.apache.lucene.search.grouping;

import java.util.*;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

// TODO
//   - should test relevance sort too
//   - test null
//   - test ties
//   - test compound sort

public class TestGrouping extends LuceneTestCase {

  public void testBasic() throws Exception {

    final String groupField = "author";

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
                               random,
                               dir,
                               newIndexWriterConfig(TEST_VERSION_CURRENT,
                                                    new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    // 0
    Document doc = new Document();
    doc.add(new Field(groupField, "author1", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("content", "random text", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "1", Field.Store.YES, Field.Index.NO));
    w.addDocument(doc);

    // 1
    doc = new Document();
    doc.add(new Field(groupField, "author1", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("content", "some more random text", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "2", Field.Store.YES, Field.Index.NO));
    w.addDocument(doc);

    // 2
    doc = new Document();
    doc.add(new Field(groupField, "author1", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("content", "some more random textual data", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "3", Field.Store.YES, Field.Index.NO));
    w.addDocument(doc);

    // 3
    doc = new Document();
    doc.add(new Field(groupField, "author2", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("content", "some random text", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "4", Field.Store.YES, Field.Index.NO));
    w.addDocument(doc);

    // 4
    doc = new Document();
    doc.add(new Field(groupField, "author3", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("content", "some more random text", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "5", Field.Store.YES, Field.Index.NO));
    w.addDocument(doc);

    // 5
    doc = new Document();
    doc.add(new Field(groupField, "author3", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("content", "random", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "6", Field.Store.YES, Field.Index.NO));
    w.addDocument(doc);

    // 6 -- no author field
    doc = new Document();
    doc.add(new Field("content", "random word stuck in alot of other text", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("id", "6", Field.Store.YES, Field.Index.NO));
    w.addDocument(doc);

    IndexSearcher indexSearcher = new IndexSearcher(w.getReader());
    w.close();

    final Sort groupSort = Sort.RELEVANCE;
    final FirstPassGroupingCollector c1 = new FirstPassGroupingCollector(groupField, groupSort, 10);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c1);

    final SecondPassGroupingCollector c2 = new SecondPassGroupingCollector(groupField, c1.getTopGroups(0, true), groupSort, null, 5, true, false, true);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c2);
    
    final TopGroups groups = c2.getTopGroups(0);

    assertEquals(7, groups.totalHitCount);
    assertEquals(7, groups.totalGroupedHitCount);
    assertEquals(4, groups.groups.length);

    // relevance order: 5, 0, 3, 4, 1, 2, 6

    // the later a document is added the higher this docId
    // value
    GroupDocs group = groups.groups[0];
    assertEquals("author3", group.groupValue);
    assertEquals(2, group.scoreDocs.length);
    assertEquals(5, group.scoreDocs[0].doc);
    assertEquals(4, group.scoreDocs[1].doc);
    assertTrue(group.scoreDocs[0].score > group.scoreDocs[1].score);

    group = groups.groups[1];
    assertEquals("author1", group.groupValue);
    assertEquals(3, group.scoreDocs.length);
    assertEquals(0, group.scoreDocs[0].doc);
    assertEquals(1, group.scoreDocs[1].doc);
    assertEquals(2, group.scoreDocs[2].doc);
    assertTrue(group.scoreDocs[0].score > group.scoreDocs[1].score);
    assertTrue(group.scoreDocs[1].score > group.scoreDocs[2].score);

    group = groups.groups[2];
    assertEquals("author2", group.groupValue);
    assertEquals(1, group.scoreDocs.length);
    assertEquals(3, group.scoreDocs[0].doc);

    group = groups.groups[3];
    assertNull(group.groupValue);
    assertEquals(1, group.scoreDocs.length);
    assertEquals(6, group.scoreDocs[0].doc);

    indexSearcher.getIndexReader().close();
    dir.close();
  }

  private static class GroupDoc {
    final int id;
    final String group;
    final String sort1;
    final String sort2;
    final String content;

    public GroupDoc(int id, String group, String sort1, String sort2, String content) {
      this.id = id;
      this.group = group;
      this.sort1 = sort1;
      this.sort2 = sort2;
      this.content = content;
    }
  }

  private Sort getRandomSort() {
    final List<SortField> sortFields = new ArrayList<SortField>();
    if (random.nextBoolean()) {
      if (random.nextBoolean()) {
        sortFields.add(new SortField("sort1", SortField.STRING, random.nextBoolean()));
      } else {
        sortFields.add(new SortField("sort2", SortField.STRING, random.nextBoolean()));
      }
    } else if (random.nextBoolean()) {
      sortFields.add(new SortField("sort1", SortField.STRING, random.nextBoolean()));
      sortFields.add(new SortField("sort2", SortField.STRING, random.nextBoolean()));
    }
    sortFields.add(new SortField("id", SortField.INT));
    return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
  }

  private Comparator<GroupDoc> getComparator(Sort sort) {
    final SortField[] sortFields = sort.getSort();
    return new Comparator<GroupDoc>() {
      public int compare(GroupDoc d1, GroupDoc d2) {
        for(SortField sf : sortFields) {
          final int cmp;
          if (sf.getField().equals("sort1")) {
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

  private Comparable<?>[] fillFields(GroupDoc d, Sort sort) {
    final SortField[] sortFields = sort.getSort();
    final Comparable<?>[] fields = new Comparable[sortFields.length];
    for(int fieldIDX=0;fieldIDX<sortFields.length;fieldIDX++) {
      final Comparable<?> c;
      final SortField sf = sortFields[fieldIDX];
      if (sf.getField().equals("sort1")) {
        c = d.sort1;
      } else if (sf.getField().equals("sort2")) {
        c = d.sort2;
      } else {
        assertEquals("id", sf.getField());
        c = new Integer(d.id);
      }
      fields[fieldIDX] = c;
    }
    return fields;
  }

  private TopGroups slowGrouping(GroupDoc[] groupDocs,
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
    final HashMap<String,List<GroupDoc>> groups = new HashMap<String,List<GroupDoc>>();
    final List<String> sortedGroups = new ArrayList<String>();
    final List<Comparable<?>[]> sortedGroupFields = new ArrayList<Comparable<?>[]>();

    int totalHitCount = 0;
    Set<String> knownGroups = new HashSet<String>();

    for(GroupDoc d : groupDocs) {
      // TODO: would be better to filter by searchTerm before sorting!
      if (!d.content.equals(searchTerm)) {
        continue;
      }
      totalHitCount++;
      if (doAllGroups) {
        if (!knownGroups.contains(d.group)) {
          knownGroups.add(d.group);
        }
      }

      List<GroupDoc> l = groups.get(d.group);
      if (l == null) {
        sortedGroups.add(d.group);
        if (fillFields) {
          sortedGroupFields.add(fillFields(d, groupSort));
        }
        l = new ArrayList<GroupDoc>();
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
    final GroupDocs[] result = new GroupDocs[limit-groupOffset];
    int totalGroupedHitCount = 0;
    for(int idx=groupOffset;idx < limit;idx++) {
      final String group = sortedGroups.get(idx);
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
            fd = new FieldDoc(d.id, 0.0f, fillFields(d, docSort));
          } else {
            fd = new FieldDoc(d.id, 0.0f);
          }
          hits[docIDX-docOffset] = fd;
        }
      } else  {
        hits = new ScoreDoc[0];
      }

      result[idx-groupOffset] = new GroupDocs(0.0f,
                                              docs.size(),
                                              hits,
                                              group,
                                              fillFields ? sortedGroupFields.get(idx) : null);
    }

    if (doAllGroups) {
      return new TopGroups(
          new TopGroups(groupSort.getSort(), docSort.getSort(), totalHitCount, totalGroupedHitCount, result),
          knownGroups.size()
      );
    } else {
      return new TopGroups(groupSort.getSort(), docSort.getSort(), totalHitCount, totalGroupedHitCount, result);
    }
  }

  public void testRandom() throws Exception {
    for(int iter=0;iter<3;iter++) {

      if (VERBOSE) {
        System.out.println("TEST: iter=" + iter);
      }

      final int numDocs = _TestUtil.nextInt(random, 100, 1000) * RANDOM_MULTIPLIER;
      //final int numDocs = _TestUtil.nextInt(random, 5, 20);

      final int numGroups = _TestUtil.nextInt(random, 1, numDocs);

      if (VERBOSE) {
        System.out.println("TEST: numDocs=" + numDocs + " numGroups=" + numGroups);
      }
      
      final List<String> groups = new ArrayList<String>();
      for(int i=0;i<numGroups;i++) {
        groups.add(_TestUtil.randomRealisticUnicodeString(random));
        //groups.add(_TestUtil.randomUnicodeString(random));
        assertEquals(-1, groups.get(groups.size()-1).indexOf(0xffff));
        //groups.add(new BytesRef(_TestUtil.randomSimpleString(random)));
      }
      final String[] contentStrings = new String[] {"a", "b", "c", "d"};

      Directory dir = newDirectory();
      RandomIndexWriter w = new RandomIndexWriter(
                                                  random,
                                                  dir,
                                                  newIndexWriterConfig(TEST_VERSION_CURRENT,
                                                                       new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));

      Document doc = new Document();
      Document docNoGroup = new Document();
      Field group = newField("group", "", Field.Index.NOT_ANALYZED);
      doc.add(group);
      Field sort1 = newField("sort1", "", Field.Index.NOT_ANALYZED);
      doc.add(sort1);
      docNoGroup.add(sort1);
      Field sort2 = newField("sort2", "", Field.Index.NOT_ANALYZED);
      doc.add(sort2);
      docNoGroup.add(sort2);
      Field content = newField("content", "", Field.Index.NOT_ANALYZED);
      doc.add(content);
      docNoGroup.add(content);
      NumericField id = new NumericField("id");
      doc.add(id);
      docNoGroup.add(id);
      final GroupDoc[] groupDocs = new GroupDoc[numDocs];
      for(int i=0;i<numDocs;i++) {
        final String groupValue;
        if (random.nextInt(24) == 17) {
          // So we test the "doc doesn't have the group'd
          // field" case:
          groupValue = null;
        } else {
          groupValue = groups.get(random.nextInt(groups.size()));
        }
        final GroupDoc groupDoc = new GroupDoc(i,
                                               groupValue,
                                               groups.get(random.nextInt(groups.size())),
                                               groups.get(random.nextInt(groups.size())),
                                               contentStrings[random.nextInt(contentStrings.length)]);
        if (VERBOSE) {
          System.out.println("  doc content=" + groupDoc.content + " id=" + i + " group=" + (groupDoc.group == null ? "null" : groupDoc.group) + " sort1=" + groupDoc.sort1 + " sort2=" + groupDoc.sort2);
        }

        groupDocs[i] = groupDoc;
        if (groupDoc.group != null) {
          group.setValue(groupDoc.group);
        }
        sort1.setValue(groupDoc.sort1);
        sort2.setValue(groupDoc.sort2);
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

      final IndexSearcher s = new IndexSearcher(r);

      for(int searchIter=0;searchIter<100;searchIter++) {

        if (VERBOSE) {
          System.out.println("TEST: searchIter=" + searchIter);
        }

        final String searchTerm = contentStrings[random.nextInt(contentStrings.length)];
        final boolean fillFields = random.nextBoolean();
        final boolean getScores = random.nextBoolean();
        final boolean getMaxScores = random.nextBoolean();
        final Sort groupSort = getRandomSort();
        // TODO: also test null (= sort by relevance)
        final Sort docSort = getRandomSort();

        final int topNGroups = _TestUtil.nextInt(random, 1, 30);
        final int docsPerGroup = _TestUtil.nextInt(random, 1, 50);
        final int groupOffset = _TestUtil.nextInt(random, 0, (topNGroups-1)/2);
        //final int groupOffset = 0;

        final int docOffset = _TestUtil.nextInt(random, 0, docsPerGroup-1);
        //final int docOffset = 0;

        final boolean doCache = random.nextBoolean();
        final boolean doAllGroups = random.nextBoolean();
        if (VERBOSE) {
          System.out.println("TEST: groupSort=" + groupSort + " docSort=" + docSort + " searchTerm=" + searchTerm + " topNGroups=" + topNGroups + " groupOffset=" + groupOffset + " docOffset=" + docOffset + " doCache=" + doCache + " docsPerGroup=" + docsPerGroup + " doAllGroups=" + doAllGroups);
        }

        final AllGroupsCollector groupCountCollector;
        if (doAllGroups) {
          groupCountCollector = new AllGroupsCollector("group");
        } else {
          groupCountCollector = null;
        }

        final FirstPassGroupingCollector c1 = new FirstPassGroupingCollector("group", groupSort, groupOffset+topNGroups);
        final CachingCollector cCache;
        final Collector c;
        if (doCache) {
          final double maxCacheMB = random.nextDouble();
          if (VERBOSE) {
            System.out.println("TEST: maxCacheMB=" + maxCacheMB);
          }

          if (doAllGroups) {
            cCache = CachingCollector.create(c1, true, maxCacheMB);
            c = MultiCollector.wrap(cCache, groupCountCollector);
          } else {
            c = cCache = CachingCollector.create(c1, true, maxCacheMB);
          }
        } else if (doAllGroups) {
          c = MultiCollector.wrap(c1, groupCountCollector);
          cCache = null;
        } else {
          c = c1;
          cCache = null;
        }
        s.search(new TermQuery(new Term("content", searchTerm)), c);

        final Collection<SearchGroup> topGroups = c1.getTopGroups(groupOffset, fillFields);
        final TopGroups groupsResult;

        if (topGroups != null) {

          if (VERBOSE) {
            System.out.println("TEST: topGroups");
            for (SearchGroup searchGroup : topGroups) {
              System.out.println("  " + (searchGroup.groupValue == null ? "null" : searchGroup.groupValue) + ": " + Arrays.deepToString(searchGroup.sortValues));
            }
          }

          final SecondPassGroupingCollector c2 = new SecondPassGroupingCollector("group", topGroups, groupSort, docSort, docOffset+docsPerGroup, getScores, getMaxScores, fillFields);
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
              s.search(new TermQuery(new Term("content", searchTerm)), c2);
            }
          } else {
            s.search(new TermQuery(new Term("content", searchTerm)), c2);
          }
        
          if (doAllGroups) {
            TopGroups tempTopGroups = c2.getTopGroups(docOffset);
            groupsResult = new TopGroups(tempTopGroups, groupCountCollector.getGroupCount());
          } else {
            groupsResult = c2.getTopGroups(docOffset);
          }
        } else {
          groupsResult = null;
          if (VERBOSE) {
            System.out.println("TEST:   no results");
          }
        }

        final TopGroups expectedGroups = slowGrouping(groupDocs, searchTerm, fillFields, getScores, getMaxScores, doAllGroups, groupSort, docSort, topNGroups, docsPerGroup, groupOffset, docOffset);

        try {
          // NOTE: intentional but temporary field cache insanity!
          assertEquals(FieldCache.DEFAULT.getInts(r, "id"), expectedGroups, groupsResult);
        } finally {
          FieldCache.DEFAULT.purge(r);
        }
      }

      r.close();
      dir.close();
    }
  }

  private void assertEquals(int[] docIDtoID, TopGroups expected, TopGroups actual) {
    if (expected == null) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);

    assertEquals(expected.groups.length, actual.groups.length);
    assertEquals(expected.totalHitCount, actual.totalHitCount);
    assertEquals(expected.totalGroupedHitCount, actual.totalGroupedHitCount);
    if (expected.totalGroupCount != null) {
      assertEquals(expected.totalGroupCount, actual.totalGroupCount);
    }
    
    for(int groupIDX=0;groupIDX<expected.groups.length;groupIDX++) {
      if (VERBOSE) {
        System.out.println("  check groupIDX=" + groupIDX);
      }
      final GroupDocs expectedGroup = expected.groups[groupIDX];
      final GroupDocs actualGroup = actual.groups[groupIDX];
      assertEquals(expectedGroup.groupValue, actualGroup.groupValue);
      assertArrayEquals(expectedGroup.groupSortValues, actualGroup.groupSortValues);

      // TODO
      // assertEquals(expectedGroup.maxScore, actualGroup.maxScore);
      assertEquals(expectedGroup.totalHits, actualGroup.totalHits);

      final ScoreDoc[] expectedFDs = expectedGroup.scoreDocs;
      final ScoreDoc[] actualFDs = actualGroup.scoreDocs;

      assertEquals(expectedFDs.length, actualFDs.length);
      for(int docIDX=0;docIDX<expectedFDs.length;docIDX++) {
        final FieldDoc expectedFD = (FieldDoc) expectedFDs[docIDX];
        final FieldDoc actualFD = (FieldDoc) actualFDs[docIDX];
        assertEquals(expectedFD.doc, docIDtoID[actualFD.doc]);
        // TODO
        // assertEquals(expectedFD.score, actualFD.score);
        assertArrayEquals(expectedFD.fields, actualFD.fields);
      }
    }
  }
}
