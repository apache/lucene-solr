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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

import java.io.IOException;
import java.util.*;

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
    final TermFirstPassGroupingCollector c1 = new TermFirstPassGroupingCollector(groupField, groupSort, 10);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c1);

    final TermSecondPassGroupingCollector c2 = new TermSecondPassGroupingCollector(groupField, c1.getTopGroups(0, true), groupSort, null, 5, true, false, true);
    indexSearcher.search(new TermQuery(new Term("content", "random")), c2);

    final TopGroups groups = c2.getTopGroups(0);

    assertEquals(7, groups.totalHitCount);
    assertEquals(7, groups.totalGroupedHitCount);
    assertEquals(4, groups.groups.length);

    // relevance order: 5, 0, 3, 4, 1, 2, 6

    // the later a document is added the higher this docId
    // value
    GroupDocs group = groups.groups[0];
    assertEquals(new BytesRef("author3"), group.groupValue);
    assertEquals(2, group.scoreDocs.length);
    assertEquals(5, group.scoreDocs[0].doc);
    assertEquals(4, group.scoreDocs[1].doc);
    assertTrue(group.scoreDocs[0].score > group.scoreDocs[1].score);

    group = groups.groups[1];
    assertEquals(new BytesRef("author1"), group.groupValue);
    assertEquals(3, group.scoreDocs.length);
    assertEquals(0, group.scoreDocs[0].doc);
    assertEquals(1, group.scoreDocs[1].doc);
    assertEquals(2, group.scoreDocs[2].doc);
    assertTrue(group.scoreDocs[0].score > group.scoreDocs[1].score);
    assertTrue(group.scoreDocs[1].score > group.scoreDocs[2].score);

    group = groups.groups[2];
    assertEquals(new BytesRef("author2"), group.groupValue);
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
    final List<SortField> sortFields = new ArrayList<SortField>();
    if (random.nextInt(7) == 2) {
      sortFields.add(SortField.FIELD_SCORE);
    } else {
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
    }
    // Break ties:
    sortFields.add(new SortField("id", SortField.INT));
    return new Sort(sortFields.toArray(new SortField[sortFields.size()]));
  }

  private Comparator<GroupDoc> getComparator(Sort sort) {
    final SortField[] sortFields = sort.getSort();
    return new Comparator<GroupDoc>() {
      // @Override -- Not until Java 1.6
      public int compare(GroupDoc d1, GroupDoc d2) {
        for(SortField sf : sortFields) {
          final int cmp;
          if (sf.getType() == SortField.SCORE) {
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

  private Comparable<?>[] fillFields(GroupDoc d, Sort sort) {
    final SortField[] sortFields = sort.getSort();
    final Comparable<?>[] fields = new Comparable[sortFields.length];
    for(int fieldIDX=0;fieldIDX<sortFields.length;fieldIDX++) {
      final Comparable<?> c;
      final SortField sf = sortFields[fieldIDX];
      if (sf.getType() == SortField.SCORE) {
        c = new Float(d.score);
      } else if (sf.getField().equals("sort1")) {
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

  /*
  private String groupToString(BytesRef b) {
    if (b == null) {
      return "null";
    } else {
      return b.utf8ToString();
    }
  }
  */

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
    final HashMap<BytesRef,List<GroupDoc>> groups = new HashMap<BytesRef,List<GroupDoc>>();
    final List<BytesRef> sortedGroups = new ArrayList<BytesRef>();
    final List<Comparable<?>[]> sortedGroupFields = new ArrayList<Comparable<?>[]>();

    int totalHitCount = 0;
    Set<BytesRef> knownGroups = new HashSet<BytesRef>();

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
    @SuppressWarnings("unchecked")
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

      result[idx-groupOffset] = new GroupDocs<BytesRef>(0.0f,
                                              docs.size(),
                                              hits,
                                              group,
                                              fillFields ? sortedGroupFields.get(idx) : null);
    }

    if (doAllGroups) {
      return new TopGroups<BytesRef>(
          new TopGroups<BytesRef>(groupSort.getSort(), docSort.getSort(), totalHitCount, totalGroupedHitCount, result),
          knownGroups.size()
      );
    } else {
      return new TopGroups<BytesRef>(groupSort.getSort(), docSort.getSort(), totalHitCount, totalGroupedHitCount, result);
    }
  }

  private IndexReader getDocBlockReader(Directory dir, GroupDoc[] groupDocs) throws IOException {
    // Coalesce by group, but in random order:
    Collections.shuffle(Arrays.asList(groupDocs), random);
    final Map<BytesRef,List<GroupDoc>> groupMap = new HashMap<BytesRef,List<GroupDoc>>();
    final List<BytesRef> groupValues = new ArrayList<BytesRef>();
    
    for(GroupDoc groupDoc : groupDocs) {
      if (!groupMap.containsKey(groupDoc.group)) {
        groupValues.add(groupDoc.group);
        groupMap.put(groupDoc.group, new ArrayList<GroupDoc>());
      }
      groupMap.get(groupDoc.group).add(groupDoc);
    }

    RandomIndexWriter w = new RandomIndexWriter(
                                                random,
                                                dir,
                                                newIndexWriterConfig(TEST_VERSION_CURRENT,
                                                                     new MockAnalyzer(random)));

    final List<List<Document>> updateDocs = new ArrayList<List<Document>>();
    //System.out.println("TEST: index groups");
    for(BytesRef group : groupValues) {
      final List<Document> docs = new ArrayList<Document>();
      //System.out.println("TEST:   group=" + (group == null ? "null" : group.utf8ToString()));
      for(GroupDoc groupValue : groupMap.get(group)) {
        Document doc = new Document();
        docs.add(doc);
        if (groupValue.group != null) {
          doc.add(newField("group", groupValue.group.utf8ToString(), Field.Index.NOT_ANALYZED));
        }
        doc.add(newField("sort1", groupValue.sort1.utf8ToString(), Field.Index.NOT_ANALYZED));
        doc.add(newField("sort2", groupValue.sort2.utf8ToString(), Field.Index.NOT_ANALYZED));
        doc.add(new NumericField("id").setIntValue(groupValue.id));
        doc.add(newField("content", groupValue.content, Field.Index.ANALYZED));
        //System.out.println("TEST:     doc content=" + groupValue.content + " group=" + (groupValue.group == null ? "null" : groupValue.group.utf8ToString()) + " sort1=" + groupValue.sort1.utf8ToString() + " id=" + groupValue.id);
      }
      // So we can pull filter marking last doc in block:
      final Field groupEnd = newField("groupend", "x", Field.Index.NOT_ANALYZED);
      groupEnd.setOmitTermFreqAndPositions(true);
      groupEnd.setOmitNorms(true);
      docs.get(docs.size()-1).add(groupEnd);
      // Add as a doc block:
      w.addDocuments(docs);
      if (group != null && random.nextInt(7) == 4) {
        updateDocs.add(docs);
      }
    }

    for(List<Document> docs : updateDocs) {
      // Just replaces docs w/ same docs:
      w.updateDocuments(new Term("group", docs.get(0).get("group")),
                        docs);
    }

    final IndexReader r = w.getReader();
    w.close();

    return r;
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

      final List<BytesRef> groups = new ArrayList<BytesRef>();
      for(int i=0;i<numGroups;i++) {
        groups.add(new BytesRef(_TestUtil.randomRealisticUnicodeString(random)));
        //groups.add(new BytesRef(_TestUtil.randomSimpleString(random)));
      }
      final String[] contentStrings = new String[_TestUtil.nextInt(random, 2, 20)];
      if (VERBOSE) {
        System.out.println("TEST: create fake content");
      }
      for(int contentIDX=0;contentIDX<contentStrings.length;contentIDX++) {
        final StringBuilder sb = new StringBuilder();
        sb.append("real" + random.nextInt(3)).append(' ');
        final int fakeCount = random.nextInt(10);
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
                                                  random,
                                                  dir,
                                                  newIndexWriterConfig(TEST_VERSION_CURRENT,
                                                                       new MockAnalyzer(random)));

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
      Field content = newField("content", "", Field.Index.ANALYZED);
      doc.add(content);
      docNoGroup.add(content);
      NumericField id = new NumericField("id");
      doc.add(id);
      docNoGroup.add(id);
      final GroupDoc[] groupDocs = new GroupDoc[numDocs];
      for(int i=0;i<numDocs;i++) {
        final BytesRef groupValue;
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
          System.out.println("  doc content=" + groupDoc.content + " id=" + i + " group=" + (groupDoc.group == null ? "null" : groupDoc.group.utf8ToString()) + " sort1=" + groupDoc.sort1.utf8ToString() + " sort2=" + groupDoc.sort2.utf8ToString());
        }

        groupDocs[i] = groupDoc;
        if (groupDoc.group != null) {
          group.setValue(groupDoc.group.utf8ToString());
        }
        sort1.setValue(groupDoc.sort1.utf8ToString());
        sort2.setValue(groupDoc.sort2.utf8ToString());
        content.setValue(groupDoc.content);
        id.setIntValue(groupDoc.id);
        if (groupDoc.group == null) {
          w.addDocument(docNoGroup);
        } else {
          w.addDocument(doc);
        }
      }

      final GroupDoc[] groupDocsByID = new GroupDoc[groupDocs.length];
      System.arraycopy(groupDocs, 0, groupDocsByID, 0, groupDocs.length);

      final IndexReader r = w.getReader();
      w.close();

      // NOTE: intentional but temporary field cache insanity!
      final int[] docIDToID = FieldCache.DEFAULT.getInts(r, "id");
      IndexReader r2 = null;
      Directory dir2 = null;

      try {
        final IndexSearcher s = new IndexSearcher(r);

        for(int contentID=0;contentID<3;contentID++) {
          final ScoreDoc[] hits = s.search(new TermQuery(new Term("content", "real"+contentID)), numDocs).scoreDocs;
          for(ScoreDoc hit : hits) {
            final GroupDoc gd = groupDocs[docIDToID[hit.doc]];
            assertTrue(gd.score == 0.0);
            gd.score = hit.score;
            assertEquals(gd.id, docIDToID[hit.doc]);
            //System.out.println("  score=" + hit.score + " id=" + docIDToID[hit.doc]);
          }
        }

        for(GroupDoc gd : groupDocs) {
          assertTrue(gd.score != 0.0);
        }

        // Build 2nd index, where docs are added in blocks by
        // group, so we can use single pass collector
        dir2 = newDirectory();
        r2 = getDocBlockReader(dir2, groupDocs);
        final Filter lastDocInBlock = new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("groupend", "x"))));
        final int[] docIDToID2 = FieldCache.DEFAULT.getInts(r2, "id");

        final IndexSearcher s2 = new IndexSearcher(r2);

        // Reader2 only increases maxDoc() vs reader, which
        // means a monotonic shift in scores, so we can
        // reliably remap them w/ Map:
        final Map<String,Map<Float,Float>> scoreMap = new HashMap<String,Map<Float,Float>>();

        // Tricky: must separately set .score2, because the doc
        // block index was created with possible deletions!
        //System.out.println("fixup score2");
        for(int contentID=0;contentID<3;contentID++) {
          //System.out.println("  term=real" + contentID);
          final Map<Float,Float> termScoreMap = new HashMap<Float,Float>();
          scoreMap.put("real"+contentID, termScoreMap);
          //System.out.println("term=real" + contentID + " dfold=" + s.docFreq(new Term("content", "real"+contentID)) +
          //" dfnew=" + s2.docFreq(new Term("content", "real"+contentID)));
          final ScoreDoc[] hits = s2.search(new TermQuery(new Term("content", "real"+contentID)), numDocs).scoreDocs;
          for(ScoreDoc hit : hits) {
            final GroupDoc gd = groupDocsByID[docIDToID2[hit.doc]];
            assertTrue(gd.score2 == 0.0);
            gd.score2 = hit.score;
            assertEquals(gd.id, docIDToID2[hit.doc]);
            //System.out.println("    score=" + gd.score + " score2=" + hit.score + " id=" + docIDToID2[hit.doc]);
            termScoreMap.put(gd.score, gd.score2);
          }
        }

        for(int searchIter=0;searchIter<100;searchIter++) {

          if (VERBOSE) {
            System.out.println("TEST: searchIter=" + searchIter);
          }

          final String searchTerm = "real" + random.nextInt(3);
          final boolean fillFields = random.nextBoolean();
          boolean getScores = random.nextBoolean();
          final boolean getMaxScores = random.nextBoolean();
          final Sort groupSort = getRandomSort();
          //final Sort groupSort = new Sort(new SortField[] {new SortField("sort1", SortField.STRING), new SortField("id", SortField.INT)});
          // TODO: also test null (= sort by relevance)
          final Sort docSort = getRandomSort();

          for(SortField sf : docSort.getSort()) {
            if (sf.getType() == SortField.SCORE) {
              getScores = true;
            }
          }

          for(SortField sf : groupSort.getSort()) {
            if (sf.getType() == SortField.SCORE) {
              getScores = true;
            }
          }

          final int topNGroups = _TestUtil.nextInt(random, 1, 30);
          //final int topNGroups = 4;
          final int docsPerGroup = _TestUtil.nextInt(random, 1, 50);

          final int groupOffset = _TestUtil.nextInt(random, 0, (topNGroups-1)/2);
          //final int groupOffset = 0;

          final int docOffset = _TestUtil.nextInt(random, 0, docsPerGroup-1);
          //final int docOffset = 0;

          final boolean doCache = random.nextBoolean();
          final boolean doAllGroups = random.nextBoolean();
          if (VERBOSE) {
            System.out.println("TEST: groupSort=" + groupSort + " docSort=" + docSort + " searchTerm=" + searchTerm + " topNGroups=" + topNGroups + " groupOffset=" + groupOffset + " docOffset=" + docOffset + " doCache=" + doCache + " docsPerGroup=" + docsPerGroup + " doAllGroups=" + doAllGroups + " getScores=" + getScores + " getMaxScores=" + getMaxScores);
          }

          final TermAllGroupsCollector allGroupsCollector;
          if (doAllGroups) {
            allGroupsCollector = new TermAllGroupsCollector("group");
          } else {
            allGroupsCollector = null;
          }

          final TermFirstPassGroupingCollector c1 = new TermFirstPassGroupingCollector("group", groupSort, groupOffset+topNGroups);
          final CachingCollector cCache;
          final Collector c;
        
          final boolean useWrappingCollector = random.nextBoolean();
        
          if (doCache) {
            final double maxCacheMB = random.nextDouble();
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
              c = cCache = CachingCollector.create(false, true, maxCacheMB);
            }
          } else {
            cCache = null;
            if (doAllGroups) {
              c = MultiCollector.wrap(c1, allGroupsCollector);
            } else {
              c = c1;
            }
          }
        
          s.search(new TermQuery(new Term("content", searchTerm)), c);

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
              s.search(new TermQuery(new Term("content", searchTerm)), c1);
              if (doAllGroups) {
                s.search(new TermQuery(new Term("content", searchTerm)), allGroupsCollector);
              }
            }
          }

          final Collection<SearchGroup<BytesRef>> topGroups = c1.getTopGroups(groupOffset, fillFields);
          final TopGroups groupsResult;

          if (topGroups != null) {

            if (VERBOSE) {
              System.out.println("TEST: topGroups");
              for (SearchGroup<BytesRef> searchGroup : topGroups) {
                System.out.println("  " + (searchGroup.groupValue == null ? "null" : searchGroup.groupValue.utf8ToString()) + ": " + Arrays.deepToString(searchGroup.sortValues));
              }
            }

            final TermSecondPassGroupingCollector c2 = new TermSecondPassGroupingCollector("group", topGroups, groupSort, docSort, docOffset+docsPerGroup, getScores, getMaxScores, fillFields);
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
              TopGroups<BytesRef> tempTopGroups = c2.getTopGroups(docOffset);
              groupsResult = new TopGroups<BytesRef>(tempTopGroups, allGroupsCollector.getGroupCount());
            } else {
              groupsResult = c2.getTopGroups(docOffset);
            }
          } else {
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
              System.out.println("TEST: expected groups");
              for(GroupDocs<BytesRef> gd : expectedGroups.groups) {
                System.out.println("  group=" + (gd.groupValue == null ? "null" : gd.groupValue.utf8ToString()));
                for(ScoreDoc sd : gd.scoreDocs) {
                  System.out.println("    id=" + sd.doc + " score=" + sd.score);
                }
              }
            }
          }
          assertEquals(docIDToID, expectedGroups, groupsResult, true, getScores);

          final boolean needsScores = getScores || getMaxScores || docSort == null;
          final BlockGroupingCollector c3 = new BlockGroupingCollector(groupSort, groupOffset+topNGroups, needsScores, lastDocInBlock);
          final TermAllGroupsCollector allGroupsCollector2;
          final Collector c4;
          if (doAllGroups) {
            allGroupsCollector2 = new TermAllGroupsCollector("group");
            c4 = MultiCollector.wrap(c3, allGroupsCollector2);
          } else {
            allGroupsCollector2 = null;
            c4 = c3;
          }
          s2.search(new TermQuery(new Term("content", searchTerm)), c4);
          @SuppressWarnings("unchecked")
          final TopGroups<BytesRef> tempTopGroups2 = c3.getTopGroups(docSort, groupOffset, docOffset, docOffset+docsPerGroup, fillFields);
          final TopGroups groupsResult2;
          if (doAllGroups && tempTopGroups2 != null) {
            assertEquals((int) tempTopGroups2.totalGroupCount, allGroupsCollector2.getGroupCount());
            groupsResult2 = new TopGroups<BytesRef>(tempTopGroups2, allGroupsCollector2.getGroupCount());
          } else {
            groupsResult2 = tempTopGroups2;
          }

          if (expectedGroups != null) {
            // Fixup scores for reader2
            for (GroupDocs groupDocsHits : expectedGroups.groups) {
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
              if (sortFields[groupSortIDX].getType() == SortField.SCORE) {
                for (GroupDocs groupDocsHits : expectedGroups.groups) {
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
              if (docSortFields[docSortIDX].getType() == SortField.SCORE) {
                for (GroupDocs groupDocsHits : expectedGroups.groups) {
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

          assertEquals(docIDToID2, expectedGroups, groupsResult2, false, getScores);
        }
      } finally {
        FieldCache.DEFAULT.purge(r);
        if (r2 != null) {
          FieldCache.DEFAULT.purge(r2);
        }
      }

      r.close();
      dir.close();

      r2.close();
      dir2.close();
    }
  }

  private void assertEquals(int[] docIDtoID, TopGroups expected, TopGroups actual, boolean verifyGroupValues, boolean testScores) {
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
      if (verifyGroupValues) {
        assertEquals(expectedGroup.groupValue, actualGroup.groupValue);
      }
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
        //System.out.println("  actual doc=" + docIDtoID[actualFD.doc] + " score=" + actualFD.score);
        assertEquals(expectedFD.doc, docIDtoID[actualFD.doc]);
        if (testScores) {
          assertEquals(expectedFD.score, actualFD.score);
        } else {
          // TODO: too anal for now
          //assertEquals(Float.NaN, actualFD.score);
        }
        assertArrayEquals(expectedFD.fields, actualFD.fields);
      }
    }
  }
}
