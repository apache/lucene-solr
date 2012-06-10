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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.dv.DVGroupFacetCollector;
import org.apache.lucene.search.grouping.term.TermGroupFacetCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util._TestUtil;

import java.io.IOException;
import java.util.*;

public class GroupFacetCollectorTest extends AbstractGroupingTestCase {

  public void testSimple() throws Exception {
    final String groupField = "hotel";
    FieldType customType = new FieldType();
    customType.setStored(true);

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random(),
        dir,
        newIndexWriterConfig(TEST_VERSION_CURRENT,
            new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    boolean canUseDV = !"Lucene3x".equals(w.w.getConfig().getCodec().getName());
    boolean useDv = canUseDV && random().nextBoolean();

    // 0
    Document doc = new Document();
    addField(doc, groupField, "a", canUseDV);
    addField(doc, "airport", "ams", canUseDV);
    addField(doc, "duration", "5", canUseDV);
    w.addDocument(doc);

    // 1
    doc = new Document();
    addField(doc, groupField, "a", canUseDV);
    addField(doc, "airport", "dus", canUseDV);
    addField(doc, "duration", "10", canUseDV);
    w.addDocument(doc);

    // 2
    doc = new Document();
    addField(doc, groupField, "b", canUseDV);
    addField(doc, "airport", "ams", canUseDV);
    addField(doc, "duration", "10", canUseDV);
    w.addDocument(doc);
    w.commit(); // To ensure a second segment

    // 3
    doc = new Document();
    addField(doc, groupField, "b", canUseDV);
    addField(doc, "airport", "ams", canUseDV);
    addField(doc, "duration", "5", canUseDV);
    w.addDocument(doc);

    // 4
    doc = new Document();
    addField(doc, groupField, "b", canUseDV);
    addField(doc, "airport", "ams", canUseDV);
    addField(doc, "duration", "5", canUseDV);
    w.addDocument(doc);

    IndexSearcher indexSearcher = new IndexSearcher(w.getReader());
    AbstractGroupFacetCollector groupedAirportFacetCollector = createRandomCollector(groupField, "airport", null, false, useDv);
    indexSearcher.search(new MatchAllDocsQuery(), groupedAirportFacetCollector);
    TermGroupFacetCollector.GroupedFacetResult airportResult = groupedAirportFacetCollector.mergeSegmentResults(10, 0, false);
    assertEquals(3, airportResult.getTotalCount());
    assertEquals(0, airportResult.getTotalMissingCount());

    List<TermGroupFacetCollector.FacetEntry> entries = airportResult.getFacetEntries(0, 10);
    assertEquals(2, entries.size());
    assertEquals("ams", entries.get(0).getValue().utf8ToString());
    assertEquals(2, entries.get(0).getCount());
    assertEquals("dus", entries.get(1).getValue().utf8ToString());
    assertEquals(1, entries.get(1).getCount());


    AbstractGroupFacetCollector groupedDurationFacetCollector = createRandomCollector(groupField, "duration", null, false, useDv);
    indexSearcher.search(new MatchAllDocsQuery(), groupedDurationFacetCollector);
    TermGroupFacetCollector.GroupedFacetResult durationResult = groupedDurationFacetCollector.mergeSegmentResults(10, 0, false);
    assertEquals(4, durationResult.getTotalCount());
    assertEquals(0, durationResult.getTotalMissingCount());

    entries = durationResult.getFacetEntries(0, 10);
    assertEquals(2, entries.size());
    assertEquals("10", entries.get(0).getValue().utf8ToString());
    assertEquals(2, entries.get(0).getCount());
    assertEquals("5", entries.get(1).getValue().utf8ToString());
    assertEquals(2, entries.get(1).getCount());

    // 5
    doc = new Document();
    addField(doc, groupField, "b", canUseDV);
    addField(doc, "duration", "5", canUseDV);
    w.addDocument(doc);

    // 6
    doc = new Document();
    addField(doc, groupField, "b", canUseDV);
    addField(doc, "airport", "bru", canUseDV);
    addField(doc, "duration", "10", canUseDV);
    w.addDocument(doc);

    // 7
    doc = new Document();
    addField(doc, groupField, "b", canUseDV);
    addField(doc, "airport", "bru", canUseDV);
    addField(doc, "duration", "15", canUseDV);
    w.addDocument(doc);

    // 8
    doc = new Document();
    addField(doc, groupField, "a", canUseDV);
    addField(doc, "airport", "bru", canUseDV);
    addField(doc, "duration", "10", canUseDV);
    w.addDocument(doc);

    indexSearcher.getIndexReader().close();
    indexSearcher = new IndexSearcher(w.getReader());
    groupedAirportFacetCollector = createRandomCollector(groupField, "airport", null, true, useDv);
    indexSearcher.search(new MatchAllDocsQuery(), groupedAirportFacetCollector);
    airportResult = groupedAirportFacetCollector.mergeSegmentResults(3, 0, true);
    assertEquals(5, airportResult.getTotalCount());
    assertEquals(1, airportResult.getTotalMissingCount());

    entries = airportResult.getFacetEntries(1, 2);
    assertEquals(2, entries.size());
    assertEquals("bru", entries.get(0).getValue().utf8ToString());
    assertEquals(2, entries.get(0).getCount());
    assertEquals("dus", entries.get(1).getValue().utf8ToString());
    assertEquals(1, entries.get(1).getCount());

    groupedDurationFacetCollector = createRandomCollector(groupField, "duration", null, false, useDv);
    indexSearcher.search(new MatchAllDocsQuery(), groupedDurationFacetCollector);
    durationResult = groupedDurationFacetCollector.mergeSegmentResults(10, 2, true);
    assertEquals(5, durationResult.getTotalCount());
    assertEquals(0, durationResult.getTotalMissingCount());

    entries = durationResult.getFacetEntries(1, 1);
    assertEquals(1, entries.size());
    assertEquals("5", entries.get(0).getValue().utf8ToString());
    assertEquals(2, entries.get(0).getCount());

    // 9
    doc = new Document();
    addField(doc, groupField, "c", canUseDV);
    addField(doc, "airport", "bru", canUseDV);
    addField(doc, "duration", "15", canUseDV);
    w.addDocument(doc);

    // 10
    doc = new Document();
    addField(doc, groupField, "c", canUseDV);
    addField(doc, "airport", "dus", canUseDV);
    addField(doc, "duration", "10", canUseDV);
    w.addDocument(doc);

    indexSearcher.getIndexReader().close();
    indexSearcher = new IndexSearcher(w.getReader());
    groupedAirportFacetCollector = createRandomCollector(groupField, "airport", null, false, useDv);
    indexSearcher.search(new MatchAllDocsQuery(), groupedAirportFacetCollector);
    airportResult = groupedAirportFacetCollector.mergeSegmentResults(10, 0, false);
    assertEquals(7, airportResult.getTotalCount());
    assertEquals(1, airportResult.getTotalMissingCount());

    entries = airportResult.getFacetEntries(0, 10);
    assertEquals(3, entries.size());
    assertEquals("ams", entries.get(0).getValue().utf8ToString());
    assertEquals(2, entries.get(0).getCount());
    assertEquals("bru", entries.get(1).getValue().utf8ToString());
    assertEquals(3, entries.get(1).getCount());
    assertEquals("dus", entries.get(2).getValue().utf8ToString());
    assertEquals(2, entries.get(2).getCount());

    groupedDurationFacetCollector = createRandomCollector(groupField, "duration", "1", false, useDv);
    indexSearcher.search(new MatchAllDocsQuery(), groupedDurationFacetCollector);
    durationResult = groupedDurationFacetCollector.mergeSegmentResults(10, 0, true);
    assertEquals(5, durationResult.getTotalCount());
    assertEquals(0, durationResult.getTotalMissingCount());

    entries = durationResult.getFacetEntries(0, 10);
    assertEquals(2, entries.size());
    assertEquals("10", entries.get(0).getValue().utf8ToString());
    assertEquals(3, entries.get(0).getCount());
    assertEquals("15", entries.get(1).getValue().utf8ToString());
    assertEquals(2, entries.get(1).getCount());

    w.close();
    indexSearcher.getIndexReader().close();
    dir.close();
  }

  private void addField(Document doc, String field, String value, boolean canUseIDV) {
    doc.add(new StringField(field, value, Field.Store.NO));
    if (canUseIDV) {
      doc.add(new SortedBytesDocValuesField(field, new BytesRef(value)));
    }
  }

  public void testRandom() throws Exception {
    Random random = random();
    int numberOfRuns = _TestUtil.nextInt(random, 3, 6);
    for (int indexIter = 0; indexIter < numberOfRuns; indexIter++) {
      boolean multipleFacetsPerDocument = random.nextBoolean();
      IndexContext context = createIndexContext(multipleFacetsPerDocument);
      final IndexSearcher searcher = newSearcher(context.indexReader);

      for (int searchIter = 0; searchIter < 100; searchIter++) {
        boolean useDv = context.useDV && random.nextBoolean();
        String searchTerm = context.contentStrings[random.nextInt(context.contentStrings.length)];
        int limit = random.nextInt(context.facetValues.size());
        int offset = random.nextInt(context.facetValues.size() - limit);
        int size = offset + limit;
        int minCount = random.nextBoolean() ? 0 : random.nextInt(1 + context.facetWithMostGroups / 10);
        boolean orderByCount = random.nextBoolean();
        String randomStr = getFromSet(context.facetValues, random.nextInt(context.facetValues.size()));
        final String facetPrefix;
        if (randomStr == null) {
          facetPrefix = null;
        } else {
          int codePointLen = randomStr.codePointCount(0, randomStr.length());
          int randomLen = random.nextInt(codePointLen);
          if (codePointLen == randomLen - 1) {
            facetPrefix = null;
          } else {
            int end = randomStr.offsetByCodePoints(0, randomLen);
            facetPrefix = random.nextBoolean() ? null : randomStr.substring(end);
          }
        }

        GroupedFacetResult expectedFacetResult = createExpectedFacetResult(searchTerm, context, offset, limit, minCount, orderByCount, facetPrefix);
        AbstractGroupFacetCollector groupFacetCollector = createRandomCollector("group", "facet", facetPrefix, multipleFacetsPerDocument, useDv);
        searcher.search(new TermQuery(new Term("content", searchTerm)), groupFacetCollector);
        TermGroupFacetCollector.GroupedFacetResult actualFacetResult = groupFacetCollector.mergeSegmentResults(size, minCount, orderByCount);

        List<TermGroupFacetCollector.FacetEntry> expectedFacetEntries = expectedFacetResult.getFacetEntries();
        List<TermGroupFacetCollector.FacetEntry> actualFacetEntries = actualFacetResult.getFacetEntries(offset, limit);

        if (VERBOSE) {
          System.out.println("Collector: " + groupFacetCollector.getClass().getSimpleName());
          System.out.println("Num group: " + context.numGroups);
          System.out.println("Num doc: " + context.numDocs);
          System.out.println("Index iter: " + indexIter);
          System.out.println("multipleFacetsPerDocument: " + multipleFacetsPerDocument);
          System.out.println("Search iter: " + searchIter);

          System.out.println("Search term: " + searchTerm);
          System.out.println("Min count: " + minCount);
          System.out.println("Facet offset: " + offset);
          System.out.println("Facet limit: " + limit);
          System.out.println("Facet prefix: " + facetPrefix);
          System.out.println("Order by count: " + orderByCount);

          System.out.println("\n=== Expected: \n");
          System.out.println("Total count " + expectedFacetResult.getTotalCount());
          System.out.println("Total missing count " + expectedFacetResult.getTotalMissingCount());
          int counter = 1;
          for (TermGroupFacetCollector.FacetEntry expectedFacetEntry : expectedFacetEntries) {
            System.out.println(
                String.format(
                    "%d. Expected facet value %s with count %d",
                    counter++, expectedFacetEntry.getValue().utf8ToString(), expectedFacetEntry.getCount()
                )
            );
          }

          System.out.println("\n=== Actual: \n");
          System.out.println("Total count " + actualFacetResult.getTotalCount());
          System.out.println("Total missing count " + actualFacetResult.getTotalMissingCount());
          counter = 1;
          for (TermGroupFacetCollector.FacetEntry actualFacetEntry : actualFacetEntries) {
            System.out.println(
                String.format(
                    "%d. Actual facet value %s with count %d",
                    counter++, actualFacetEntry.getValue().utf8ToString(), actualFacetEntry.getCount()
                )
            );
          }
          System.out.println("\n===================================================================================");
        }

        assertEquals(expectedFacetResult.getTotalCount(), actualFacetResult.getTotalCount());
        assertEquals(expectedFacetResult.getTotalMissingCount(), actualFacetResult.getTotalMissingCount());
        assertEquals(expectedFacetEntries.size(), actualFacetEntries.size());
        for (int i = 0; i < expectedFacetEntries.size(); i++) {
          TermGroupFacetCollector.FacetEntry expectedFacetEntry = expectedFacetEntries.get(i);
          TermGroupFacetCollector.FacetEntry actualFacetEntry = actualFacetEntries.get(i);
          assertEquals(expectedFacetEntry.getValue().utf8ToString() + " != " + actualFacetEntry.getValue().utf8ToString(), expectedFacetEntry.getValue(), actualFacetEntry.getValue());
          assertEquals(expectedFacetEntry.getCount() + " != " + actualFacetEntry.getCount(), expectedFacetEntry.getCount(), actualFacetEntry.getCount());
        }
      }

      context.indexReader.close();
      context.dir.close();
    }
  }

  private IndexContext createIndexContext(boolean multipleFacetValuesPerDocument) throws IOException {
    final Random random = random();
    final int numDocs = _TestUtil.nextInt(random, 138, 1145) * RANDOM_MULTIPLIER;
    final int numGroups = _TestUtil.nextInt(random, 1, numDocs / 4);
    final int numFacets = _TestUtil.nextInt(random, 1, numDocs / 6);

    if (VERBOSE) {
      System.out.println("TEST: numDocs=" + numDocs + " numGroups=" + numGroups);
    }

    final List<String> groups = new ArrayList<String>();
    for (int i = 0; i < numGroups; i++) {
      groups.add(generateRandomNonEmptyString());
    }
    final List<String> facetValues = new ArrayList<String>();
    for (int i = 0; i < numFacets; i++) {
      facetValues.add(generateRandomNonEmptyString());
    }
    final String[] contentBrs = new String[_TestUtil.nextInt(random, 2, 20)];
    if (VERBOSE) {
      System.out.println("TEST: create fake content");
    }
    for (int contentIDX = 0; contentIDX < contentBrs.length; contentIDX++) {
      contentBrs[contentIDX] = generateRandomNonEmptyString();
      if (VERBOSE) {
        System.out.println("  content=" + contentBrs[contentIDX]);
      }
    }

    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(
        random,
        dir,
        newIndexWriterConfig(
            TEST_VERSION_CURRENT,
            new MockAnalyzer(random)
        )
    );
    boolean canUseDV = !"Lucene3x".equals(writer.w.getConfig().getCodec().getName());
    boolean useDv = canUseDV && random.nextBoolean();

    Document doc = new Document();
    Document docNoGroup = new Document();
    Document docNoFacet = new Document();
    Document docNoGroupNoFacet = new Document();
    Field group = newStringField("group", "", Field.Store.NO);
    Field groupDc = new SortedBytesDocValuesField("group", new BytesRef());
    if (useDv) {
      doc.add(groupDc);
      docNoFacet.add(groupDc);
    }
    doc.add(group);
    docNoFacet.add(group);
    Field[] facetFields;
    if (useDv) {
      facetFields = new Field[2];
      facetFields[0] = newStringField("facet", "", Field.Store.NO);
      doc.add(facetFields[0]);
      docNoGroup.add(facetFields[0]);
      facetFields[1] = new SortedBytesDocValuesField("facet", new BytesRef());
      doc.add(facetFields[1]);
      docNoGroup.add(facetFields[1]);
    } else {
      facetFields = multipleFacetValuesPerDocument ? new Field[2 + random.nextInt(6)] : new Field[1];
      for (int i = 0; i < facetFields.length; i++) {
        facetFields[i] = newStringField("facet", "", Field.Store.NO);
        doc.add(facetFields[i]);
        docNoGroup.add(facetFields[i]);
      }
    }
    Field content = newStringField("content", "", Field.Store.NO);
    doc.add(content);
    docNoGroup.add(content);
    docNoFacet.add(content);
    docNoGroupNoFacet.add(content);

    NavigableSet<String> uniqueFacetValues = new TreeSet<String>(new Comparator<String>() {

      public int compare(String a, String b) {
        if (a == b) {
          return 0;
        } else if (a == null) {
          return -1;
        } else if (b == null) {
          return 1;
        } else {
          return a.compareTo(b);
        }
      }

    });
    Map<String, Map<String, Set<String>>> searchTermToFacetToGroups = new HashMap<String, Map<String, Set<String>>>();
    int facetWithMostGroups = 0;
    for (int i = 0; i < numDocs; i++) {
      final String groupValue;
      if (random.nextInt(24) == 17) {
        // So we test the "doc doesn't have the group'd
        // field" case:
        groupValue = null;
      } else {
        groupValue = groups.get(random.nextInt(groups.size()));
      }

      String contentStr = contentBrs[random.nextInt(contentBrs.length)];
      if (!searchTermToFacetToGroups.containsKey(contentStr)) {
        searchTermToFacetToGroups.put(contentStr, new HashMap<String, Set<String>>());
      }
      Map<String, Set<String>> facetToGroups = searchTermToFacetToGroups.get(contentStr);

      List<String> facetVals = new ArrayList<String>();
      if (random.nextInt(24) != 18) {
        if (useDv) {
          String facetValue = facetValues.get(random.nextInt(facetValues.size()));
          uniqueFacetValues.add(facetValue);
          if (!facetToGroups.containsKey(facetValue)) {
            facetToGroups.put(facetValue, new HashSet<String>());
          }
          Set<String> groupsInFacet = facetToGroups.get(facetValue);
          groupsInFacet.add(groupValue);
          if (groupsInFacet.size() > facetWithMostGroups) {
            facetWithMostGroups = groupsInFacet.size();
          }
          facetFields[0].setStringValue(facetValue);
          facetFields[1].setBytesValue(new BytesRef(facetValue));
          facetVals.add(facetValue);
        } else {
          for (Field facetField : facetFields) {
            String facetValue = facetValues.get(random.nextInt(facetValues.size()));
            uniqueFacetValues.add(facetValue);
            if (!facetToGroups.containsKey(facetValue)) {
              facetToGroups.put(facetValue, new HashSet<String>());
            }
            Set<String> groupsInFacet = facetToGroups.get(facetValue);
            groupsInFacet.add(groupValue);
            if (groupsInFacet.size() > facetWithMostGroups) {
              facetWithMostGroups = groupsInFacet.size();
            }
            facetField.setStringValue(facetValue);
            facetVals.add(facetValue);
          }
        }
      } else {
        uniqueFacetValues.add(null);
        if (!facetToGroups.containsKey(null)) {
          facetToGroups.put(null, new HashSet<String>());
        }
        Set<String> groupsInFacet = facetToGroups.get(null);
        groupsInFacet.add(groupValue);
        if (groupsInFacet.size() > facetWithMostGroups) {
          facetWithMostGroups = groupsInFacet.size();
        }
      }

      if (VERBOSE) {
        System.out.println("  doc content=" + contentStr + " group=" + (groupValue == null ? "null" : groupValue) + " facetVals=" + facetVals);
      }

      if (groupValue != null) {
        if (useDv) {
          groupDc.setBytesValue(new BytesRef(groupValue));
        }
        group.setStringValue(groupValue);
      }
      content.setStringValue(contentStr);
      if (groupValue == null && facetVals.isEmpty()) {
        writer.addDocument(docNoGroupNoFacet);
      } else if (facetVals.isEmpty()) {
        writer.addDocument(docNoFacet);
      } else if (groupValue == null) {
        writer.addDocument(docNoGroup);
      } else {
        writer.addDocument(doc);
      }
    }

    DirectoryReader reader = writer.getReader();
    writer.close();

    return new IndexContext(searchTermToFacetToGroups, reader, numDocs, dir, facetWithMostGroups, numGroups, contentBrs, uniqueFacetValues, useDv);
  }

  private GroupedFacetResult createExpectedFacetResult(String searchTerm, IndexContext context, int offset, int limit, int minCount, final boolean orderByCount, String facetPrefix) {
    Map<String, Set<String>> facetGroups = context.searchTermToFacetGroups.get(searchTerm);
    if (facetGroups == null) {
      facetGroups = new HashMap<String, Set<String>>();
    }

    int totalCount = 0;
    int totalMissCount = 0;
    Set<String> facetValues;
    if (facetPrefix != null) {
      facetValues = new HashSet<String>();
      for (String facetValue : context.facetValues) {
        if (facetValue != null && facetValue.startsWith(facetPrefix)) {
          facetValues.add(facetValue);
        }
      }
    } else {
      facetValues = context.facetValues;
    }

    List<TermGroupFacetCollector.FacetEntry> entries = new ArrayList<TermGroupFacetCollector.FacetEntry>(facetGroups.size());
    // also includes facets with count 0
    for (String facetValue : facetValues) {
      if (facetValue == null) {
        continue;
      }

      Set<String> groups = facetGroups.get(facetValue);
      int count = groups != null ? groups.size() : 0;
      if (count >= minCount) {
        entries.add(new TermGroupFacetCollector.FacetEntry(new BytesRef(facetValue), count));
      }
      totalCount += count;
    }

    // Only include null count when no facet prefix is specified
    if (facetPrefix == null) {
      Set<String> groups = facetGroups.get(null);
      if (groups != null) {
        totalMissCount = groups.size();
      }
    }

    Collections.sort(entries, new Comparator<TermGroupFacetCollector.FacetEntry>() {

      public int compare(TermGroupFacetCollector.FacetEntry a, TermGroupFacetCollector.FacetEntry b) {
        if (orderByCount) {
          int cmp = b.getCount() - a.getCount();
          if (cmp != 0) {
            return cmp;
          }
        }
        return a.getValue().compareTo(b.getValue());
      }

    });

    int endOffset = offset + limit;
    List<TermGroupFacetCollector.FacetEntry> entriesResult;
    if (offset >= entries.size()) {
      entriesResult = Collections.emptyList();
    } else if (endOffset >= entries.size()) {
      entriesResult = entries.subList(offset, entries.size());
    } else {
      entriesResult = entries.subList(offset, endOffset);
    }
    return new GroupedFacetResult(totalCount, totalMissCount, entriesResult);
  }

  private AbstractGroupFacetCollector createRandomCollector(String groupField, String facetField, String facetPrefix, boolean multipleFacetsPerDocument, boolean useDv) {
    BytesRef facetPrefixBR = facetPrefix == null ? null : new BytesRef(facetPrefix);
    if (useDv) {
      return DVGroupFacetCollector.createDvGroupFacetCollector(groupField, DocValues.Type.BYTES_VAR_SORTED,
          random().nextBoolean(), facetField, DocValues.Type.BYTES_VAR_SORTED, random().nextBoolean(), facetPrefixBR, random().nextInt(1024));
    } else {
      return TermGroupFacetCollector.createTermGroupFacetCollector(groupField, facetField, multipleFacetsPerDocument, facetPrefixBR, random().nextInt(1024));
    }
  }

  private String getFromSet(Set<String> set, int index) {
    int currentIndex = 0;
    for (String bytesRef : set) {
      if (currentIndex++ == index) {
        return bytesRef;
      }
    }

    return null;
  }

  private class IndexContext {

    final int numDocs;
    final DirectoryReader indexReader;
    final Map<String, Map<String, Set<String>>> searchTermToFacetGroups;
    final NavigableSet<String> facetValues;
    final Directory dir;
    final int facetWithMostGroups;
    final int numGroups;
    final String[] contentStrings;
    final boolean useDV;

    public IndexContext(Map<String, Map<String, Set<String>>> searchTermToFacetGroups, DirectoryReader r,
                        int numDocs, Directory dir, int facetWithMostGroups, int numGroups, String[] contentStrings, NavigableSet<String> facetValues, boolean useDV) {
      this.searchTermToFacetGroups = searchTermToFacetGroups;
      this.indexReader = r;
      this.numDocs = numDocs;
      this.dir = dir;
      this.facetWithMostGroups = facetWithMostGroups;
      this.numGroups = numGroups;
      this.contentStrings = contentStrings;
      this.facetValues = facetValues;
      this.useDV = useDV;
    }
  }

  private class GroupedFacetResult {

    final int totalCount;
    final int totalMissingCount;
    final List<TermGroupFacetCollector.FacetEntry> facetEntries;

    private GroupedFacetResult(int totalCount, int totalMissingCount, List<TermGroupFacetCollector.FacetEntry> facetEntries) {
      this.totalCount = totalCount;
      this.totalMissingCount = totalMissingCount;
      this.facetEntries = facetEntries;
    }

    public int getTotalCount() {
      return totalCount;
    }

    public int getTotalMissingCount() {
      return totalMissingCount;
    }

    public List<TermGroupFacetCollector.FacetEntry> getFacetEntries() {
      return facetEntries;
    }
  }

}
