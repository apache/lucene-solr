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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.function.FunctionDistinctValuesCollector;
import org.apache.lucene.search.grouping.function.FunctionFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermDistinctValuesCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;


public class DistinctValuesCollectorTest extends AbstractGroupingTestCase {

  private final static NullComparator nullComparator = new NullComparator();
  
  private final String groupField = "author";
  private final String countField = "publisher";

  public void testSimple() throws Exception {
    Random random = random();
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random,
        dir,
        newIndexWriterConfig(new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));
    Document doc = new Document();
    addField(doc, groupField, "1");
    addField(doc, countField, "1");
    doc.add(new TextField("content", "random text", Field.Store.NO));
    doc.add(new StringField("id", "1", Field.Store.NO));
    w.addDocument(doc);

    // 1
    doc = new Document();
    addField(doc, groupField, "1");
    addField(doc, countField, "1");
    doc.add(new TextField("content", "some more random text blob", Field.Store.NO));
    doc.add(new StringField("id", "2", Field.Store.NO));
    w.addDocument(doc);

    // 2
    doc = new Document();
    addField(doc, groupField, "1");
    addField(doc, countField, "2");
    doc.add(new TextField("content", "some more random textual data", Field.Store.NO));
    doc.add(new StringField("id", "3", Field.Store.NO));
    w.addDocument(doc);
    w.commit(); // To ensure a second segment

    // 3 -- no count field
    doc = new Document();
    addField(doc, groupField, "2");
    doc.add(new TextField("content", "some random text", Field.Store.NO));
    doc.add(new StringField("id", "4", Field.Store.NO));
    w.addDocument(doc);

    // 4
    doc = new Document();
    addField(doc, groupField, "3");
    addField(doc, countField, "1");
    doc.add(new TextField("content", "some more random text", Field.Store.NO));
    doc.add(new StringField("id", "5", Field.Store.NO));
    w.addDocument(doc);

    // 5
    doc = new Document();
    addField(doc, groupField, "3");
    addField(doc, countField, "1");
    doc.add(new TextField("content", "random blob", Field.Store.NO));
    doc.add(new StringField("id", "6", Field.Store.NO));
    w.addDocument(doc);

    // 6 -- no author field
    doc = new Document();
    doc.add(new TextField("content", "random word stuck in alot of other text", Field.Store.YES));
    addField(doc, countField, "1");
    doc.add(new StringField("id", "6", Field.Store.NO));
    w.addDocument(doc);

    IndexSearcher indexSearcher = newSearcher(w.getReader());
    w.close();

    Comparator<DistinctValuesCollector.GroupCount<Comparable<Object>>> cmp = new Comparator<DistinctValuesCollector.GroupCount<Comparable<Object>>>() {

      @Override
      public int compare(DistinctValuesCollector.GroupCount<Comparable<Object>> groupCount1, DistinctValuesCollector.GroupCount<Comparable<Object>> groupCount2) {
        if (groupCount1.groupValue == null) {
          if (groupCount2.groupValue == null) {
            return 0;
          }
          return -1;
        } else if (groupCount2.groupValue == null) {
          return 1;
        } else {
          return groupCount1.groupValue.compareTo(groupCount2.groupValue);
        }
      }

    };

    // === Search for content:random
    FirstPassGroupingCollector<Comparable<Object>> firstCollector = createRandomFirstPassCollector(new Sort(), groupField, 10);
    indexSearcher.search(new TermQuery(new Term("content", "random")), firstCollector);
    DistinctValuesCollector<Comparable<Object>> distinctValuesCollector
        = createDistinctCountCollector(firstCollector, groupField, countField);
    indexSearcher.search(new TermQuery(new Term("content", "random")), distinctValuesCollector);

    List<DistinctValuesCollector.GroupCount<Comparable<Object>>> gcs = distinctValuesCollector.getGroups();
    Collections.sort(gcs, cmp);
    assertEquals(4, gcs.size());

    compareNull(gcs.get(0).groupValue);
    List<Comparable<?>> countValues = new ArrayList<Comparable<?>>(gcs.get(0).uniqueValues);
    assertEquals(1, countValues.size());
    compare("1", countValues.get(0));

    compare("1", gcs.get(1).groupValue);
    countValues = new ArrayList<Comparable<?>>(gcs.get(1).uniqueValues);
    Collections.sort(countValues, nullComparator);
    assertEquals(2, countValues.size());
    compare("1", countValues.get(0));
    compare("2", countValues.get(1));

    compare("2", gcs.get(2).groupValue);
    countValues = new ArrayList<Comparable<?>>(gcs.get(2).uniqueValues);
    assertEquals(1, countValues.size());
    compareNull(countValues.get(0));

    compare("3", gcs.get(3).groupValue);
    countValues = new ArrayList<Comparable<?>>(gcs.get(3).uniqueValues);
    assertEquals(1, countValues.size());
    compare("1", countValues.get(0));

    // === Search for content:some
    firstCollector = createRandomFirstPassCollector(new Sort(), groupField, 10);
    indexSearcher.search(new TermQuery(new Term("content", "some")), firstCollector);
    distinctValuesCollector = createDistinctCountCollector(firstCollector, groupField, countField);
    indexSearcher.search(new TermQuery(new Term("content", "some")), distinctValuesCollector);

    gcs = distinctValuesCollector.getGroups();
    Collections.sort(gcs, cmp);
    assertEquals(3, gcs.size());

    compare("1", gcs.get(0).groupValue);
    countValues = new ArrayList<Comparable<?>>(gcs.get(0).uniqueValues);
    assertEquals(2, countValues.size());
    Collections.sort(countValues, nullComparator);
    compare("1", countValues.get(0));
    compare("2", countValues.get(1));

    compare("2", gcs.get(1).groupValue);
    countValues = new ArrayList<Comparable<?>>(gcs.get(1).uniqueValues);
    assertEquals(1, countValues.size());
    compareNull(countValues.get(0));

    compare("3", gcs.get(2).groupValue);
    countValues = new ArrayList<Comparable<?>>(gcs.get(2).uniqueValues);
    assertEquals(1, countValues.size());
    compare("1", countValues.get(0));

     // === Search for content:blob
    firstCollector = createRandomFirstPassCollector(new Sort(), groupField, 10);
    indexSearcher.search(new TermQuery(new Term("content", "blob")), firstCollector);
    distinctValuesCollector = createDistinctCountCollector(firstCollector, groupField, countField);
    indexSearcher.search(new TermQuery(new Term("content", "blob")), distinctValuesCollector);

    gcs = distinctValuesCollector.getGroups();
    Collections.sort(gcs, cmp);
    assertEquals(2, gcs.size());

    compare("1", gcs.get(0).groupValue);
    countValues = new ArrayList<Comparable<?>>(gcs.get(0).uniqueValues);
    // B/c the only one document matched with blob inside the author 1 group
    assertEquals(1, countValues.size());
    compare("1", countValues.get(0));

    compare("3", gcs.get(1).groupValue);
    countValues = new ArrayList<Comparable<?>>(gcs.get(1).uniqueValues);
    assertEquals(1, countValues.size());
    compare("1", countValues.get(0));

    indexSearcher.getIndexReader().close();
    dir.close();
  }

  public void testRandom() throws Exception {
    Random random = random();
    int numberOfRuns = TestUtil.nextInt(random, 3, 6);
    for (int indexIter = 0; indexIter < numberOfRuns; indexIter++) {
      IndexContext context = createIndexContext();
      for (int searchIter = 0; searchIter < 100; searchIter++) {
        final IndexSearcher searcher = newSearcher(context.indexReader);
        String term = context.contentStrings[random.nextInt(context.contentStrings.length)];
        Sort groupSort = new Sort(new SortField("id", SortField.Type.STRING));
        int topN = 1 + random.nextInt(10);

        List<DistinctValuesCollector.GroupCount<Comparable<?>>> expectedResult = createExpectedResult(context, term, groupSort, topN);

        FirstPassGroupingCollector<Comparable<?>> firstCollector = createRandomFirstPassCollector(groupSort, groupField, topN);
        searcher.search(new TermQuery(new Term("content", term)), firstCollector);
        DistinctValuesCollector<Comparable<?>> distinctValuesCollector
            = createDistinctCountCollector(firstCollector, groupField, countField);
        searcher.search(new TermQuery(new Term("content", term)), distinctValuesCollector);
        @SuppressWarnings("unchecked")
        List<DistinctValuesCollector.GroupCount<Comparable<?>>> actualResult = distinctValuesCollector.getGroups();

        if (VERBOSE) {
          System.out.println("Index iter=" + indexIter);
          System.out.println("Search iter=" + searchIter);
          System.out.println("1st pass collector class name=" + firstCollector.getClass().getName());
          System.out.println("2nd pass collector class name=" + distinctValuesCollector.getClass().getName());
          System.out.println("Search term=" + term);
          System.out.println("1st pass groups=" + firstCollector.getTopGroups(0, false));
          System.out.println("Expected:");      
          printGroups(expectedResult);
          System.out.println("Actual:");      
          printGroups(actualResult);
        }

        assertEquals(expectedResult.size(), actualResult.size());
        for (int i = 0; i < expectedResult.size(); i++) {
          DistinctValuesCollector.GroupCount<Comparable<?>> expected = expectedResult.get(i);
          DistinctValuesCollector.GroupCount<Comparable<?>> actual = actualResult.get(i);
          assertValues(expected.groupValue, actual.groupValue);
          assertEquals(expected.uniqueValues.size(), actual.uniqueValues.size());
          List<Comparable<?>> expectedUniqueValues = new ArrayList<>(expected.uniqueValues);
          Collections.sort(expectedUniqueValues, nullComparator);
          List<Comparable<?>> actualUniqueValues = new ArrayList<>(actual.uniqueValues);
          Collections.sort(actualUniqueValues, nullComparator);
          for (int j = 0; j < expectedUniqueValues.size(); j++) {
            assertValues(expectedUniqueValues.get(j), actualUniqueValues.get(j));
          }
        }
      }
      context.indexReader.close();
      context.directory.close();
    }
  }

  private void printGroups(List<? extends DistinctValuesCollector.GroupCount<Comparable<?>>> results) {
    for(int i=0;i<results.size();i++) {
      DistinctValuesCollector.GroupCount<Comparable<?>> group = results.get(i);
      Object gv = group.groupValue;
      if (gv instanceof BytesRef) {
        System.out.println(i + ": groupValue=" + ((BytesRef) gv).utf8ToString());
      } else {
        System.out.println(i + ": groupValue=" + gv);
      }
      for(Object o : group.uniqueValues) {
        if (o instanceof BytesRef) {
          System.out.println("  " + ((BytesRef) o).utf8ToString());
        } else {
          System.out.println("  " + o);
        }
      }
    }
  }

  private void assertValues(Object expected, Object actual) {
    if (expected == null) {
      compareNull(actual);
    } else {
      compare(((BytesRef) expected).utf8ToString(), actual);
    }
  }
  
  private void compare(String expected, Object groupValue) {
    if (BytesRef.class.isAssignableFrom(groupValue.getClass())) {
      assertEquals(expected, ((BytesRef) groupValue).utf8ToString());
    } else if (Double.class.isAssignableFrom(groupValue.getClass())) {
      assertEquals(Double.parseDouble(expected), groupValue);
    } else if (Long.class.isAssignableFrom(groupValue.getClass())) {
      assertEquals(Long.parseLong(expected), groupValue);
    } else if (MutableValue.class.isAssignableFrom(groupValue.getClass())) {
      MutableValueStr mutableValue = new MutableValueStr();
      mutableValue.value.copyChars(expected);
      assertEquals(mutableValue, groupValue);
    } else {
      fail();
    }
  }

  private void compareNull(Object groupValue) {
    if (groupValue == null) {
      return; // term based impl...
    }
    // DV based impls..
    if (BytesRef.class.isAssignableFrom(groupValue.getClass())) {
      assertEquals("", ((BytesRef) groupValue).utf8ToString());
    } else if (Double.class.isAssignableFrom(groupValue.getClass())) {
      assertEquals(0.0d, groupValue);
    } else if (Long.class.isAssignableFrom(groupValue.getClass())) {
      assertEquals(0L, groupValue);
      // Function based impl
    } else if (MutableValue.class.isAssignableFrom(groupValue.getClass())) {
      assertFalse(((MutableValue) groupValue).exists());
    } else {
      fail();
    }
  }

  private void addField(Document doc, String field, String value) {
    doc.add(new SortedDocValuesField(field, new BytesRef(value)));
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  private <T extends Comparable> DistinctValuesCollector<T> createDistinctCountCollector(FirstPassGroupingCollector<T> firstPassGroupingCollector,
                                                                                                                             String groupField,
                                                                                                                             String countField) throws IOException {
    Random random = random();
    Collection<SearchGroup<T>> searchGroups = firstPassGroupingCollector.getTopGroups(0, false);
    if (FunctionFirstPassGroupingCollector.class.isAssignableFrom(firstPassGroupingCollector.getClass())) {
      return (DistinctValuesCollector) new FunctionDistinctValuesCollector(new HashMap<>(), new BytesRefFieldSource(groupField), new BytesRefFieldSource(countField), (Collection) searchGroups);
    } else {
      return (DistinctValuesCollector) new TermDistinctValuesCollector(groupField, countField, (Collection) searchGroups);
    }
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  private <T> FirstPassGroupingCollector<T> createRandomFirstPassCollector(Sort groupSort, String groupField, int topNGroups) throws IOException {
    Random random = random();
    if (random.nextBoolean()) {
      return (FirstPassGroupingCollector<T>) new FunctionFirstPassGroupingCollector(new BytesRefFieldSource(groupField), new HashMap<>(), groupSort, topNGroups);
    } else {
      return (FirstPassGroupingCollector<T>) new TermFirstPassGroupingCollector(groupField, groupSort, topNGroups);
    }
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  private List<DistinctValuesCollector.GroupCount<Comparable<?>>> createExpectedResult(IndexContext context, String term, Sort groupSort, int topN) {
    class GroupCount extends DistinctValuesCollector.GroupCount<BytesRef> {
      GroupCount(BytesRef groupValue, Collection<BytesRef> uniqueValues) {
        super(groupValue);
        this.uniqueValues.addAll(uniqueValues);
      }
    }

    List result = new ArrayList();
    Map<String, Set<String>> groupCounts = context.searchTermToGroupCounts.get(term);
    int i = 0;
    for (String group : groupCounts.keySet()) {
      if (topN <= i++) {
        break;
      }
      Set<BytesRef> uniqueValues = new HashSet<>();
      for (String val : groupCounts.get(group)) {
        uniqueValues.add(val != null ? new BytesRef(val) : null);
      }
      result.add(new GroupCount(group != null ? new BytesRef(group) : null, uniqueValues));
    }
    return result;
  }

  private IndexContext createIndexContext() throws Exception {
    Random random = random();

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(
        random,
        dir,
        newIndexWriterConfig(new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy())
      );

    int numDocs = 86 + random.nextInt(1087) * RANDOM_MULTIPLIER;
    String[] groupValues = new String[numDocs / 5];
    String[] countValues = new String[numDocs / 10];
    for (int i = 0; i < groupValues.length; i++) {
      groupValues[i] = generateRandomNonEmptyString();
    }
    for (int i = 0; i < countValues.length; i++) {
      countValues[i] = generateRandomNonEmptyString();
    }
    
    List<String> contentStrings = new ArrayList<>();
    Map<String, Map<String, Set<String>>> searchTermToGroupCounts = new HashMap<>();
    for (int i = 1; i <= numDocs; i++) {
      String groupValue = random.nextInt(23) == 14 ? null : groupValues[random.nextInt(groupValues.length)];
      String countValue = random.nextInt(21) == 13 ? null : countValues[random.nextInt(countValues.length)];
      String content = "random" + random.nextInt(numDocs / 20);
      Map<String, Set<String>> groupToCounts = searchTermToGroupCounts.get(content);
      if (groupToCounts == null) {
        // Groups sort always DOCID asc...
        searchTermToGroupCounts.put(content, groupToCounts = new LinkedHashMap<>());
        contentStrings.add(content);
      }

      Set<String> countsVals = groupToCounts.get(groupValue);
      if (countsVals == null) {
        groupToCounts.put(groupValue, countsVals = new HashSet<>());
      }
      countsVals.add(countValue);

      Document doc = new Document();
      doc.add(new StringField("id", String.format(Locale.ROOT, "%09d", i), Field.Store.YES));
      doc.add(new SortedDocValuesField("id", new BytesRef(String.format(Locale.ROOT, "%09d", i))));
      if (groupValue != null) {
        addField(doc, groupField, groupValue);
      }
      if (countValue != null) {
        addField(doc, countField, countValue);
      }
      doc.add(new TextField("content", content, Field.Store.YES));
      w.addDocument(doc);
    }

    DirectoryReader reader = w.getReader();
    if (VERBOSE) {
      for(int docID=0;docID<reader.maxDoc();docID++) {
        Document doc = reader.document(docID);
        System.out.println("docID=" + docID + " id=" + doc.get("id") + " content=" + doc.get("content") + " author=" + doc.get("author") + " publisher=" + doc.get("publisher"));
      }
    }

    w.close();
    return new IndexContext(dir, reader, searchTermToGroupCounts, contentStrings.toArray(new String[contentStrings.size()]));
  }

  private static class IndexContext {

    final Directory directory;
    final DirectoryReader indexReader;
    final Map<String, Map<String, Set<String>>> searchTermToGroupCounts;
    final String[] contentStrings;

    IndexContext(Directory directory, DirectoryReader indexReader, 
                 Map<String, Map<String, Set<String>>> searchTermToGroupCounts, String[] contentStrings) {
      this.directory = directory;
      this.indexReader = indexReader;
      this.searchTermToGroupCounts = searchTermToGroupCounts;
      this.contentStrings = contentStrings;
    }
  }

  private static class NullComparator implements Comparator<Comparable<?>> {

    @Override
    @SuppressWarnings({"unchecked","rawtypes"})
    public int compare(Comparable a, Comparable b) {
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

  }

}
