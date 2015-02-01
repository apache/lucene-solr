package org.apache.lucene.document;

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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

// We need DV random-access ords support for multi-valued sorting:
@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
public class TestBigDecimalFields extends LuceneTestCase {

  private BigDecimal make(String token, int scale) {
    return new BigDecimal(new BigInteger(token), scale);
  }

  public void testRange1() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", 30, 2);
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document doc = w.newDocument();
    doc.addBigDecimal("num", make("3000000000000000000", 2));
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addBigDecimal("num", make("2000000000000000000", 2));
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addBigDecimal("num", make("7000000000000000000", 2));
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // Make sure range query hits the right number of hits
    assertEquals(2, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigDecimalRangeFilter("num",
                                                                 make("0", 2), true,
                                                                 make("3000000000000000000", 2), true), 1).totalHits);
    assertEquals(3, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigDecimalRangeFilter("num",
                                                                 make("0", 2), true,
                                                                 make("10000000000000000000", 2), true), 1).totalHits);
    TopDocs hits = s.search(new MatchAllDocsQuery(),
                            fieldTypes.newBigDecimalRangeFilter("num",
                                                                make("1000000000000000000", 2), true,
                                                                make("2500000000000000000", 2), true), 1);
    assertEquals(1, hits.totalHits);
    assertEquals(make("2000000000000000000", 2), s.doc(hits.scoreDocs[0].doc).getBigDecimal("num"));

    doc = w.newDocument();
    doc.addBigDecimal("num", make("17", 2));
    doc.addAtom("id", "four");
    w.addDocument(doc);
    w.forceMerge(1);
    r.close();
    r = DirectoryReader.open(w, true);
    s = newSearcher(r);

    assertEquals(4, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigDecimalRangeFilter("num",
                                                                 make("0", 2), true,
                                                                 make("10000000000000000000", 2), true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testRange2() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", 3, 1);
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document doc = w.newDocument();
    doc.addBigDecimal("num", make("-100", 1));
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addBigDecimal("num", make("200", 1));
    doc.addAtom("id", "two");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // Make sure range query hits the right number of hits
    assertEquals(1, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigDecimalRangeFilter("num",
                                                                 make("-200", 1), true,
                                                                 make("17", 1), true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  private BigDecimal randomBigDecimal(int scale) {
    BigInteger big = new BigInteger(TestUtil.nextInt(random(), 4, 100), random()); 
    if (random().nextBoolean()) {
      // nocommit why only positive?
      big = big.negate();
    }
    return new BigDecimal(big, scale);
  }

  public void testRandomRangeAndSort() throws Exception {
    Directory dir = newDirectory();
    int numDocs = atLeast(100);
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", TestUtil.nextInt(random(), 20, 100), 4);
    List<BigDecimal> values = new ArrayList<>();
    Map<BigDecimal,Integer> valueCounts = new HashMap<>();
    for(int i=0;i<numDocs;i++) {
      Document doc = w.newDocument();
      doc.addUniqueInt("id", i);
      BigDecimal big = randomBigDecimal(4);
      values.add(big);
      Integer cur = valueCounts.get(big);
      if (cur == null) {
        cur = 0;
      }
      cur++;
      valueCounts.put(big, cur);
      doc.addBigDecimal("num", big);
      w.addDocument(doc);
      if (VERBOSE) {
        System.out.println("TEST: id=" + i + " big=" + big);
      }
    }

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {
      BigDecimal x = randomBigDecimal(4);
      BigDecimal y = randomBigDecimal(4);

      BigDecimal min, max;
      if (x.compareTo(y) < 0) {
        min = x;
        max = y;
      } else {
        min = y;
        max = x;
      }
      Set<Integer> expected = new HashSet<>();
      for(int i=0;i<values.size();i++) {
        BigDecimal value = values.get(i);
        if (value.compareTo(min) >= 0 && value.compareTo(max) <= 0) {
          expected.add(i);
        }
      }
      if (VERBOSE) {
        System.out.println("TEST: iter " + iter + " count=" + expected.size() + " min=" + min + " max=" + max);
        for(int value : expected) {
          System.out.println("  " + value);
        }
      }
      
      Set<Integer> actual = new HashSet<>();
      Filter filter = fieldTypes.newBigDecimalRangeFilter("num", min, true, max, true);

      boolean reversed = random().nextBoolean();
      Sort sort = fieldTypes.newSort("num", reversed);
      if (VERBOSE) {
        System.out.println("TEST: filter=" + filter + " reversed=" + reversed + " sort=" + sort);
      }
      TopDocs hits = s.search(new MatchAllDocsQuery(), filter, numDocs, sort);
      BigDecimal last = null;
      boolean wrongValues = false;
      for(ScoreDoc hit : hits.scoreDocs) {
        Document doc = s.doc(hit.doc);
        actual.add(doc.getInt("id"));
        BigDecimal v = doc.getBigDecimal("num");
        wrongValues |= v.equals(((FieldDoc) hit).fields[0]) == false;
        if (last != null) {
          int cmp = last.compareTo(v);
          assertTrue((reversed && cmp >= 0) || (reversed == false && cmp <= 0));
        }
        last = v;
      }
      assertEquals(expected, actual);
      assertFalse(wrongValues);
    }

    for (BigDecimal value : values) {
      assertEquals(valueCounts.get(value).intValue(), s.search(fieldTypes.newBigDecimalTermQuery("num", value), 1).totalHits);
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", 20, 10);
    fieldTypes.setSortMissingFirst("num");
    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addBigDecimal("num", make("45", 10));
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addBigDecimal("num", make("-2", 10));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("num"));
    assertEquals(1, s.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals(2, s.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals(0, s.doc(hits.scoreDocs[2].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", 20, -4);
    fieldTypes.setSortMissingLast("num");
    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addBigDecimal("num", make("45", -4));
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addBigDecimal("num", make("-2", -4));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("num"));
    assertEquals(2, s.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals(0, s.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals(1, s.doc(hits.scoreDocs[2].doc).get("id"));
    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedSort() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("num");
    fieldTypes.setBigDecimalByteWidthAndScale("num", 20, 0);

    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addBigDecimal("num", make("45", 0));
    doc.addBigDecimal("num", make("-22", 0));
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addBigDecimal("num", make("-2", 0));
    doc.addBigDecimal("num", make("14", 0));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("num"));

    // Default selector is MIN:
    assertEquals(0, s.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals(2, s.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals(1, s.doc(hits.scoreDocs[2].doc).get("id"));

    fieldTypes.setMultiValuedStringSortSelector("num", SortedSetSelector.Type.MAX);
    hits = s.search(new MatchAllDocsQuery(), 10, fieldTypes.newSort("num"));
    assertEquals(2, s.doc(hits.scoreDocs[0].doc).get("id"));
    assertEquals(0, s.doc(hits.scoreDocs[1].doc).get("id"));
    assertEquals(1, s.doc(hits.scoreDocs[2].doc).get("id"));

    r.close();
    w.close();
    dir.close();
  }

  public void testMultiValuedRange() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setMultiValued("num");
    fieldTypes.setBigDecimalByteWidthAndScale("num", 20, 4);

    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addBigDecimal("num", make("45", 4));
    doc.addBigDecimal("num", make("-22", 4));
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addBigDecimal("num", make("-2", 4));
    doc.addBigDecimal("num", make("14", 4));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newBigDecimalRangeFilter("num", make("-100", 4), true, make("100", 4), true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newBigDecimalRangeFilter("num", make("40", 4), true, make("45", 4), true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testJustStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", 20, 4);
    Document doc = w.newDocument();
    doc.addStoredBigDecimal("num", make("100", 4));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    doc = s.doc(0);
    assertEquals(make("100", 4), doc.getBigDecimal("num"));
    r.close();
    w.close();
  }

  public void testExcStoredNoScale() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    shouldFail(() -> doc.addStoredBigDecimal("num", make("100", 4)),
               "field \"num\": cannot addStored: you must first record the byte width and scale for this BIG_DECIMAL field");
    w.close();
  }

  public void testExcIndexedThenStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", 20, 4);
    doc.addBigDecimal("num", make("100", 4));
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addStoredBigDecimal("num", make("200", 4)),
               "field \"num\": cannot addStored: field was already added non-stored");
    w.close();
  }

  public void testExcStoredThenIndexed() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", 20, 7);
    Document doc = w.newDocument();
    doc.addStoredBigDecimal("num", make("100", 7));
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addBigDecimal("num", make("200", 7)),
               "field \"num\": this field is only stored; use addStoredXXX instead");
    w.close();
  }

  public void testTooLarge() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", 10, 4);
    doc.addBigDecimal("num", make("1000000000000000000000000", 4));
    shouldFail(() -> w.addDocument(doc),
               "field \"num\": BigInteger 1000000000000000000000000 exceeds allowed byte width 10");
    w.close();
  }

  public void testWrongScale() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", 10, 4);
    doc.addBigDecimal("num", make("1000000000", 2));
    shouldFail(() -> w.addDocument(doc),
               "field \"num\": BIG_DECIMAL was configured with scale=4, but indexed value has scale=2");
    w.close();
  }

  public void testWrongScaleJustStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigDecimalByteWidthAndScale("num", 20, 4);
    Document doc = w.newDocument();
    doc.addStoredBigDecimal("num", make("100", 5));
    shouldFail(() -> w.addDocument(doc),
               "field \"num\": BIG_DECIMAL was configured with scale=4, but stored value has scale=5");
    w.close();
  }
}
