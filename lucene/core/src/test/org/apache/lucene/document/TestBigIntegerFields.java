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
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBigIntegerFields extends LuceneTestCase {
  public void testRange1() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigIntByteWidth("num", 30);
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document doc = w.newDocument();
    doc.addBigInteger("num", new BigInteger("3000000000000000000"));
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addBigInteger("num", new BigInteger("2000000000000000000"));
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addBigInteger("num", new BigInteger("7000000000000000000"));
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // Make sure range query hits the right number of hits
    assertEquals(2, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigIntRangeFilter("num",
                                                             new BigInteger("0"), true,
                                                             new BigInteger("3000000000000000000"), true), 1).totalHits);
    assertEquals(3, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigIntRangeFilter("num",
                                                             new BigInteger("0"), true,
                                                             new BigInteger("10000000000000000000"), true), 1).totalHits);
    TopDocs hits = s.search(new MatchAllDocsQuery(),
                            fieldTypes.newBigIntRangeFilter("num",
                                                            new BigInteger("1000000000000000000"), true,
                                                            new BigInteger("2500000000000000000"), true), 1);
    assertEquals(1, hits.totalHits);
    assertEquals(new BigInteger("2000000000000000000"), s.doc(hits.scoreDocs[0].doc).getBigInteger("num"));

    doc = w.newDocument();
    doc.addBigInteger("num", new BigInteger("17"));
    doc.addAtom("id", "four");
    w.addDocument(doc);
    w.forceMerge(1);
    r.close();
    r = DirectoryReader.open(w, true);
    s = newSearcher(r);

    assertEquals(4, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigIntRangeFilter("num",
                                                             new BigInteger("0"), true,
                                                             new BigInteger("10000000000000000000"), true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testBigIntRange2() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigIntByteWidth("num", 3);
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    Document doc = w.newDocument();
    doc.addBigInteger("num", new BigInteger("-100"));
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addBigInteger("num", new BigInteger("200"));
    doc.addAtom("id", "two");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // Make sure range query hits the right number of hits
    assertEquals(1, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigIntRangeFilter("num",
                                                             new BigInteger("-200"), true,
                                                             new BigInteger("17"), true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  private BigInteger randomBigInt() {
    BigInteger big = new BigInteger(TestUtil.nextInt(random(), 4, 100), random()); 
    if (random().nextBoolean()) {
      big = big.negate();
    }
    return big;
  }

  public void testRandomRangeAndSort() throws Exception {
    Directory dir = newDirectory();
    int numDocs = atLeast(100);
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigIntByteWidth("num", TestUtil.nextInt(random(), 20, 100));
    List<BigInteger> values = new ArrayList<>();
    Map<BigInteger,Integer> valueCounts = new HashMap<>();
    for(int i=0;i<numDocs;i++) {
      Document doc = w.newDocument();
      doc.addUniqueInt("id", i);
      BigInteger big = randomBigInt();
      values.add(big);
      Integer cur = valueCounts.get(big);
      if (cur == null) {
        cur = 0;
      }
      cur++;
      valueCounts.put(big, cur);
      doc.addBigInteger("num", big);
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
      BigInteger x = randomBigInt();
      BigInteger y = randomBigInt();

      BigInteger min, max;
      if (x.compareTo(y) < 0) {
        min = x;
        max = y;
      } else {
        min = y;
        max = x;
      }
      Set<Integer> expected = new HashSet<>();
      for(int i=0;i<values.size();i++) {
        BigInteger value = values.get(i);
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
      Filter filter = fieldTypes.newBigIntRangeFilter("num", min, true, max, true);

      boolean reversed = random().nextBoolean();
      Sort sort = fieldTypes.newSort("num", reversed);
      if (VERBOSE) {
        System.out.println("TEST: filter=" + filter + " reversed=" + reversed + " sort=" + sort);
      }
      TopDocs hits = s.search(new MatchAllDocsQuery(), filter, numDocs, sort);
      BigInteger last = null;
      boolean wrongValues = false;
      for(ScoreDoc hit : hits.scoreDocs) {
        Document doc = s.doc(hit.doc);
        actual.add(doc.getInt("id"));
        BigInteger v = doc.getBigInteger("num");
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

    for (BigInteger value : values) {
      assertEquals(valueCounts.get(value).intValue(), s.search(fieldTypes.newBigIntTermQuery("num", value), 1).totalHits);
    }

    r.close();
    w.close();
    dir.close();
  }

  public void testSortMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = newRandomIndexWriter(dir);
    FieldTypes fieldTypes = w.getFieldTypes();
    fieldTypes.setBigIntByteWidth("num", 20);
    fieldTypes.setSortMissingFirst("num");
    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addBigInteger("num", BigInteger.valueOf(45));
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addBigInteger("num", BigInteger.valueOf(-2));
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
    fieldTypes.setBigIntByteWidth("num", 20);
    fieldTypes.setSortMissingLast("num");
    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addBigInteger("num", BigInteger.valueOf(45));
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addBigInteger("num", BigInteger.valueOf(-2));
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
    fieldTypes.setBigIntByteWidth("num", 20);

    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addBigInteger("num", BigInteger.valueOf(45));
    doc.addBigInteger("num", BigInteger.valueOf(-22));
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addBigInteger("num", BigInteger.valueOf(-2));
    doc.addBigInteger("num", BigInteger.valueOf(14));
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
    fieldTypes.setBigIntByteWidth("num", 20);

    Document doc = w.newDocument();
    doc.addUniqueInt("id", 0);
    doc.addBigInteger("num", BigInteger.valueOf(45));
    doc.addBigInteger("num", BigInteger.valueOf(-22));
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 1);
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addUniqueInt("id", 2);
    doc.addBigInteger("num", BigInteger.valueOf(-2));
    doc.addBigInteger("num", BigInteger.valueOf(14));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    fieldTypes = r.getFieldTypes();

    IndexSearcher s = newSearcher(r);
    assertEquals(2, s.search(new MatchAllDocsQuery(), fieldTypes.newBigIntRangeFilter("num", BigInteger.valueOf(-100), true, BigInteger.valueOf(100), true), 1).totalHits);
    assertEquals(1, s.search(new MatchAllDocsQuery(), fieldTypes.newBigIntRangeFilter("num", BigInteger.valueOf(40), true, BigInteger.valueOf(45), true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  public void testJustStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addStoredBigInteger("num", BigInteger.valueOf(100));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    doc = s.doc(0);
    assertEquals(BigInteger.valueOf(100), doc.getBigInteger("num"));
    r.close();
    w.close();
  }

  public void testExcIndexedThenStored() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addDouble("num", 100.);
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addStoredDouble("num", 200.),
               "field \"num\": cannot addStored: field was already added non-stored");
    w.close();
  }

  public void testExcStoredThenIndexed() throws Exception {
    IndexWriter w = newIndexWriter(dir);
    Document doc = w.newDocument();
    doc.addStoredBigInteger("num", BigInteger.valueOf(100));
    w.addDocument(doc);
    final Document doc2 = w.newDocument();
    shouldFail(() -> doc2.addBigInteger("num", BigInteger.valueOf(200)),
               "field \"num\": this field is only stored; use addStoredXXX instead");
    w.close();
  }
}
