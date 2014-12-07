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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBigInteger extends LuceneTestCase {
  // nocommit test multi-valued too
  public void testRange1() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    // nocommit need random test, across segments, with merging
    Document doc = w.newDocument();
    doc.addBigInteger("big", new BigInteger("3000000000000000000"));
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addBigInteger("big", new BigInteger("2000000000000000000"));
    doc.addAtom("id", "two");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addBigInteger("big", new BigInteger("7000000000000000000"));
    doc.addAtom("id", "three");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // Make sure range query hits the right number of hits
    assertEquals(2, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigIntRangeFilter("big",
                                                             new BigInteger("0"), true,
                                                             new BigInteger("3000000000000000000"), true), 1).totalHits);
    assertEquals(3, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigIntRangeFilter("big",
                                                             new BigInteger("0"), true,
                                                             new BigInteger("10000000000000000000"), true), 1).totalHits);
    TopDocs hits = s.search(new MatchAllDocsQuery(),
                            fieldTypes.newBigIntRangeFilter("big",
                                                            new BigInteger("1000000000000000000"), true,
                                                            new BigInteger("2500000000000000000"), true), 1);
    assertEquals(1, hits.totalHits);
    assertEquals(new BigInteger("2000000000000000000"), s.doc(hits.scoreDocs[0].doc).getBigInteger("big"));

    doc = w.newDocument();
    doc.addBigInteger("big", new BigInteger("17"));
    doc.addAtom("id", "four");
    w.addDocument(doc);
    w.forceMerge(1);
    r.close();
    r = DirectoryReader.open(w, true);
    s = newSearcher(r);

    assertEquals(4, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newBigIntRangeFilter("big",
                                                             new BigInteger("0"), true,
                                                             new BigInteger("10000000000000000000"), true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  /*
  public void testFoo() throws Exception {
    for(int i=-257;i<=257;i++) {
      byte[] bytes = BigInteger.valueOf(i).toByteArray();
      if (bytes.length == 2) {
        System.out.println(String.format(Locale.ROOT, "%d -> %x %x", i, bytes[0] & 0xff, bytes[1] & 0xff));
      } else {
        System.out.println(String.format(Locale.ROOT, "%d -> %x", i, bytes[0] & 0xff));
      }
    }
  }
  */

  /*
  public void testBigIntRange2() throws Exception {
    Directory dir = newDirectory();

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    FieldTypes fieldTypes = w.getFieldTypes();
    //System.out.println("id type: " + fieldTypes.getFieldType("id"));

    // nocommit need random test, across segments, with merging
    Document doc = w.newDocument();
    doc.addBigInteger("big", new BigInteger("-100"));
    doc.addAtom("id", "one");
    w.addDocument(doc);

    doc = w.newDocument();
    doc.addBigInteger("big", new BigInteger("200"));
    doc.addAtom("id", "two");
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);

    // nocommit add & test TermQuery

    // Make sure range query hits the right number of hits
    assertEquals(1, s.search(new MatchAllDocsQuery(),
                             fieldTypes.newRangeFilter("big",
                                                       new BigInteger("-200"), true,
                                                       new BigInteger("17"), true), 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }
  */

  // nocommit sorting

  private BigInteger randomBigInt() {
    BigInteger big = new BigInteger(TestUtil.nextInt(random(), 4, 100), random()); 
    // nocommit only positive for now
    if (false && random().nextBoolean()) {
      big = big.negate();
    }
    return big;
  }

  public void testRandom() throws Exception {
    Directory dir = newDirectory();
    int numDocs = atLeast(100);
    RandomIndexWriter w = newRandomIndexWriter(dir);
    List<BigInteger> values = new ArrayList<>();
    for(int i=0;i<numDocs;i++) {
      Document doc = w.newDocument();
      doc.addUniqueInt("id", i);
      BigInteger big = randomBigInt();
      values.add(big);
      doc.addBigInteger("big", big);
      w.addDocument(doc);
      if (VERBOSE) {
        System.out.println("id=" + i + " big=" + big);
      }
    }

    IndexReader r = w.getReader();
    FieldTypes fieldTypes = r.getFieldTypes();

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
        System.out.println("iter " + iter + " count=" + expected.size() + " min=" + min + " max=" + max);
        for(int value : expected) {
          System.out.println("  " + value);
        }
      }
      
      Set<Integer> actual = new HashSet<>();
      TopDocs hits = s.search(new MatchAllDocsQuery(), fieldTypes.newBigIntRangeFilter("big", min, true, max, true), numDocs);
      for(ScoreDoc hit : hits.scoreDocs) {
        actual.add(s.doc(hit.doc).getInt("id"));
      }
      assertEquals(expected, actual);
    }

    r.close();
    w.close();
    dir.close();
  }
}
