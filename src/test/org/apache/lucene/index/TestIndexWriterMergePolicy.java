package org.apache.lucene.index;

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

import java.io.IOException;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util._TestUtil;

import org.apache.lucene.util.LuceneTestCase;

public class TestIndexWriterMergePolicy extends LuceneTestCase {

  // Test the normal case
  public void testNormalCase() throws IOException {
    Directory dir = new RAMDirectory();

    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(10);
    writer.setMergeFactor(10);
    writer.setMergePolicy(new LogDocMergePolicy());

    for (int i = 0; i < 100; i++) {
      addDoc(writer);
      checkInvariants(writer);
    }

    writer.close();
  }

  // Test to see if there is over merge
  public void testNoOverMerge() throws IOException {
    Directory dir = new RAMDirectory();

    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(10);
    writer.setMergeFactor(10);
    writer.setMergePolicy(new LogDocMergePolicy());

    boolean noOverMerge = false;
    for (int i = 0; i < 100; i++) {
      addDoc(writer);
      checkInvariants(writer);
      if (writer.getNumBufferedDocuments() + writer.getSegmentCount() >= 18) {
        noOverMerge = true;
      }
    }
    assertTrue(noOverMerge);

    writer.close();
  }

  // Test the case where flush is forced after every addDoc
  public void testForceFlush() throws IOException {
    Directory dir = new RAMDirectory();

    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(10);
    writer.setMergeFactor(10);
    LogDocMergePolicy mp = new LogDocMergePolicy();
    mp.setMinMergeDocs(100);
    writer.setMergePolicy(mp);

    for (int i = 0; i < 100; i++) {
      addDoc(writer);
      writer.close();

      writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.LIMITED);
      writer.setMaxBufferedDocs(10);
      writer.setMergePolicy(mp);
      mp.setMinMergeDocs(100);
      writer.setMergeFactor(10);
      checkInvariants(writer);
    }

    writer.close();
  }

  // Test the case where mergeFactor changes
  public void testMergeFactorChange() throws IOException {
    Directory dir = new RAMDirectory();

    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setMaxBufferedDocs(10);
    writer.setMergeFactor(100);
    writer.setMergePolicy(new LogDocMergePolicy());

    for (int i = 0; i < 250; i++) {
      addDoc(writer);
      checkInvariants(writer);
    }

    writer.setMergeFactor(5);

    // merge policy only fixes segments on levels where merges
    // have been triggered, so check invariants after all adds
    for (int i = 0; i < 10; i++) {
      addDoc(writer);
    }
    checkInvariants(writer);

    writer.close();
  }

  // Test the case where both mergeFactor and maxBufferedDocs change
  public void testMaxBufferedDocsChange() throws IOException {
    Directory dir = new RAMDirectory();

    IndexWriter writer = new IndexWriter(dir, true, new WhitespaceAnalyzer(), true);
    writer.setMaxBufferedDocs(101);
    writer.setMergeFactor(101);
    writer.setMergePolicy(new LogDocMergePolicy());

    // leftmost* segment has 1 doc
    // rightmost* segment has 100 docs
    for (int i = 1; i <= 100; i++) {
      for (int j = 0; j < i; j++) {
        addDoc(writer);
        checkInvariants(writer);
      }
      writer.close();

      writer = new IndexWriter(dir, true, new WhitespaceAnalyzer(), false);
      writer.setMaxBufferedDocs(101);
      writer.setMergeFactor(101);
      writer.setMergePolicy(new LogDocMergePolicy());
    }

    writer.setMaxBufferedDocs(10);
    writer.setMergeFactor(10);

    // merge policy only fixes segments on levels where merges
    // have been triggered, so check invariants after all adds
    for (int i = 0; i < 100; i++) {
      addDoc(writer);
    }
    checkInvariants(writer);

    for (int i = 100; i < 1000; i++) {
      addDoc(writer);
    }
    checkInvariants(writer);

    writer.close();
  }

  // Test the case where a merge results in no doc at all
  public void testMergeDocCount0() throws IOException {
    Directory dir = new RAMDirectory();

    IndexWriter writer = new IndexWriter(dir, true, new WhitespaceAnalyzer(), true);
    writer.setMergePolicy(new LogDocMergePolicy());
    writer.setMaxBufferedDocs(10);
    writer.setMergeFactor(100);

    for (int i = 0; i < 250; i++) {
      addDoc(writer);
      checkInvariants(writer);
    }
    writer.close();

    IndexReader reader = IndexReader.open(dir);
    reader.deleteDocuments(new Term("content", "aaa"));
    reader.close();

    writer = new IndexWriter(dir, true, new WhitespaceAnalyzer(), false);
    writer.setMergePolicy(new LogDocMergePolicy());
    writer.setMaxBufferedDocs(10);
    writer.setMergeFactor(5);

    // merge factor is changed, so check invariants after all adds
    for (int i = 0; i < 10; i++) {
      addDoc(writer);
    }
    checkInvariants(writer);
    assertEquals(10, writer.docCount());

    writer.close();
  }

  private void addDoc(IndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    writer.addDocument(doc);
  }

  private void checkInvariants(IndexWriter writer) throws IOException {
    _TestUtil.syncConcurrentMerges(writer);
    int maxBufferedDocs = writer.getMaxBufferedDocs();
    int mergeFactor = writer.getMergeFactor();
    int maxMergeDocs = writer.getMaxMergeDocs();

    int ramSegmentCount = writer.getNumBufferedDocuments();
    assertTrue(ramSegmentCount < maxBufferedDocs);

    int lowerBound = -1;
    int upperBound = maxBufferedDocs;
    int numSegments = 0;

    int segmentCount = writer.getSegmentCount();
    for (int i = segmentCount - 1; i >= 0; i--) {
      int docCount = writer.getDocCount(i);
      assertTrue(docCount > lowerBound);

      if (docCount <= upperBound) {
        numSegments++;
      } else {
        if (upperBound * mergeFactor <= maxMergeDocs) {
          assertTrue(numSegments < mergeFactor);
        }

        do {
          lowerBound = upperBound;
          upperBound *= mergeFactor;
        } while (docCount > upperBound);
        numSegments = 1;
      }
    }
    if (upperBound * mergeFactor <= maxMergeDocs) {
      assertTrue(numSegments < mergeFactor);
    }

    String[] files = writer.getDirectory().list();
    int segmentCfsCount = 0;
    for (int i = 0; i < files.length; i++) {
      if (files[i].endsWith(".cfs")) {
        segmentCfsCount++;
      }
    }
    assertEquals(segmentCount, segmentCfsCount);
  }

  private void printSegmentDocCounts(IndexWriter writer) {
    int segmentCount = writer.getSegmentCount();
    System.out.println("" + segmentCount + " segments total");
    for (int i = 0; i < segmentCount; i++) {
      System.out.println("  segment " + i + " has " + writer.getDocCount(i)
          + " docs");
    }
  }
}
