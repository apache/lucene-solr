package org.apache.lucene.index;

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

import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.Ignore;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

@TimeoutSuite(millis = 80 * TimeUnits.HOUR)
@Ignore("very slow")
@SuppressCodecs("Lucene3x")
public class Test2BSortedDocValues extends LuceneTestCase {
  
  // indexes Integer.MAX_VALUE docs with a fixed binary field
  public void testFixedSorted() throws Exception {
    BaseDirectoryWrapper dir = newFSDirectory(_TestUtil.getTempDir("2BFixedSorted"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    
    IndexWriter w = new IndexWriter(dir,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setRAMBufferSizeMB(256.0)
        .setMergeScheduler(new ConcurrentMergeScheduler())
        .setMergePolicy(newLogMergePolicy(false, 10))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE));

    Document doc = new Document();
    byte bytes[] = new byte[2];
    BytesRef data = new BytesRef(bytes);
    SortedDocValuesField dvField = new SortedDocValuesField("dv", data);
    doc.add(dvField);
    
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      bytes[0] = (byte)(i >> 8);
      bytes[1] = (byte) i;
      w.addDocument(doc);
      if (i % 100000 == 0) {
        System.out.println("indexed: " + i);
        System.out.flush();
      }
    }
    
    w.forceMerge(1);
    w.close();
    
    System.out.println("verifying...");
    System.out.flush();
    
    DirectoryReader r = DirectoryReader.open(dir);
    int expectedValue = 0;
    for (AtomicReaderContext context : r.leaves()) {
      AtomicReader reader = context.reader();
      BytesRef scratch = new BytesRef();
      BinaryDocValues dv = reader.getSortedDocValues("dv");
      for (int i = 0; i < reader.maxDoc(); i++) {
        bytes[0] = (byte)(expectedValue >> 8);
        bytes[1] = (byte) expectedValue;
        dv.get(i, scratch);
        assertEquals(data, scratch);
        expectedValue++;
      }
    }
    
    r.close();
    dir.close();
  }
  
  // indexes Integer.MAX_VALUE docs with a fixed binary field
  // TODO: must use random.nextBytes (like Test2BTerms) to avoid BytesRefHash probing issues
  public void test2BOrds() throws Exception {
    BaseDirectoryWrapper dir = newFSDirectory(_TestUtil.getTempDir("2BOrds"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    
    IndexWriter w = new IndexWriter(dir,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setRAMBufferSizeMB(256.0)
        .setMergeScheduler(new ConcurrentMergeScheduler())
        .setMergePolicy(newLogMergePolicy(false, 10))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE));

    Document doc = new Document();
    byte bytes[] = new byte[4];
    BytesRef data = new BytesRef(bytes);
    SortedDocValuesField dvField = new SortedDocValuesField("dv", data);
    doc.add(dvField);
    
    long seed = random().nextLong();
    Random random = new Random(seed);
    
    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      random.nextBytes(bytes);
      w.addDocument(doc);
      if (i % 100000 == 0) {
        System.out.println("indexed: " + i);
        System.out.flush();
      }
    }
    
    w.forceMerge(1);
    w.close();
    
    System.out.println("verifying...");
    System.out.flush();
    
    DirectoryReader r = DirectoryReader.open(dir);
    random.setSeed(seed);
    for (AtomicReaderContext context : r.leaves()) {
      AtomicReader reader = context.reader();
      BytesRef scratch = new BytesRef();
      BinaryDocValues dv = reader.getSortedDocValues("dv");
      for (int i = 0; i < reader.maxDoc(); i++) {
        random.nextBytes(bytes);
        dv.get(i, scratch);
        assertEquals(data, scratch);
      }
    }
    
    r.close();
    dir.close();
  }
  
  // TODO: variable
}
