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
package org.apache.lucene.index;


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase.Monster;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

@SuppressCodecs({"SimpleText", "Direct"})
@TimeoutSuite(millis = 80 * TimeUnits.HOUR) // effectively no limit
// The six hour time was achieved on a Linux 3.13 system with these specs:
// 3-core AMD at 2.5Ghz, 12 GB RAM, 5GB test heap, 2 test JVMs, 2TB SATA.
@Monster("takes ~ 6 hours if the heap is 5gb")
@SuppressSysoutChecks(bugUrl = "Stuff gets printed.")
public class Test2BBinaryDocValues extends LuceneTestCase {
  
  // indexes IndexWriter.MAX_DOCS docs with a fixed binary field
  public void testFixedBinary() throws Exception {
    BaseDirectoryWrapper dir = newFSDirectory(createTempDir("2BFixedBinary"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    
    IndexWriter w = new IndexWriter(dir,
        new IndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setRAMBufferSizeMB(256.0)
        .setMergeScheduler(new ConcurrentMergeScheduler())
        .setMergePolicy(newLogMergePolicy(false, 10))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        .setCodec(TestUtil.getDefaultCodec()));

    Document doc = new Document();
    byte bytes[] = new byte[4];
    BytesRef data = new BytesRef(bytes);
    BinaryDocValuesField dvField = new BinaryDocValuesField("dv", data);
    doc.add(dvField);
    
    for (int i = 0; i < IndexWriter.MAX_DOCS; i++) {
      bytes[0] = (byte)(i >> 24);
      bytes[1] = (byte)(i >> 16);
      bytes[2] = (byte)(i >> 8);
      bytes[3] = (byte) i;
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
    for (LeafReaderContext context : r.leaves()) {
      LeafReader reader = context.reader();
      BinaryDocValues dv = reader.getBinaryDocValues("dv");
      for (int i = 0; i < reader.maxDoc(); i++) {
        bytes[0] = (byte)(expectedValue >> 24);
        bytes[1] = (byte)(expectedValue >> 16);
        bytes[2] = (byte)(expectedValue >> 8);
        bytes[3] = (byte) expectedValue;
        assertEquals(i, dv.nextDoc());
        final BytesRef term = dv.binaryValue();
        assertEquals(data, term);
        expectedValue++;
      }
    }
    
    r.close();
    dir.close();
  }
  
  // indexes IndexWriter.MAX_DOCS docs with a variable binary field
  public void testVariableBinary() throws Exception {
    BaseDirectoryWrapper dir = newFSDirectory(createTempDir("2BVariableBinary"));
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(MockDirectoryWrapper.Throttling.NEVER);
    }
    
    IndexWriter w = new IndexWriter(dir,
        new IndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setRAMBufferSizeMB(256.0)
        .setMergeScheduler(new ConcurrentMergeScheduler())
        .setMergePolicy(newLogMergePolicy(false, 10))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        .setCodec(TestUtil.getDefaultCodec()));

    Document doc = new Document();
    byte bytes[] = new byte[4];
    ByteArrayDataOutput encoder = new ByteArrayDataOutput(bytes);
    BytesRef data = new BytesRef(bytes);
    BinaryDocValuesField dvField = new BinaryDocValuesField("dv", data);
    doc.add(dvField);
    
    for (int i = 0; i < IndexWriter.MAX_DOCS; i++) {
      encoder.reset(bytes);
      encoder.writeVInt(i % 65535); // 1, 2, or 3 bytes
      data.length = encoder.getPosition();
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
    ByteArrayDataInput input = new ByteArrayDataInput();
    for (LeafReaderContext context : r.leaves()) {
      LeafReader reader = context.reader();
      BinaryDocValues dv = reader.getBinaryDocValues("dv");
      for (int i = 0; i < reader.maxDoc(); i++) {
        assertEquals(i, dv.nextDoc());
        final BytesRef term = dv.binaryValue();
        input.reset(term.bytes, term.offset, term.length);
        assertEquals(expectedValue % 65535, input.readVInt());
        assertTrue(input.eof());
        expectedValue++;
      }
    }
    
    r.close();
    dir.close();
  }
}
