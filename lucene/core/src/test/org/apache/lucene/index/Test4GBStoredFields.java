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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TimeUnits;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.carrotsearch.randomizedtesting.generators.RandomInts;

/**
 * This test creates an index with one segment that is a little larger than 4GB.
 */
@SuppressCodecs({ "SimpleText" })
@TimeoutSuite(millis = 4 * TimeUnits.HOUR)
public class Test4GBStoredFields extends LuceneTestCase {

  @Nightly
  public void test() throws Exception {
    MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), new MMapDirectory(_TestUtil.getTempDir("4GBStoredFields")));
    dir.setThrottling(MockDirectoryWrapper.Throttling.NEVER);

    IndexWriter w = new IndexWriter(dir,
        new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setRAMBufferSizeMB(256.0)
        .setMergeScheduler(new ConcurrentMergeScheduler())
        .setMergePolicy(newLogMergePolicy(false, 10))
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE));

    MergePolicy mp = w.getConfig().getMergePolicy();
    if (mp instanceof LogByteSizeMergePolicy) {
     // 1 petabyte:
     ((LogByteSizeMergePolicy) mp).setMaxMergeMB(1024*1024*1024);
    }

    final Document doc = new Document();
    final FieldType ft = new FieldType();
    ft.setIndexed(false);
    ft.setStored(true);
    ft.freeze();
    final int valueLength = RandomInts.randomIntBetween(random(), 1 << 13, 1 << 20);
    final byte[] value = new byte[valueLength];
    for (int i = 0; i < valueLength; ++i) {
      // random so that even compressing codecs can't compress it
      value[i] = (byte) random().nextInt(256);
    }
    final Field f = new Field("fld", value, ft);
    doc.add(f);

    final int numDocs = (int) ((1L << 32) / valueLength + 100);
    for (int i = 0; i < numDocs; ++i) {
      w.addDocument(doc);
      if (VERBOSE && i % (numDocs / 10) == 0) {
        System.out.println(i + " of " + numDocs + "...");
      }
    }
    w.forceMerge(1);
    w.close();
    if (VERBOSE) {
      boolean found = false;
      for (String file : dir.listAll()) {
        if (file.endsWith(".fdt")) {
          final long fileLength = dir.fileLength(file);
          if (fileLength >= 1L << 32) {
            found = true;
          }
          System.out.println("File length of " + file + " : " + fileLength);
        }
      }
      if (!found) {
        System.out.println("No .fdt file larger than 4GB, test bug?");
      }
    }

    DirectoryReader rd = DirectoryReader.open(dir);
    Document sd = rd.document(numDocs - 1);
    assertNotNull(sd);
    assertEquals(1, sd.getFields().size());
    BytesRef valueRef = sd.getBinaryValue("fld");
    assertNotNull(valueRef);
    assertEquals(new BytesRef(value), valueRef);
    rd.close();

    dir.close();
  }

}
