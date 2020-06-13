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


import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.util.TestUtil;

/**
 * Test that the default codec detects bit flips at open or checkIntegrity time.
 */
@SuppressFileSystems("ExtrasFS")
@AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-9356")
public class TestAllFilesDetectBitFlips extends LuceneTestCase {

  public void test() throws Exception {
    doTest(false);
  }

  public void testCFS() throws Exception {
    doTest(true);
  }

  public void doTest(boolean cfs) throws Exception {
    Directory dir = newDirectory();

    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
    conf.setCodec(TestUtil.getDefaultCodec());

    if (cfs == false) {
      conf.setUseCompoundFile(false);
      conf.getMergePolicy().setNoCFSRatio(0.0);
    }

    RandomIndexWriter riw = new RandomIndexWriter(random(), dir, conf);
    // Use LineFileDocs so we (hopefully) get most Lucene features
    // tested, e.g. IntPoint was recently added to it:
    LineFileDocs docs = new LineFileDocs(random());
    for (int i = 0; i < 100; i++) {
      riw.addDocument(docs.nextDoc());
      if (random().nextInt(7) == 0) {
        riw.commit();
      }
      if (random().nextInt(20) == 0) {
        riw.deleteDocuments(new Term("docid", Integer.toString(i)));
      }
      if (random().nextInt(15) == 0) {
        riw.updateNumericDocValue(new Term("docid", Integer.toString(i)), "docid_intDV", Long.valueOf(i));
      }
    }
    if (TEST_NIGHTLY == false) {
      riw.forceMerge(1);
    }
    riw.close();
    checkBitFlips(dir);
    dir.close();
  }
  
  private void checkBitFlips(Directory dir) throws IOException {
    for(String name : dir.listAll()) {
      if (name.equals(IndexWriter.WRITE_LOCK_NAME) == false) {
        corruptFile(dir, name);
      }
    }
  }
  
  private void corruptFile(Directory dir, String victim) throws IOException {
    try (BaseDirectoryWrapper dirCopy = newDirectory()) {
      dirCopy.setCheckIndexOnClose(false);

      long victimLength = dir.fileLength(victim);
      long flipOffset = TestUtil.nextLong(random(), 0, victimLength - 1);

      if (VERBOSE) {
        System.out.println("TEST: now corrupt file " + victim + " by changing byte at offset " + flipOffset + " (length= " + victimLength + ")");
      }

      for(String name : dir.listAll()) {
        if (name.equals(victim) == false) {
          dirCopy.copyFrom(dir, name, name, IOContext.DEFAULT);
        } else {
          try (IndexOutput out = dirCopy.createOutput(name, IOContext.DEFAULT);
              IndexInput in = dir.openInput(name, IOContext.DEFAULT)) {
              out.copyBytes(in, flipOffset);
              out.writeByte((byte) (in.readByte() + TestUtil.nextInt(random(), 0x01, 0xFF)));
              out.copyBytes(in, victimLength - flipOffset - 1);
          }
          try (IndexInput in = dirCopy.openInput(name, IOContext.DEFAULT)) {
            try {
              CodecUtil.checksumEntireFile(in);
              System.out.println("TEST: changing a byte in " + victim + " did not update the checksum)");
              return;
            } catch (CorruptIndexException e) {
              // ok
            }
          }
        }
        dirCopy.sync(Collections.singleton(name));
      }

      // corruption must be detected
      expectThrowsAnyOf(Arrays.asList(CorruptIndexException.class, IndexFormatTooOldException.class, IndexFormatTooNewException.class),
          () -> {
            try (IndexReader reader = DirectoryReader.open(dirCopy)) {
              for (LeafReaderContext context : reader.leaves()) {
                context.reader().checkIntegrity();
              }
            }
          }
      );
    }
  }
}
