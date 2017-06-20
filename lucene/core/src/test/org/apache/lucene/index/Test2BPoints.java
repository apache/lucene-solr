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
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase.Monster;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.TimeUnits;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

// e.g. run like this: ant test -Dtestcase=Test2BPoints -Dtests.nightly=true -Dtests.verbose=true -Dtests.monster=true
// 
//   or: python -u /l/util/src/python/repeatLuceneTest.py -heap 6g -once -nolog -tmpDir /b/tmp -logDir /l/logs Test2BPoints.test2D -verbose

@SuppressCodecs({ "SimpleText", "Memory", "Direct", "Compressing" })
@TimeoutSuite(millis = 365 * 24 * TimeUnits.HOUR) // hopefully ~1 year is long enough ;)
@Monster("takes at least 4 hours and consumes many GB of temp disk space")
public class Test2BPoints extends LuceneTestCase {
  public void test1D() throws Exception {
    Directory dir = FSDirectory.open(createTempDir("2BPoints1D"));

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()))
      .setCodec(getCodec())
      .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
      .setRAMBufferSizeMB(256.0)
      .setMergeScheduler(new ConcurrentMergeScheduler())
      .setMergePolicy(newLogMergePolicy(false, 10))
      .setOpenMode(IndexWriterConfig.OpenMode.CREATE);

    ((ConcurrentMergeScheduler) iwc.getMergeScheduler()).setMaxMergesAndThreads(6, 3);
    
    IndexWriter w = new IndexWriter(dir, iwc);

    MergePolicy mp = w.getConfig().getMergePolicy();
    if (mp instanceof LogByteSizeMergePolicy) {
     // 1 petabyte:
     ((LogByteSizeMergePolicy) mp).setMaxMergeMB(1024*1024*1024);
    }

    final int numDocs = (Integer.MAX_VALUE / 26) + 1;
    int counter = 0;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (int j=0;j<26;j++) {
        long x = (((long) random().nextInt() << 32)) | (long) counter;
        doc.add(new LongPoint("long", x));
        counter++;
      }
      w.addDocument(doc);
      if (VERBOSE && i % 100000 == 0) {
        System.out.println(i + " of " + numDocs + "...");
      }
    }
    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(numDocs, s.count(LongPoint.newRangeQuery("long", Long.MIN_VALUE, Long.MAX_VALUE)));
    assertTrue(r.leaves().get(0).reader().getPointValues("long").size() > Integer.MAX_VALUE);
    r.close();
    w.close();
    System.out.println("TEST: now CheckIndex");
    TestUtil.checkIndex(dir);
    dir.close();
  }

  public void test2D() throws Exception {
    Directory dir = FSDirectory.open(createTempDir("2BPoints2D"));

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()))
      .setCodec(getCodec())
      .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
      .setRAMBufferSizeMB(256.0)
      .setMergeScheduler(new ConcurrentMergeScheduler())
      .setMergePolicy(newLogMergePolicy(false, 10))
      .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    
    ((ConcurrentMergeScheduler) iwc.getMergeScheduler()).setMaxMergesAndThreads(6, 3);

    IndexWriter w = new IndexWriter(dir, iwc);

    MergePolicy mp = w.getConfig().getMergePolicy();
    if (mp instanceof LogByteSizeMergePolicy) {
     // 1 petabyte:
     ((LogByteSizeMergePolicy) mp).setMaxMergeMB(1024*1024*1024);
    }

    final int numDocs = (Integer.MAX_VALUE / 26) + 1;
    int counter = 0;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (int j=0;j<26;j++) {
        long x = (((long) random().nextInt() << 32)) | (long) counter;
        long y = (((long) random().nextInt() << 32)) | (long) random().nextInt();
        doc.add(new LongPoint("long", x, y));
        counter++;
      }
      w.addDocument(doc);
      if (VERBOSE && i % 100000 == 0) {
        System.out.println(i + " of " + numDocs + "...");
      }
    }
    w.forceMerge(1);
    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(numDocs, s.count(LongPoint.newRangeQuery("long", new long[] {Long.MIN_VALUE, Long.MIN_VALUE}, new long[] {Long.MAX_VALUE, Long.MAX_VALUE})));
    assertTrue(r.leaves().get(0).reader().getPointValues("long").size() > Integer.MAX_VALUE);
    r.close();
    w.close();
    System.out.println("TEST: now CheckIndex");
    TestUtil.checkIndex(dir);
    dir.close();
  }

  private static Codec getCodec() {
    return Codec.forName("Lucene70");
  }
}
