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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene60.Lucene60PointsReader;
import org.apache.lucene.codecs.lucene60.Lucene60PointsWriter;
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
    System.out.println("DIR: " + ((FSDirectory) dir).getDirectory());

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
    long counter = 0;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (int j=0;j<26;j++) {
        doc.add(new LongPoint("long", counter));
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
    assertEquals(1250, s.count(LongPoint.newRangeQuery("long", 33640828, 33673327)));
    assertTrue(r.leaves().get(0).reader().getPointValues().size("long") > Integer.MAX_VALUE);
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
    long counter = 0;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      for (int j=0;j<26;j++) {
        doc.add(new LongPoint("long", counter, 2*counter+1));
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
    assertEquals(1250, s.count(LongPoint.newRangeQuery("long", new long[] {33640828, 33673327}, new long[] {Long.MIN_VALUE, Long.MAX_VALUE})));
    assertTrue(r.leaves().get(0).reader().getPointValues().size("long") > Integer.MAX_VALUE);
    r.close();
    w.close();
    System.out.println("TEST: now CheckIndex");
    TestUtil.checkIndex(dir);
    dir.close();
  }

  private static Codec getCodec() {

    return new FilterCodec("Lucene60", Codec.forName("Lucene60")) {
      @Override
      public PointsFormat pointsFormat() {
        return new PointsFormat() {
          @Override
          public PointsWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
            int maxPointsInLeafNode = 1024;
            double maxMBSortInHeap = 256.0;
            return new Lucene60PointsWriter(writeState, maxPointsInLeafNode, maxMBSortInHeap);
          }

          @Override
          public PointsReader fieldsReader(SegmentReadState readState) throws IOException {
            return new Lucene60PointsReader(readState);
          }
        };
      }
    };
  }
}
