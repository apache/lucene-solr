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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;


public class TestOptimizeForever extends LuceneTestCase {

  // Just counts how many merges are done for optimize
  private static class MyIndexWriter extends IndexWriter {

    AtomicInteger optimizeMergeCount = new AtomicInteger();
    private boolean first;

    public MyIndexWriter(Directory dir, IndexWriterConfig conf) throws Exception {
      super(dir, conf);
    }

    @Override
    public void merge(MergePolicy.OneMerge merge) throws CorruptIndexException, IOException {
      if (merge.optimize && (first || merge.segments.size() == 1)) {
        first = false;
        if (VERBOSE) {
          System.out.println("TEST: optimized merge");
        }
        optimizeMergeCount.incrementAndGet();
      }
      super.merge(merge);
    }
  }

  public void test() throws Exception {
    final Directory d = newDirectory();
    final MyIndexWriter w = new MyIndexWriter(d, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    w.setInfoStream(VERBOSE ? System.out : null);

    // Try to make an index that requires optimizing:
    w.getConfig().setMaxBufferedDocs(_TestUtil.nextInt(random, 2, 11));
    final int numStartDocs = atLeast(20);
    final LineFileDocs docs = new LineFileDocs(random);
    for(int docIDX=0;docIDX<numStartDocs;docIDX++) {
      w.addDocument(docs.nextDoc());
    }
    MergePolicy mp = w.getConfig().getMergePolicy();
    final int mergeAtOnce = 1+w.segmentInfos.size();
    if (mp instanceof TieredMergePolicy) {
      ((TieredMergePolicy) mp).setMaxMergeAtOnce(mergeAtOnce);
    } else if (mp instanceof LogMergePolicy) {
      ((LogMergePolicy) mp).setMergeFactor(mergeAtOnce);
    } else {
      // skip test
      w.close();
      d.close();
      return;
    }

    final AtomicBoolean doStop = new AtomicBoolean();
    w.getConfig().setMaxBufferedDocs(2);
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          while (!doStop.get()) {
            w.updateDocument(new Term("docid", "" + random.nextInt(numStartDocs)),
                             docs.nextDoc());
            // Force deletes to apply
            w.getReader().close();
          }
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
      };
    t.start();
    w.optimize();
    doStop.set(true);
    t.join();
    assertTrue("optimize count is " + w.optimizeMergeCount.get(), w.optimizeMergeCount.get() <= 1);
    w.close();
    d.close();
  }
}
