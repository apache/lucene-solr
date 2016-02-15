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
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base test case for {@link MergePolicy}.
 */
public abstract class BaseMergePolicyTestCase extends LuceneTestCase {
  
  /** Create a new {@link MergePolicy} instance. */
  protected abstract MergePolicy mergePolicy();

  public void testForceMergeNotNeeded() throws IOException {
    try (Directory dir = newDirectory()) {
      final AtomicBoolean mayMerge = new AtomicBoolean(true);
      final MergeScheduler mergeScheduler = new SerialMergeScheduler() {
          @Override
          synchronized public void merge(IndexWriter writer, MergeTrigger trigger, boolean newMergesFound) throws IOException {
            if (mayMerge.get() == false) {
              MergePolicy.OneMerge merge = writer.getNextMerge();
              if (merge != null) {
                System.out.println("TEST: we should not need any merging, yet merge policy returned merge " + merge);
                throw new AssertionError();
              }
            }

            super.merge(writer, trigger, newMergesFound);
          }
        };

      MergePolicy mp = mergePolicy();
      assumeFalse("this test cannot tolerate random forceMerges", mp.toString().contains("MockRandomMergePolicy"));
      mp.setNoCFSRatio(random().nextBoolean() ? 0 : 1);

      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setMergeScheduler(mergeScheduler);
      iwc.setMergePolicy(mp);

      IndexWriter writer = new IndexWriter(dir, iwc);
      final int numSegments = TestUtil.nextInt(random(), 2, 20);
      for (int i = 0; i < numSegments; ++i) {
        final int numDocs = TestUtil.nextInt(random(), 1, 5);
        for (int j = 0; j < numDocs; ++j) {
          writer.addDocument(new Document());
        }
        writer.getReader().close();
      }
      for (int i = 5; i >= 0; --i) {
        final int segmentCount = writer.getSegmentCount();
        final int maxNumSegments = i == 0 ? 1 : TestUtil.nextInt(random(), 1, 10);
        mayMerge.set(segmentCount > maxNumSegments);
        if (VERBOSE) {
          System.out.println("TEST: now forceMerge(maxNumSegments=" + maxNumSegments + ") vs segmentCount=" + segmentCount);
        }
        writer.forceMerge(maxNumSegments);
      }
      writer.close();
    }
  }
  
}
