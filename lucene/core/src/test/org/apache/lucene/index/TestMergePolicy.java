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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

public class TestMergePolicy extends LuceneTestCase {

  public void testWaitForOneMerge() throws IOException, InterruptedException {
    try (Directory dir = newDirectory()) {
      MergePolicy.MergeSpecification ms = createRandomMergeSpecification(dir, 1 + random().nextInt(10));
      for (MergePolicy.OneMerge m : ms.merges) {
        assertFalse(m.hasCompletedSuccessfully().isPresent());
      }
      Thread t = new Thread(() -> {
        try {
          for (MergePolicy.OneMerge m : ms.merges) {
            m.close(true, false,  mr -> {});
          }
        } catch (IOException e) {
          throw new AssertionError(e);
        }
      });
      t.start();
      assertTrue(ms.await(100, TimeUnit.HOURS));
      for (MergePolicy.OneMerge m : ms.merges) {
        assertTrue(m.hasCompletedSuccessfully().get());
      }
      t.join();
    }
  }

  public void testTimeout() throws IOException, InterruptedException {
    try (Directory dir = newDirectory()) {
      MergePolicy.MergeSpecification ms = createRandomMergeSpecification(dir, 3);
      for (MergePolicy.OneMerge m : ms.merges) {
        assertFalse(m.hasCompletedSuccessfully().isPresent());
      }
      Thread t = new Thread(() -> {
        try {
          ms.merges.get(0).close(true, false,  mr -> {});
        } catch (IOException e) {
          throw new AssertionError(e);
        }
      });
      t.start();
      assertFalse(ms.await(10, TimeUnit.MILLISECONDS));
      assertFalse(ms.merges.get(1).hasCompletedSuccessfully().isPresent());
      t.join();
    }
  }

  public void testTimeoutLargeNumberOfMerges() throws IOException, InterruptedException {
    try (Directory dir = newDirectory()) {
      MergePolicy.MergeSpecification ms = createRandomMergeSpecification(dir, 10000);
      for (MergePolicy.OneMerge m : ms.merges) {
        assertFalse(m.hasCompletedSuccessfully().isPresent());
      }
      AtomicInteger i = new AtomicInteger(0);
      AtomicBoolean stop = new AtomicBoolean(false);
      Thread t = new Thread(() -> {
        while (stop.get() == false) {
          try {
            ms.merges.get(i.getAndIncrement()).close(true, false, mr -> {});
            Thread.sleep(1);
          } catch (IOException | InterruptedException e) {
            throw new AssertionError(e);
          }
        }
      });
      t.start();
      assertFalse(ms.await(10, TimeUnit.MILLISECONDS));
      stop.set(true);
      t.join();
      for (int j = 0; j < ms.merges.size(); j++) {
        if (j < i.get()) {
          assertTrue(ms.merges.get(j).hasCompletedSuccessfully().get());
        } else {
          assertFalse(ms.merges.get(j).hasCompletedSuccessfully().isPresent());
        }
      }
    }
  }

  public void testFinishTwice() throws IOException {
    try (Directory dir = newDirectory()) {
      MergePolicy.MergeSpecification spec = createRandomMergeSpecification(dir, 1);
      MergePolicy.OneMerge oneMerge = spec.merges.get(0);
      oneMerge.close(true, false, mr -> {});
      expectThrows(IllegalStateException.class, () -> oneMerge.close(false, false, mr -> {}));
    }
  }

  public void testTotalMaxDoc() throws IOException {
    try (Directory dir = newDirectory()) {
      MergePolicy.MergeSpecification spec = createRandomMergeSpecification(dir, 1);
      int docs = 0;
      MergePolicy.OneMerge oneMerge = spec.merges.get(0);
      for (SegmentCommitInfo info : oneMerge.segments) {
        docs += info.info.maxDoc();
      }
      assertEquals(docs, oneMerge.totalMaxDoc);
    }
  }

  private static MergePolicy.MergeSpecification createRandomMergeSpecification(Directory dir, int numMerges) {
    MergePolicy.MergeSpecification ms = new MergePolicy.MergeSpecification();
      for (int ii = 0; ii < numMerges; ++ii) {
        final SegmentInfo si = new SegmentInfo(
            dir, // dir
            Version.LATEST, // version
            Version.LATEST, // min version
            TestUtil.randomSimpleString(random()), // name
            random().nextInt(1000), // maxDoc
            random().nextBoolean(), // isCompoundFile
            null, // codec
            Collections.emptyMap(), // diagnostics
            TestUtil.randomSimpleString(// id
                random(),
                StringHelper.ID_LENGTH,
                StringHelper.ID_LENGTH).getBytes(StandardCharsets.US_ASCII),
            Collections.emptyMap(), // attributes
            null /* indexSort */);
        final List<SegmentCommitInfo> segments = new LinkedList<SegmentCommitInfo>();
        segments.add(new SegmentCommitInfo(si, 0, 0, 0, 0, 0, StringHelper.randomId()));
        ms.add(new MergePolicy.OneMerge(segments));
      }
      return ms;
  }
}
