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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class TestDocIDMerger extends LuceneTestCase {

  private static class TestSubUnsorted extends DocIDMerger.Sub {
    private int docID = -1;
    final int valueStart;
    final int maxDoc;

    public TestSubUnsorted(MergeState.DocMap docMap, int maxDoc, int valueStart) {
      super(docMap);
      this.maxDoc = maxDoc;
      this.valueStart = valueStart;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }

    public int getValue() {
      return valueStart + docID;
    }
  }

  public void testNoSort() throws Exception {

    int subCount = TestUtil.nextInt(random(), 1, 20);
    List<TestSubUnsorted> subs = new ArrayList<>();
    int valueStart = 0;
    for(int i=0;i<subCount;i++) {
      int maxDoc = TestUtil.nextInt(random(), 1, 1000);
      final int docBase = valueStart;
      subs.add(new TestSubUnsorted(new MergeState.DocMap() {
          @Override
          public int get(int docID) {
            return docBase + docID;
          }
        }, maxDoc, valueStart));
      valueStart += maxDoc;
    }

    DocIDMerger<TestSubUnsorted> merger = DocIDMerger.of(subs, false);

    int count = 0;
    while (true) {
      TestSubUnsorted sub = merger.next();
      if (sub == null) {
        break;
      }
      assertEquals(count, sub.mappedDocID);
      assertEquals(count, sub.getValue());
      count++;
    }

    assertEquals(valueStart, count);
  }

  private static class TestSubSorted extends DocIDMerger.Sub {
    private int docID = -1;
    final int maxDoc;
    final int index;

    public TestSubSorted(MergeState.DocMap docMap, int maxDoc, int index) {
      super(docMap);
      this.maxDoc = maxDoc;
      this.index = index;
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }

    @Override
    public String toString() {
      return "TestSubSorted(index=" + index + ", mappedDocID=" + mappedDocID+ ")";
    }
  }

  public void testWithSort() throws Exception {

    int subCount = TestUtil.nextInt(random(), 1, 20);
    List<int[]> oldToNew = new ArrayList<>();
    // how many docs we've written to each sub:
    List<Integer> uptos = new ArrayList<>();
    int totDocCount = 0;
    for(int i=0;i<subCount;i++) {
      int maxDoc = TestUtil.nextInt(random(), 1, 1000);
      uptos.add(0);
      oldToNew.add(new int[maxDoc]);
      totDocCount += maxDoc;
    }

    List<int[]> completedSubs = new ArrayList<>();

    // randomly distribute target docIDs into the segments:
    for(int docID=0;docID<totDocCount;docID++) {
      int sub = random().nextInt(oldToNew.size());
      int upto = uptos.get(sub);
      int[] subDocs = oldToNew.get(sub);
      subDocs[upto] = docID;
      upto++;
      if (upto == subDocs.length) {
        completedSubs.add(subDocs);
        oldToNew.remove(sub);
        uptos.remove(sub);
      } else {
        uptos.set(sub, upto);
      }
    }
    assertEquals(0, oldToNew.size());

    // sometimes do some deletions:
    final FixedBitSet liveDocs;
    if (random().nextBoolean()) {
      liveDocs = new FixedBitSet(totDocCount);
      liveDocs.set(0, totDocCount);
      int deleteAttemptCount = TestUtil.nextInt(random(), 1, totDocCount);
      for(int i=0;i<deleteAttemptCount;i++) {
        liveDocs.clear(random().nextInt(totDocCount));
      }
    } else {
      liveDocs = null;
    }

    List<TestSubSorted> subs = new ArrayList<>();
    for(int i=0;i<subCount;i++) {
      final int[] docMap = completedSubs.get(i);
      subs.add(new TestSubSorted(new MergeState.DocMap() {
          @Override
          public int get(int docID) {
            int mapped = docMap[docID];
            if (liveDocs == null || liveDocs.get(mapped)) {
              return mapped;
            } else {
              return -1;
            }
          }
        }, docMap.length, i));
    }

    DocIDMerger<TestSubSorted> merger = DocIDMerger.of(subs, true);

    int count = 0;
    while (true) {
      TestSubSorted sub = merger.next();
      if (sub == null) {
        break;
      }
      if (liveDocs != null) {
        count = liveDocs.nextSetBit(count);
      }
      assertEquals(count, sub.mappedDocID);
      count++;
    }

    if (liveDocs != null) {
      if (count < totDocCount) {
        assertEquals(NO_MORE_DOCS, liveDocs.nextSetBit(count));
      } else {
        assertEquals(totDocCount, count);
      }
    } else {
      assertEquals(totDocCount, count);
    }
  }
}
