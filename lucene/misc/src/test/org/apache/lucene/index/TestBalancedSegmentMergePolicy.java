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
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BalancedSegmentMergePolicy.MergePolicyParams;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Simple tests for {@link BalancedSegmentMergePolicy}
 * doesn't actually test that this thing does what it says it should,
 * just merges with it!
 */
public class TestBalancedSegmentMergePolicy extends LuceneTestCase {
  Directory dir;
  RandomIndexWriter iw;
  
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    BalancedSegmentMergePolicy mp = new BalancedSegmentMergePolicy();
    mp.setMergePolicyParams(newMergePolicyParams(random()));
    iwc.setMergePolicy(mp);
    iw = new RandomIndexWriter(random(), dir, iwc);
    LineFileDocs docs = new LineFileDocs(random(), true);
    int numDocs = atLeast(200);
    for (int i = 0; i < numDocs; i++) {
      iw.addDocument(docs.nextDoc());
      if (random().nextInt(6) == 0) {
        iw.deleteDocuments(new Term("docid", Integer.toString(i)));
      }
      if (random().nextInt(10) == 0) {
        iw.commit();
      }
    }
  }
  
  public void tearDown() throws Exception {
    iw.close();
    dir.close();
    super.tearDown();
  }
  
  public void testForceMerge() throws Exception {
    int numSegments = _TestUtil.nextInt(random(), 1, 4);
    iw.forceMerge(numSegments);
    DirectoryReader ir = iw.getReader();
    assertTrue(ir.getSequentialSubReaders().size() <= numSegments);
    ir.close();
  }
  
  private static MergePolicyParams newMergePolicyParams(Random random) {
    MergePolicyParams params = new MergePolicyParams();
    if (rarely(random)) {
      params.setMergeFactor(_TestUtil.nextInt(random, 2, 9));
    } else {
      params.setMergeFactor(_TestUtil.nextInt(random, 10, 50));
    }
    int mergeFactor = params.getMergeFactor();
    params.setMaxSmallSegments(_TestUtil.nextInt(random, mergeFactor, mergeFactor*2));
    params.setNumLargeSegments(_TestUtil.nextInt(random, 2, 3));
    params.setUseCompoundFile(random.nextBoolean());
    return params;
  }
}
