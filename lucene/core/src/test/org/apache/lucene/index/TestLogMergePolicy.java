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

import org.apache.lucene.index.MergePolicy.MergeSpecification;
import org.apache.lucene.index.MergePolicy.OneMerge;

public class TestLogMergePolicy extends BaseMergePolicyTestCase {

  public MergePolicy mergePolicy() {
    return newLogMergePolicy(random());
  }

  public void testDefaultForcedMergeMB() {
    LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
    assertTrue(mp.getMaxMergeMBForForcedMerge() > 0.0);
  }

  @Override
  protected void assertSegmentInfos(MergePolicy policy, SegmentInfos infos) throws IOException {
    // TODO
  }

  @Override
  protected void assertMerge(MergePolicy policy, MergeSpecification merge) throws IOException {
    LogMergePolicy lmp = (LogMergePolicy) policy;
    for (OneMerge oneMerge : merge.merges) {
      assertEquals(lmp.getMergeFactor(), oneMerge.segments.size());
    }
  }
}
