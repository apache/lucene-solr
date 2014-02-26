package org.apache.lucene.util;

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

import java.io.IOException;
import java.util.BitSet;

public class TestPForDeltaDocIdSet extends BaseDocIdSetTestCase<PForDeltaDocIdSet> {

  @Override
  public PForDeltaDocIdSet copyOf(BitSet bs, int length) throws IOException {
    final PForDeltaDocIdSet.Builder builder = new PForDeltaDocIdSet.Builder().setIndexInterval(TestUtil.nextInt(random(), 1, 20));
    for (int doc = bs.nextSetBit(0); doc != -1; doc = bs.nextSetBit(doc + 1)) {
      builder.add(doc);
    }
    return builder.build();
  }

  @Override
  public void assertEquals(int numBits, BitSet ds1, PForDeltaDocIdSet ds2)
      throws IOException {
    super.assertEquals(numBits, ds1, ds2);
    assertEquals(ds1.cardinality(), ds2.cardinality());
  }

}
