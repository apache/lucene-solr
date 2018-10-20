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
package org.apache.lucene.util;


import java.io.IOException;
import java.util.BitSet;

import org.apache.lucene.search.DocIdSet;

public class TestNotDocIdSet extends BaseDocIdSetTestCase<NotDocIdSet> {

  @Override
  public NotDocIdSet copyOf(BitSet bs, int length) throws IOException {
    final FixedBitSet set = new FixedBitSet(length);
    for (int doc = bs.nextClearBit(0); doc < length; doc = bs.nextClearBit(doc + 1)) {
      set.set(doc);
    }
    return new NotDocIdSet(length, new BitDocIdSet(set));
  }

  @Override
  public void assertEquals(int numBits, BitSet ds1, NotDocIdSet ds2)
      throws IOException {
    super.assertEquals(numBits, ds1, ds2);
    final Bits bits2 = ds2.bits();
    assertNotNull(bits2); // since we wrapped a FixedBitSet
    assertEquals(numBits, bits2.length());
    for (int i = 0; i < numBits; ++i) {
      assertEquals(ds1.get(i), bits2.get(i));
    }
  }

  public void testBits() throws IOException {
    assertNull(new NotDocIdSet(3, DocIdSet.EMPTY).bits());
    assertNotNull(new NotDocIdSet(3, new BitDocIdSet(new FixedBitSet(3))).bits());
  }

}
