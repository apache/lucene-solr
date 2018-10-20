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

public class TestRoaringDocIdSet extends BaseDocIdSetTestCase<RoaringDocIdSet> {

  @Override
  public RoaringDocIdSet copyOf(BitSet bs, int length) throws IOException {
    final RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(length);
    for (int i = bs.nextSetBit(0); i != -1; i = bs.nextSetBit(i + 1)) {
      builder.add(i);
    }
    return builder.build();
  }

  @Override
  public void assertEquals(int numBits, BitSet ds1, RoaringDocIdSet ds2)
      throws IOException {
    super.assertEquals(numBits, ds1, ds2);
    assertEquals(ds1.cardinality(), ds2.cardinality());
  }

}
