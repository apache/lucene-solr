package org.apache.lucene.analysis;

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
import java.util.Random;

import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util._TestUtil;

// TODO: maybe, instead to be more "natural", we should make
// a MockRemovesTokensTF, ideally subclassing FilteringTF
// (in modules/analysis)

// Randomly injects holes:
public final class MockHoleInjectingTokenFilter extends TokenFilter {

  private final long randomSeed;
  private Random random;
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  private int maxPos;
  private int pos;

  public MockHoleInjectingTokenFilter(Random random, TokenStream in) {
    super(in);
    randomSeed = random.nextLong();
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    random = new Random(randomSeed);
    maxPos = -1;
    pos = -1;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      final int posInc = posIncAtt.getPositionIncrement();

      int nextPos = pos + posInc;

      // Carefully inject a hole only where it won't mess up
      // the graph:
      if (posInc > 0 && maxPos <= nextPos && random.nextInt(5) == 3) {
        final int holeSize = _TestUtil.nextInt(random, 1, 5);
        posIncAtt.setPositionIncrement(posInc + holeSize);
        nextPos += holeSize;
      }

      pos = nextPos;
      maxPos = Math.max(maxPos, pos + posLenAtt.getPositionLength());

      return true;
    } else {
      return false;
    }
  }

  // TODO: end?
}
