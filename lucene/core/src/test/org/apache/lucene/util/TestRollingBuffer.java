package org.apache.lucene.util;

import java.util.Random;

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

public class TestRollingBuffer extends LuceneTestCase {

  private static class Position implements RollingBuffer.Resettable {
    public int pos;

    @Override
    public void reset() {
      pos = -1;
    }
  }

  public void test() {
    
    final RollingBuffer<Position> buffer = new RollingBuffer<Position>() {
      @Override
      protected Position newInstance() {
        final Position pos = new Position();
        pos.pos = -1;
        return pos;
      }
    };

    for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {

      int freeBeforePos = 0;
      final int maxPos = atLeast(10000);
      final FixedBitSet posSet = new FixedBitSet(maxPos + 1000);
      int posUpto = 0;
      Random random = random();
      while (freeBeforePos < maxPos) {
        if (random.nextInt(4) == 1) {
          final int limit = rarely() ? 1000 : 20;
          final int inc = random.nextInt(limit);
          final int pos = freeBeforePos + inc;
          posUpto = Math.max(posUpto, pos);
          if (VERBOSE) {
            System.out.println("  check pos=" + pos + " posUpto=" + posUpto);
          }
          final Position posData = buffer.get(pos);
          if (!posSet.getAndSet(pos)) {
            assertEquals(-1, posData.pos);
            posData.pos = pos;
          } else {
            assertEquals(pos, posData.pos);
          }
        } else {
          if (posUpto > freeBeforePos) {
            freeBeforePos += random.nextInt(posUpto - freeBeforePos);
          }
          if (VERBOSE) {
            System.out.println("  freeBeforePos=" + freeBeforePos);
          }
          buffer.freeBefore(freeBeforePos);
        }          
      }

      buffer.reset();
    }
  }
}
