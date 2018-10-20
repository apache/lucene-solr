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
package org.apache.lucene.analysis;

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.TestUtil;

// TODO: sometimes remove tokens too...?

/** Randomly inserts overlapped (posInc=0) tokens with
 *  posLength sometimes &gt; 1.  The chain must have
 *  an OffsetAttribute.  */

public final class MockGraphTokenFilter extends LookaheadTokenFilter<LookaheadTokenFilter.Position> {

  private static boolean DEBUG = false;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  private final long seed;
  private Random random;

  public MockGraphTokenFilter(Random random, TokenStream input) {
    super(input);
    seed = random.nextLong();
  }

  @Override
  protected Position newPosition() {
    return new Position();
  }

  @Override
  protected void afterPosition() throws IOException {
    if (DEBUG) {
      System.out.println("MockGraphTF.afterPos");
    }
    if (random.nextInt(7) == 5) {

      final int posLength = TestUtil.nextInt(random, 1, 5);

      if (DEBUG) {
        System.out.println("  do insert! posLen=" + posLength);
      }

      final Position posEndData = positions.get(outputPos + posLength);

      // Look ahead as needed until we figure out the right
      // endOffset:
      while(!end && posEndData.endOffset == -1 && inputPos <= (outputPos + posLength)) {
        if (!peekToken()) {
          break;
        }
      }

      if (posEndData.endOffset != -1) {
        // Notify super class that we are injecting a token:
        insertToken();
        clearAttributes();
        posLenAtt.setPositionLength(posLength);
        termAtt.append(TestUtil.randomUnicodeString(random));
        posIncAtt.setPositionIncrement(0);
        offsetAtt.setOffset(positions.get(outputPos).startOffset,
                            posEndData.endOffset);
        if (DEBUG) {
          System.out.println("  inject: outputPos=" + outputPos + " startOffset=" + offsetAtt.startOffset() +
                             " endOffset=" + offsetAtt.endOffset() +
                             " posLength=" + posLenAtt.getPositionLength());
        }
        // TODO: set TypeAtt too?
      } else {
        // Either 1) the tokens ended before our posLength,
        // or 2) our posLength ended inside a hole from the
        // input.  In each case we just skip the inserted
        // token.
      }
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    // NOTE: must be "deterministically random" because
    // BaseTokenStreamTestCase pulls tokens twice on the
    // same input and asserts they are the same:
    this.random = new Random(seed);
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.random = null;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (DEBUG) {
      System.out.println("MockGraphTF.incr inputPos=" + inputPos + " outputPos=" + outputPos);
    }
    if (random == null) {
      throw new IllegalStateException("incrementToken called in wrong state!");
    }
    return nextToken();
  }
}
