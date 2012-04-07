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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.RollingBuffer;
import org.apache.lucene.util._TestUtil;

// TODO: sometimes remove tokens too...?

/** Randomly inserts overlapped (posInc=0) tokens with
 *  posLength sometimes > 1.  The chain must have
 *  an OffsetAttribute.  */

public final class MockGraphTokenFilter extends TokenFilter {

  private static boolean DEBUG = false;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  private final long seed;
  private Random random;

  // Don't init to -1 (caller must first call reset):
  private int inputPos;
  private int outputPos;
  // Don't init to -1 (caller must first call reset):
  private int lastOutputPos;
  private boolean end;

  private final class Position implements RollingBuffer.Resettable {
    final List<AttributeSource.State> states = new ArrayList<AttributeSource.State>();
    int nextRead;

    // Any token leaving from this position should have this startOffset:
    int startOffset = -1;

    // Any token arriving to this positoin should have this endOffset:
    int endOffset = -1;

    @Override
    public void reset() {
      states.clear();
      nextRead = 0;
      startOffset = -1;
      endOffset = -1;
    }

    public void captureState() throws IOException {
      assert startOffset == offsetAtt.startOffset();
      states.add(MockGraphTokenFilter.this.captureState());
    }
  }

  private final RollingBuffer<Position> positions = new RollingBuffer<Position>() {
    @Override
    protected Position newInstance() {
      return new Position();
    }
  };

  public MockGraphTokenFilter(Random random, TokenStream input) {
    super(input);
    seed = random.nextLong();
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    end = false;
    positions.reset();
    // NOTE: must be "deterministically random" because
    // BaseTokenStreamTestCase pulls tokens twice on the
    // same input and asserts they are the same:
    this.random = new Random(seed);
    inputPos = -1;
    outputPos = 0;
    lastOutputPos = -1;
  }

  private enum TOKEN_POS {SAME, NEXT, END};

  private TOKEN_POS nextInputToken() throws IOException {
    assert !end;
    if (DEBUG) {
      System.out.println("  call input.incr");
    }
    final boolean result = input.incrementToken();
    if (result) {
      final int posInc = posIncAtt.getPositionIncrement();
      final int posLength = posLengthAtt.getPositionLength();

      // NOTE: when posLength > 1, we have a hole... we
      // don't allow injected tokens to start or end
      // "inside" a hole, so we don't need to make up
      // offsets inside it

      assert inputPos != -1 || posInc > 0;
      inputPos += posInc;
      if (DEBUG) {
        System.out.println("    got token term=" + termAtt + " posLength=" + posLength + " posInc=" + posInc + " inputPos=" + inputPos);
      }
      final Position posData = positions.get(inputPos);
      if (posInc == 0) {
        assert posData.startOffset == offsetAtt.startOffset();
      } else {
        assert posData.startOffset == -1;
        posData.startOffset = offsetAtt.startOffset();
        if (DEBUG) {
          System.out.println("    record startOffset[" + inputPos + "]=" + posData.startOffset);
        }
      }

      final Position posEndData = positions.get(inputPos + posLength);
      if (posEndData.endOffset == -1) {
        // First time we are seeing a token that
        // arrives to this position: record the
        // endOffset
        posEndData.endOffset = offsetAtt.endOffset();
        if (DEBUG) {
          System.out.println("    record endOffset[" + (inputPos+posLength) + "]=" + posEndData.endOffset);
        }
      } else {
        // We've already seen a token arriving there;
        // make sure its endOffset is the same (NOTE:
        // some tokenizers, eg WDF, will fail
        // this...):
        assert posEndData.endOffset == offsetAtt.endOffset(): "posEndData.endOffset=" + posEndData.endOffset + " vs offsetAtt.endOffset()=" + offsetAtt.endOffset();
      }
      if (posInc == 0) {
        return TOKEN_POS.SAME;
      } else {
        return TOKEN_POS.NEXT;
      }
    } else {
      if (DEBUG) {
        System.out.println("    got END");
      }
      return TOKEN_POS.END;
    }
  }

  private void pushOutputPos() {
    posIncAtt.setPositionIncrement(outputPos - lastOutputPos);
    if (DEBUG) {
      System.out.println("  pushOutputPos: set posInc=" + posIncAtt.getPositionIncrement());
    }
    lastOutputPos = outputPos;
    positions.freeBefore(outputPos);
  }

  @Override
  public boolean incrementToken() throws IOException {

    if (DEBUG) {
      System.out.println("MockGraphTF.incr inputPos=" + inputPos + " outputPos=" + outputPos);
    }

    while (true) {
      final Position posData = positions.get(outputPos);
      if (posData.nextRead < posData.states.size()) {
        // Serve up all buffered tokens from this position:
        if (DEBUG) {
          System.out.println("  restore buffered nextRead=" + posData.nextRead + " vs " + posData.states.size());
        }
        restoreState(posData.states.get(posData.nextRead++));
        if (DEBUG) {
          System.out.println("    term=" + termAtt + " outputPos=" + outputPos);
        }
        pushOutputPos();
        return true;
      }

      boolean tokenPending = false;

      final int prevInputPos = inputPos;

      if (inputPos == -1 || inputPos == outputPos) {
        // We've used up the buffered tokens; pull the next
        // input token:
        if (end) {
          return false;
        }
        final TOKEN_POS result = nextInputToken();
        if (result == TOKEN_POS.SAME) {
          return true;
        } else if (result == TOKEN_POS.NEXT) {
          tokenPending = true;
        } else {
          // NOTE: we don't set end=true here... because we
          // are immediately passing through "the end" to
          // caller (return false), and caller must not call
          // us again:
          return false;
        }
      } else {
        assert inputPos > outputPos;
        if (DEBUG) {
          System.out.println("  done @ outputPos=" + outputPos);
        }
      }

      // We're done (above) serving up all tokens leaving
      // from the same position; now maybe insert a token.
      // Note that we may insert more than one token leaving
      // from this position.  We only inject tokens at
      // positions where we've seen at least one input token
      // (ie, we cannot inject inside holes):

      if (prevInputPos != -1  && positions.get(outputPos).startOffset != -1 && random.nextInt(7) == 5) {
        if (DEBUG) {
          System.out.println("  inject @ outputPos=" + outputPos);
        }

        if (tokenPending) {
          positions.get(inputPos).captureState();
        }
        final int posLength = _TestUtil.nextInt(random, 1, 5);
        final Position posEndData = positions.get(outputPos + posLength);

        // Pull enough tokens until we discover what our
        // endOffset should be:
        while (!end && posEndData.endOffset == -1 && inputPos <= (outputPos + posLength)) {
          if (DEBUG) {
            System.out.println("  lookahead [endPos=" + (outputPos + posLength) + "]...");
          }
          final TOKEN_POS result = nextInputToken();
          if (result != TOKEN_POS.END) {
            positions.get(inputPos).captureState();
          } else {
            end = true;
            if (DEBUG) {
              System.out.println("    force end lookahead");
            }
            break;
          }
        }

        // TODO: really, here, on hitting end-of-tokens,
        // we'd like to know the ending "posInc", and allow
        // our token to extend up until that.  But: a
        // TokenFilter is not allowed to call end() from
        // within its incrementToken, so we can't do that.
        // It may have been better if the ending
        // posInc/offsets were set when incrementToken
        // returned false (ie, without having to call the
        // special end method):

        if (posEndData.endOffset != -1) {
          assert posEndData.endOffset != -1;
          clearAttributes();
          posLengthAtt.setPositionLength(posLength);
          termAtt.append(_TestUtil.randomUnicodeString(random));
          pushOutputPos();
          offsetAtt.setOffset(positions.get(outputPos).startOffset,
                              positions.get(outputPos + posLength).endOffset);
          if (DEBUG) {
            System.out.println("  inject: outputPos=" + outputPos + " startOffset=" + offsetAtt.startOffset() +
                               " endOffset=" + offsetAtt.endOffset() +
                               " posLength=" + posLengthAtt.getPositionLength());
          }
          // TODO: set TypeAtt too?
          return true;

        } else {
          // Either, we hit the end of the tokens (ie, our
          // attempted posLength is too long because it
          // hangs out over the end), or, our attempted
          // posLength ended in the middle of a hole; just
          // skip injecting in these cases.  We will still
          // test these cases by having a StopFilter after
          // MockGraphTokenFilter...
        }

      } else if (tokenPending) {
        outputPos = inputPos;
        if (DEBUG) {
          System.out.println("  pass-through");
        }
        pushOutputPos();
        return true;
      } else {
        // We are skipping over a hole (posInc > 1) from our input:
        outputPos++;
        if (DEBUG) {
          System.out.println("  incr outputPos=" + outputPos);
        }
      }
    }
  }
}
