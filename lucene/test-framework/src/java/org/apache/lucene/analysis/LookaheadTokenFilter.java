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

import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.RollingBuffer;

// TODO: cut SynFilter over to this
// TODO: somehow add "nuke this input token" capability...

/** An abstract TokenFilter to make it easier to build graph
 *  token filters requiring some lookahead.  This class handles
 *  the details of buffering up tokens, recording them by
 *  position, restoring them, providing access to them, etc. */

public abstract class LookaheadTokenFilter<T extends LookaheadTokenFilter.Position> extends TokenFilter {

  private final static boolean DEBUG = false;

  protected final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  protected final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  protected final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  // Position of last read input token:
  protected int inputPos;

  // Position of next possible output token to return:
  protected int outputPos;
  
  // True if we hit end from our input:
  protected boolean end;

  private boolean tokenPending;
  private boolean insertPending;

  /** Holds all state for a single position; subclass this
   *  to record other state at each position. */ 
  protected static class Position implements RollingBuffer.Resettable {
    // Buffered input tokens at this position:
    public final List<AttributeSource.State> inputTokens = new ArrayList<AttributeSource.State>();

    // Next buffered token to be returned to consumer:
    public int nextRead;

    // Any token leaving from this position should have this startOffset:
    public int startOffset = -1;

    // Any token arriving to this position should have this endOffset:
    public int endOffset = -1;

    @Override
    public void reset() {
      inputTokens.clear();
      nextRead = 0;
      startOffset = -1;
      endOffset = -1;
    }

    public void add(AttributeSource.State state) {
      inputTokens.add(state);
    }

    public AttributeSource.State nextState() {
      assert nextRead < inputTokens.size();
      return inputTokens.get(nextRead++);
    }
  }

  protected LookaheadTokenFilter(TokenStream input) {
    super(input);
  }

  /** Call this only from within afterPosition, to insert a new
   *  token.  After calling this you should set any
   *  necessary token you need. */
  protected void insertToken() throws IOException {
    if (tokenPending) {
      positions.get(inputPos).add(captureState());
      tokenPending = false;
    }
    assert !insertPending;
    insertPending = true;
  }

  /** This is called when all input tokens leaving a given
   *  position have been returned.  Override this and
   *  call createToken and then set whichever token's
   *  attributes you want, if you want to inject
   *  a token starting from this position. */
  protected void afterPosition() throws IOException {
  }

  protected abstract T newPosition();

  protected final RollingBuffer<T> positions = new RollingBuffer<T>() {
    @Override
    protected T newInstance() {
      return newPosition();
    }
  };

  /** Returns true if there is a new token. */
  protected boolean peekToken() throws IOException {
    if (DEBUG) {
      System.out.println("LTF.peekToken inputPos=" + inputPos + " outputPos=" + outputPos + " tokenPending=" + tokenPending);
    }
    assert !end;
    assert inputPos == -1 || outputPos <= inputPos;
    if (tokenPending) {
      positions.get(inputPos).add(captureState());
      tokenPending = false;
    }
    final boolean gotToken = input.incrementToken();
    if (DEBUG) {
      System.out.println("  input.incrToken() returned " + gotToken);
    }
    if (gotToken) {
      inputPos += posIncAtt.getPositionIncrement();
      assert inputPos >= 0;
      if (DEBUG) {
        System.out.println("  now inputPos=" + inputPos);
      }
      
      final Position startPosData = positions.get(inputPos);
      final Position endPosData = positions.get(inputPos + posLenAtt.getPositionLength());

      final int startOffset = offsetAtt.startOffset();
      if (startPosData.startOffset == -1) {
        startPosData.startOffset = startOffset;
      } else {
        // Make sure our input isn't messing up offsets:
        assert startPosData.startOffset == startOffset: "prev startOffset=" + startPosData.startOffset + " vs new startOffset=" + startOffset + " inputPos=" + inputPos;
      }

      final int endOffset = offsetAtt.endOffset();
      if (endPosData.endOffset == -1) {
        endPosData.endOffset = endOffset;
      } else {
        // Make sure our input isn't messing up offsets:
        assert endPosData.endOffset == endOffset: "prev endOffset=" + endPosData.endOffset + " vs new endOffset=" + endOffset + " inputPos=" + inputPos;
      }

      tokenPending = true;
    } else {
      end = true;
    }

    return gotToken;
  }

  /** Call this when you are done looking ahead; it will set
   *  the next token to return.  Return the boolean back to
   *  the caller. */
  protected boolean nextToken() throws IOException {
    //System.out.println("  nextToken: tokenPending=" + tokenPending);
    if (DEBUG) {
      System.out.println("LTF.nextToken inputPos=" + inputPos + " outputPos=" + outputPos + " tokenPending=" + tokenPending);
    }

    Position posData = positions.get(outputPos);

    // While loop here in case we have to
    // skip over a hole from the input:
    while (true) {

      //System.out.println("    check buffer @ outputPos=" +
      //outputPos + " inputPos=" + inputPos + " nextRead=" +
      //posData.nextRead + " vs size=" +
      //posData.inputTokens.size());

      // See if we have a previously buffered token to
      // return at the current position:
      if (posData.nextRead < posData.inputTokens.size()) {
        if (DEBUG) {
          System.out.println("  return previously buffered token");
        }
        // This position has buffered tokens to serve up:
        if (tokenPending) {
          positions.get(inputPos).add(captureState());
          tokenPending = false;
        }
        restoreState(positions.get(outputPos).nextState());
        //System.out.println("      return!");
        return true;
      }

      if (inputPos == -1 || outputPos == inputPos) {
        // No more buffered tokens:
        // We may still get input tokens at this position
        //System.out.println("    break buffer");
        if (tokenPending) {
          // Fast path: just return token we had just incr'd,
          // without having captured/restored its state:
          if (DEBUG) {
            System.out.println("  pass-through: return pending token");
          }
          tokenPending = false;
          return true;
        } else if (end || !peekToken()) {
          if (DEBUG) {
            System.out.println("  END");
          }
          return false;
        }
      } else {
        if (posData.startOffset != -1) {
          // This position had at least one token leaving
          if (DEBUG) {
            System.out.println("  call afterPosition");
          }
          afterPosition();
          if (insertPending) {
            // Subclass inserted a token at this same
            // position:
            if (DEBUG) {
              System.out.println("  return inserted token");
            }
            assert insertedTokenConsistent();
            insertPending = false;
            return true;
          }
        }

        // Done with this position; move on:
        outputPos++;
        if (DEBUG) {
          System.out.println("  next position: outputPos=" + outputPos);
        }
        positions.freeBefore(outputPos);
        posData = positions.get(outputPos);
      }
    }
  }

  // If subclass inserted a token, make sure it had in fact
  // looked ahead enough:
  private boolean insertedTokenConsistent() {
    final int posLen = posLenAtt.getPositionLength();
    final Position endPosData = positions.get(outputPos + posLen);
    assert endPosData.endOffset != -1;
    assert offsetAtt.endOffset() == endPosData.endOffset;
    return true;
  }

  // TODO: end()?
  // TODO: close()?

  @Override
  public void reset() throws IOException {
    super.reset();
    positions.reset();
    inputPos = -1;
    outputPos = 0;
    tokenPending = false;
    end = false;
  }
}
