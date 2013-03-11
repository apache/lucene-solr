package org.apache.lucene.analysis;

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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RollingBuffer;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.State;
import org.apache.lucene.util.automaton.Transition;

// TODO: maybe also toFST?  then we can translate atts into FST outputs/weights

/** Consumes a TokenStream and creates an {@link Automaton}
 *  where the transition labels are UTF8 bytes from the {@link
 *  TermToBytesRefAttribute}.  Between tokens we insert
 *  POS_SEP and for holes we insert HOLE.
 *
 * @lucene.experimental */
public class TokenStreamToAutomaton {

  /** Sole constructor. */
  public TokenStreamToAutomaton() {
  }

  private static class Position implements RollingBuffer.Resettable {
    // Any tokens that ended at our position arrive to this state:
    State arriving;

    // Any tokens that start at our position leave from this state:
    State leaving;

    @Override
    public void reset() {
      arriving = null;
      leaving = null;
    }
  }

  private static class Positions extends RollingBuffer<Position> {
    @Override
    protected Position newInstance() {
      return new Position();
    }
  }

  /** Subclass & implement this if you need to change the
   *  token (such as escaping certain bytes) before it's
   *  turned into a graph. */ 
  protected BytesRef changeToken(BytesRef in) {
    return in;
  }

  /** We create transition between two adjacent tokens. */
  public static final int POS_SEP = 256;

  /** We add this arc to represent a hole. */
  public static final int HOLE = 257;

  /** Pulls the graph (including {@link
   *  PositionLengthAttribute}) from the provided {@link
   *  TokenStream}, and creates the corresponding
   *  automaton where arcs are bytes from each term. */
  public Automaton toAutomaton(TokenStream in) throws IOException {
    final Automaton a = new Automaton();
    boolean deterministic = true;

    final TermToBytesRefAttribute termBytesAtt = in.addAttribute(TermToBytesRefAttribute.class);
    final PositionIncrementAttribute posIncAtt = in.addAttribute(PositionIncrementAttribute.class);
    final PositionLengthAttribute posLengthAtt = in.addAttribute(PositionLengthAttribute.class);
    final OffsetAttribute offsetAtt = in.addAttribute(OffsetAttribute.class);

    final BytesRef term = termBytesAtt.getBytesRef();

    in.reset();

    // Only temporarily holds states ahead of our current
    // position:

    final RollingBuffer<Position> positions = new Positions();

    int pos = -1;
    Position posData = null;
    int maxOffset = 0;
    while (in.incrementToken()) {
      int posInc = posIncAtt.getPositionIncrement();
      assert pos > -1 || posInc > 0;

      if (posInc > 0) {

        // New node:
        pos += posInc;

        posData = positions.get(pos);
        assert posData.leaving == null;

        if (posData.arriving == null) {
          // No token ever arrived to this position
          if (pos == 0) {
            // OK: this is the first token
            posData.leaving = a.getInitialState();
          } else {
            // This means there's a hole (eg, StopFilter
            // does this):
            posData.leaving = new State();
            addHoles(a.getInitialState(), positions, pos);
          }
        } else {
          posData.leaving = new State();
          posData.arriving.addTransition(new Transition(POS_SEP, posData.leaving));
          if (posInc > 1) {
            // A token spanned over a hole; add holes
            // "under" it:
            addHoles(a.getInitialState(), positions, pos);
          }
        }
        positions.freeBefore(pos);
      } else {
        // note: this isn't necessarily true. its just that we aren't surely det.
        // we could optimize this further (e.g. buffer and sort synonyms at a position)
        // but thats probably overkill. this is cheap and dirty
        deterministic = false;
      }

      final int endPos = pos + posLengthAtt.getPositionLength();

      termBytesAtt.fillBytesRef();
      final BytesRef term2 = changeToken(term);
      final Position endPosData = positions.get(endPos);
      if (endPosData.arriving == null) {
        endPosData.arriving = new State();
      }

      State state = posData.leaving;
      for(int byteIDX=0;byteIDX<term2.length;byteIDX++) {
        final State nextState = byteIDX == term2.length-1 ? endPosData.arriving : new State();
        state.addTransition(new Transition(term2.bytes[term2.offset + byteIDX] & 0xff, nextState));
        state = nextState;
      }

      maxOffset = Math.max(maxOffset, offsetAtt.endOffset());
    }

    in.end();
    State endState = null;
    if (offsetAtt.endOffset() > maxOffset) {
      endState = new State();
      endState.setAccept(true);
    }

    pos++;
    while (pos <= positions.getMaxPos()) {
      posData = positions.get(pos);
      if (posData.arriving != null) {
        if (endState != null) {
          posData.arriving.addTransition(new Transition(POS_SEP, endState));
        } else {
          posData.arriving.setAccept(true);
        }
      }
      pos++;
    }

    //toDot(a);
    a.setDeterministic(deterministic);
    return a;
  }

  // for debugging!
  /*
  private static void toDot(Automaton a) throws IOException {
    final String s = a.toDot();
    Writer w = new OutputStreamWriter(new FileOutputStream("/tmp/out.dot"));
    w.write(s);
    w.close();
    System.out.println("TEST: saved to /tmp/out.dot");
  }
  */

  private static void addHoles(State startState, RollingBuffer<Position> positions, int pos) {
    Position posData = positions.get(pos);
    Position prevPosData = positions.get(pos-1);

    while(posData.arriving == null || prevPosData.leaving == null) {
      if (posData.arriving == null) {
        posData.arriving = new State();
        posData.arriving.addTransition(new Transition(POS_SEP, posData.leaving));
      }
      if (prevPosData.leaving == null) {
        if (pos == 1) {
          prevPosData.leaving = startState;
        } else {
          prevPosData.leaving = new State();
        }
        if (prevPosData.arriving != null) {
          prevPosData.arriving.addTransition(new Transition(POS_SEP, prevPosData.leaving));
        }
      }
      prevPosData.leaving.addTransition(new Transition(HOLE, posData.arriving));
      pos--;
      if (pos <= 0) {
        break;
      }
      posData = prevPosData;
      prevPosData = positions.get(pos-1);
    }
  }
}
