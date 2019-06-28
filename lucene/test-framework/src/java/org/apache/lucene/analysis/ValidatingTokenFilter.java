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
import java.io.PrintStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;

// TODO: rename to OffsetsXXXTF?  ie we only validate
// offsets (now anyway...)

// TODO: also make a DebuggingTokenFilter, that just prints
// all att values that come through it...

// TODO: BTSTC should just append this to the chain
// instead of checking itself:

/** A TokenFilter that checks consistency of the tokens (eg
 *  offsets are consistent with one another). */
public final class ValidatingTokenFilter extends TokenFilter {

  private static final int MAX_DEBUG_TOKENS = 20;

  private int pos;
  private int lastStartOffset;

  // Maps position to the start/end offset:
  private final Map<Integer,Integer> posToStartOffset = new HashMap<>();
  private final Map<Integer,Integer> posToEndOffset = new HashMap<>();

  private final PositionIncrementAttribute posIncAtt = getAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = getAttribute(PositionLengthAttribute.class);
  private final OffsetAttribute offsetAtt = getAttribute(OffsetAttribute.class);
  private final CharTermAttribute termAtt = getAttribute(CharTermAttribute.class);

  // record the last MAX_DEBUG_TOKENS tokens seen so they can be dumped on failure
  private final List<Token> tokens = new LinkedList<>();

  private final String name;

  /** The name arg is used to identify this stage when
   *  throwing exceptions (useful if you have more than one
   *  instance in your chain). */
  public ValidatingTokenFilter(TokenStream in, String name) {
    super(in);
    this.name = name;
  }

  @Override
  public boolean incrementToken() throws IOException {

    // System.out.println(name + ": incrementToken()");

    if (!input.incrementToken()) {
      return false;
    }

    int startOffset = 0;
    int endOffset = 0;
    int posLen = 0;
    int posInc = 0;

    if (posIncAtt != null) {
      posInc = posIncAtt.getPositionIncrement();
    }
    if (offsetAtt != null) {
      startOffset = offsetAtt.startOffset();
      endOffset = offsetAtt.endOffset();
    }

    posLen = posLenAtt == null ? 1 : posLenAtt.getPositionLength();

    addToken(startOffset, endOffset, posInc);

    // System.out.println(name + ": " + this);
    
    if (posIncAtt != null) {
      pos += posInc;
      if (pos == -1) {
        dumpValidatingTokenFilters(this, System.err);
        throw new IllegalStateException(name + ": first posInc must be > 0");
      }
    }
    
    if (offsetAtt != null) {
      if (startOffset < lastStartOffset) {
        dumpValidatingTokenFilters(this, System.err);
        throw new IllegalStateException(name + ": offsets must not go backwards startOffset=" + startOffset + " is < lastStartOffset=" + lastStartOffset);
      }
      lastStartOffset = offsetAtt.startOffset();
    }
    
    if (offsetAtt != null && posIncAtt != null) {

      if (!posToStartOffset.containsKey(pos)) {
        // First time we've seen a token leaving from this position:
        posToStartOffset.put(pos, startOffset);
        // System.out.println(name + "  + s " + pos + " -> " + startOffset);
      } else {
        // We've seen a token leaving from this position
        // before; verify the startOffset is the same:
        // System.out.println(name + "  + vs " + pos + " -> " + startOffset);
        final int oldStartOffset = posToStartOffset.get(pos);
        if (oldStartOffset != startOffset) {
          dumpValidatingTokenFilters(this, System.err);
          throw new IllegalStateException(name + ": inconsistent startOffset at pos=" + pos + ": " + oldStartOffset + " vs " + startOffset + "; token=" + termAtt);
        }
      }

      final int endPos = pos + posLen;

      if (!posToEndOffset.containsKey(endPos)) {
        // First time we've seen a token arriving to this position:
        posToEndOffset.put(endPos, endOffset);
        //System.out.println(name + "  + e " + endPos + " -> " + endOffset);
      } else {
        // We've seen a token arriving to this position
        // before; verify the endOffset is the same:
        //System.out.println(name + "  + ve " + endPos + " -> " + endOffset);
        final int oldEndOffset = posToEndOffset.get(endPos);
        if (oldEndOffset != endOffset) {
          dumpValidatingTokenFilters(this, System.err);
          throw new IllegalStateException(name + ": inconsistent endOffset at pos=" + endPos + ": " + oldEndOffset + " vs " + endOffset + "; token=" + termAtt);
        }
      }
    }

    return true;
  }

  @Override
  public void end() throws IOException {
    super.end();

    // TODO: what else to validate

    // TODO: check that endOffset is >= max(endOffset)
    // we've seen
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    pos = -1;
    posToStartOffset.clear();
    posToEndOffset.clear();
    lastStartOffset = 0;
    tokens.clear();
  }


  private void addToken(int startOffset, int endOffset, int posInc) {
    if (tokens.size() == MAX_DEBUG_TOKENS) {
      tokens.remove(0);
    }
    tokens.add(new Token(termAtt.toString(), posInc, startOffset, endOffset));
  }

  /**
   * Prints details about consumed tokens stored in any ValidatingTokenFilters in the input chain
   * @param in the input token stream
   * @param out the output print stream
   */
  public static void dumpValidatingTokenFilters(TokenStream in, PrintStream out) {
    if (in instanceof TokenFilter) {
      dumpValidatingTokenFilters(((TokenFilter) in).input, out);
      if (in instanceof ValidatingTokenFilter) {
        out.println(((ValidatingTokenFilter) in).dump());
      }
    }
  }

  public String dump() {
    StringBuilder buf = new StringBuilder();
    buf.append(name).append(": ");
    for (Token token : tokens) {
      buf.append(String.format(Locale.ROOT, "%s<[%d-%d] +%d> ",
          token, token.startOffset(), token.endOffset(), token.getPositionIncrement()));
    }
    return buf.toString();
  }

}
