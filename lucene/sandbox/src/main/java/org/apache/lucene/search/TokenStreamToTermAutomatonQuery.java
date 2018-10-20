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
package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.BytesRef;

/** Consumes a TokenStream and creates an {@link TermAutomatonQuery}
 *  where the transition labels are tokens from the {@link
 *  TermToBytesRefAttribute}.
 *
 *  <p>This code is very new and likely has exciting bugs!
 *
 *  @lucene.experimental */
public class TokenStreamToTermAutomatonQuery {

  private boolean preservePositionIncrements;

  /** Sole constructor. */
  public TokenStreamToTermAutomatonQuery() {
    this.preservePositionIncrements = true;
  }

  /** Whether to generate holes in the automaton for missing positions, <code>true</code> by default. */
  public void setPreservePositionIncrements(boolean enablePositionIncrements) {
    this.preservePositionIncrements = enablePositionIncrements;
  }

  /** Pulls the graph (including {@link
   *  PositionLengthAttribute}) from the provided {@link
   *  TokenStream}, and creates the corresponding
   *  automaton where arcs are bytes (or Unicode code points 
   *  if unicodeArcs = true) from each term. */
  public TermAutomatonQuery toQuery(String field, TokenStream in) throws IOException {

    final TermToBytesRefAttribute termBytesAtt = in.addAttribute(TermToBytesRefAttribute.class);
    final PositionIncrementAttribute posIncAtt = in.addAttribute(PositionIncrementAttribute.class);
    final PositionLengthAttribute posLengthAtt = in.addAttribute(PositionLengthAttribute.class);
    final OffsetAttribute offsetAtt = in.addAttribute(OffsetAttribute.class);

    in.reset();

    TermAutomatonQuery query = new TermAutomatonQuery(field);

    int pos = -1;
    int lastPos = 0;
    int maxOffset = 0;
    int maxPos = -1;
    int state = -1;
    while (in.incrementToken()) {
      int posInc = posIncAtt.getPositionIncrement();
      if (preservePositionIncrements == false && posInc > 1) {
        posInc = 1;
      }
      assert pos > -1 || posInc > 0;

      if (posInc > 1) {
        throw new IllegalArgumentException("cannot handle holes; to accept any term, use '*' term");
      }

      if (posInc > 0) {
        // New node:
        pos += posInc;
      }

      int endPos = pos + posLengthAtt.getPositionLength();
      while (state < endPos) {
        state = query.createState();
      }

      BytesRef term = termBytesAtt.getBytesRef();
      //System.out.println(pos + "-" + endPos + ": " + term.utf8ToString() + ": posInc=" + posInc);
      if (term.length == 1 && term.bytes[term.offset] == (byte) '*') {
        query.addAnyTransition(pos, endPos);
      } else {
        query.addTransition(pos, endPos, term);
      }

      maxOffset = Math.max(maxOffset, offsetAtt.endOffset());
      maxPos = Math.max(maxPos, endPos);
    }

    in.end();

    // TODO: look at endOffset?  ts2a did...

    // TODO: this (setting "last" state as the only accept state) may be too simplistic?
    query.setAccept(state, true);
    query.finish();

    return query;
  }
}
