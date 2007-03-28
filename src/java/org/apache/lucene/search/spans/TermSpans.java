package org.apache.lucene.search.spans;
/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;

import java.io.IOException;

/**
 * Expert:
 * Public for extension only
 */
public class TermSpans implements Spans {
  protected TermPositions positions;
  protected Term term;
  protected int doc;
  protected int freq;
  protected int count;
  protected int position;


  public TermSpans(TermPositions positions, Term term) throws IOException {

    this.positions = positions;
    this.term = term;
    doc = -1;
  }

  public boolean next() throws IOException {
    if (count == freq) {
      if (!positions.next()) {
        doc = Integer.MAX_VALUE;
        return false;
      }
      doc = positions.doc();
      freq = positions.freq();
      count = 0;
    }
    position = positions.nextPosition();
    count++;
    return true;
  }

  public boolean skipTo(int target) throws IOException {
    // are we already at the correct position?
    if (doc >= target) {
      return true;
    }

    if (!positions.skipTo(target)) {
      doc = Integer.MAX_VALUE;
      return false;
    }

    doc = positions.doc();
    freq = positions.freq();
    count = 0;

    position = positions.nextPosition();
    count++;

    return true;
  }

  public int doc() {
    return doc;
  }

  public int start() {
    return position;
  }

  public int end() {
    return position + 1;
  }

  public String toString() {
    return "spans(" + term.toString() + ")@" +
            (doc == -1 ? "START" : (doc == Integer.MAX_VALUE) ? "END" : doc + "-" + position);
  }


  public TermPositions getPositions() {
    return positions;
  }
}
