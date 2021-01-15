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

package org.apache.lucene.luke.models.documents;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.luke.util.BytesRefUtils;

/**
 * Holder for term vector entry representing the term and their number of occurrences, and
 * optionally, positions in the document field.
 */
public final class TermVectorEntry {

  private final String termText;
  private final long freq;
  private final List<TermVectorPosition> positions;

  /**
   * Returns a new term vector entry representing the specified term, and optionally, positions.
   *
   * @param te - positioned terms iterator
   * @return term vector entry
   * @throws IOException - if there is a low level IO error.
   */
  static TermVectorEntry of(TermsEnum te) throws IOException {
    Objects.requireNonNull(te);

    String termText = BytesRefUtils.decode(te.term());

    List<TermVectorEntry.TermVectorPosition> tvPositions = new ArrayList<>();
    PostingsEnum pe = te.postings(null, PostingsEnum.OFFSETS);
    pe.nextDoc();
    int freq = pe.freq();
    for (int i = 0; i < freq; i++) {
      int pos = pe.nextPosition();
      if (pos < 0) {
        // no position information available
        continue;
      }
      TermVectorPosition tvPos = TermVectorPosition.of(pos, pe);
      tvPositions.add(tvPos);
    }

    return new TermVectorEntry(termText, te.totalTermFreq(), tvPositions);
  }

  private TermVectorEntry(String termText, long freq, List<TermVectorPosition> positions) {
    this.termText = termText;
    this.freq = freq;
    this.positions = positions;
  }

  /** Returns the string representation for this term. */
  public String getTermText() {
    return termText;
  }

  /** Returns the number of occurrences of this term in the document field. */
  public long getFreq() {
    return freq;
  }

  /** Returns the list of positions for this term in the document field. */
  public List<TermVectorPosition> getPositions() {
    return positions;
  }

  @Override
  public String toString() {
    String positionsStr =
        positions.stream().map(TermVectorPosition::toString).collect(Collectors.joining(","));

    return "TermVectorEntry{"
        + "termText='"
        + termText
        + '\''
        + ", freq="
        + freq
        + ", positions="
        + positionsStr
        + '}';
  }

  /** Holder for position information for a term vector entry. */
  public static final class TermVectorPosition {
    private final int position;
    private final int startOffset;
    private final int endOffset;

    /**
     * Returns a new position entry representing the specified posting, and optionally, start and
     * end offsets.
     *
     * @param pos - term position
     * @param pe - positioned postings iterator
     * @return position entry
     * @throws IOException - if there is a low level IO error.
     */
    static TermVectorPosition of(int pos, PostingsEnum pe) throws IOException {
      Objects.requireNonNull(pe);

      int sOffset = pe.startOffset();
      int eOffset = pe.endOffset();
      if (sOffset >= 0 && eOffset >= 0) {
        return new TermVectorPosition(pos, sOffset, eOffset);
      }
      return new TermVectorPosition(pos);
    }

    /** Returns the position for this term in the document field. */
    public int getPosition() {
      return position;
    }

    /**
     * Returns the start offset for this term in the document field. Empty Optional instance is
     * returned if no offset information available.
     */
    public OptionalInt getStartOffset() {
      return startOffset >= 0 ? OptionalInt.of(startOffset) : OptionalInt.empty();
    }

    /**
     * Returns the end offset for this term in the document field. Empty Optional instance is
     * returned if no offset information available.
     */
    public OptionalInt getEndOffset() {
      return endOffset >= 0 ? OptionalInt.of(endOffset) : OptionalInt.empty();
    }

    @Override
    public String toString() {
      return "TermVectorPosition{"
          + "position="
          + position
          + ", startOffset="
          + startOffset
          + ", endOffset="
          + endOffset
          + '}';
    }

    private TermVectorPosition(int position) {
      this(position, -1, -1);
    }

    private TermVectorPosition(int position, int startOffset, int endOffset) {
      this.position = position;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }
  }
}
