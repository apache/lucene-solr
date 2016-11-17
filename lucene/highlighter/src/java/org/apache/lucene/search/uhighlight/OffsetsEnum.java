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
package org.apache.lucene.search.uhighlight;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;

/**
 * Holds the term &amp; PostingsEnum, and info for tracking the occurrences of a term within the text.
 * It is advanced with the underlying postings and is placed in a priority queue by highlightOffsetsEnums
 * based on the start offset.
 *
 * @lucene.internal
 */
public class OffsetsEnum implements Comparable<OffsetsEnum>, Closeable {
  private final BytesRef term;
  final PostingsEnum postingsEnum; // with offsets

  float weight; // set once in highlightOffsetsEnums
  private int pos = 0; // the occurrence counter of this term within the text being highlighted.

  public OffsetsEnum(BytesRef term, PostingsEnum postingsEnum) throws IOException {
    this.term = term; // can be null
    this.postingsEnum = Objects.requireNonNull(postingsEnum);
  }

  // note: the ordering clearly changes as the postings enum advances
  @Override
  public int compareTo(OffsetsEnum other) {
    try {
      int cmp = Integer.compare(startOffset(), other.startOffset());
      if (cmp != 0) {
        return cmp; // vast majority of the time we return here.
      }
      if (this.term == null || other.term == null) {
        if (this.term == null && other.term == null) {
          return 0;
        } else if (this.term == null) {
          return 1; // put "this" (wildcard mtq enum) last
        } else {
          return -1;
        }
      }
      return term.compareTo(other.term);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  BytesRef getTerm() throws IOException {
    // TODO TokenStreamOffsetStrategy could override OffsetsEnum; then remove this hack here
    return term != null ? term : postingsEnum.getPayload(); // abusing payload like this is a total hack!
  }

  boolean hasMorePositions() throws IOException {
    return pos < postingsEnum.freq();
  }

  void nextPosition() throws IOException {
    assert hasMorePositions();
    pos++;
    postingsEnum.nextPosition();
  }

  int startOffset() throws IOException {
    return postingsEnum.startOffset();
  }

  int endOffset() throws IOException {
    return postingsEnum.endOffset();
  }

  @Override
  public void close() throws IOException {
    // TODO TokenStreamOffsetStrategy could override OffsetsEnum; then this base impl would be no-op.
    if (postingsEnum instanceof Closeable) {
      ((Closeable) postingsEnum).close();
    }
  }
}
