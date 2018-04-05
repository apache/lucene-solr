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

package org.apache.lucene.search.matchhighlight;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.util.BytesRef;

public interface SourceAwareMatchesIterator extends MatchesIterator {

  void addSource(byte[] source);

  static SourceAwareMatchesIterator wrapOffsets(MatchesIterator in) {
    return new SourceAwareMatchesIterator() {
      @Override
      public void addSource(byte[] source) {

      }

      @Override
      public boolean next() throws IOException {
        return in.next();
      }

      @Override
      public int startPosition() {
        return in.startPosition();
      }

      @Override
      public int endPosition() {
        return in.endPosition();
      }

      @Override
      public int startOffset() throws IOException {
        return in.startOffset();
      }

      @Override
      public int endOffset() throws IOException {
        return in.endOffset();
      }

      @Override
      public BytesRef term() {
        return in.term();
      }
    };
  }

  static SourceAwareMatchesIterator fromTokenStream(MatchesIterator in, Analyzer analyzer) {
    // build a TokenStream in setSource(), adding count * analyzer.getPositionIncrementGap
    // add an OffsetsAttribute
    // when in.next() is called, advance the ts until we're at the same position
    // return the values from the offset attribute
    return null;
  }

}
