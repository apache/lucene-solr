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

import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.util.BytesRef;

public interface SourceAwareMatchesIterator extends MatchesIterator, Closeable {

  void addSource(String source) throws IOException;

  static SourceAwareMatchesIterator wrapOffsets(MatchesIterator in) {
    return new SourceAwareMatchesIterator() {
      @Override
      public void close() throws IOException {
        // no-op
      }

      @Override
      public void addSource(String source) {
        // no-op - offsets already provided
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

  static SourceAwareMatchesIterator fromTokenStream(MatchesIterator in, String field, Analyzer analyzer) {
    return new SourceAwareMatchesIterator() {

      int sourceCount = -1;
      int tsPosition = 0;
      TokenStream ts;
      OffsetAttribute offsetAttribute;
      PositionIncrementAttribute posIncAttribute;

      int startPosition, endPosition, startOffset, endOffset;

      @Override
      public void addSource(String source) throws IOException {
        sourceCount++;
        if (sourceCount > 0) {
          tsPosition += analyzer.getPositionIncrementGap(field);
        }
        ts = analyzer.tokenStream(field, new StringReader(source));
        offsetAttribute = ts.getAttribute(OffsetAttribute.class);
        posIncAttribute = ts.getAttribute(PositionIncrementAttribute.class);
        ts.reset();
      }

      @Override
      public boolean next() throws IOException {
        boolean next = in.next();
        if (next == false) {
          return false;
        }
        startPosition = in.startPosition();
        if (advanceTokenStream(startPosition) == false) {
          return false;
        }
        startOffset = offsetAttribute.startOffset();
        endPosition = in.endPosition();
        if (advanceTokenStream(endPosition) == false) {
          return false;
        }
        endOffset = offsetAttribute.endOffset();
        return true;
      }

      private boolean advanceTokenStream(int targetPos) throws IOException {
        while (tsPosition < targetPos) {
          if (ts.incrementToken() == false) {
            return false;
          }
          tsPosition += posIncAttribute.getPositionIncrement();
        }
        return true;
      }

      @Override
      public int startPosition() {
        return startPosition;
      }

      @Override
      public int endPosition() {
        return endPosition;
      }

      @Override
      public int startOffset() throws IOException {
        return startOffset;
      }

      @Override
      public int endOffset() throws IOException {
        return endOffset;
      }

      @Override
      public BytesRef term() {
        return in.term();
      }

      @Override
      public void close() throws IOException {
        if (ts != null) {
          ts.close();
        }
      }
    };
  }

}
