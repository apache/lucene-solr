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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.MatchesIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This strategy applies to fields with stored positions but no offsets. We re-analyze
 * the field's value to find out offsets of match positions.
 * <p>
 * Note that this may fail if index data (positions stored in the index) is out of sync
 * with the field values or the analyzer. This strategy assumes it'll never happen.
 */
public final class OffsetsFromPositions implements OffsetsRetrievalStrategy {
  private final String field;
  private final Analyzer analyzer;

  OffsetsFromPositions(String field, Analyzer analyzer) {
    this.field = field;
    this.analyzer = analyzer;
  }

  @Override
  public List<OffsetRange> get(MatchesIterator matchesIterator, MatchRegionRetriever.FieldValueProvider doc)
      throws IOException {
    ArrayList<OffsetRange> ranges = new ArrayList<>();
    while (matchesIterator.next()) {
      int from = matchesIterator.startPosition();
      int to = matchesIterator.endPosition();
      if (from < 0 || to < 0) {
        throw new IOException("Matches API returned negative positions for field: " + field);
      }
      ranges.add(new OffsetRange(from, to));
    }

    // Convert from positions to offsets.
    ranges = convertPositionsToOffsets(ranges, analyzer, field, doc.getValues(field));

    return ranges;
  }

  @Override
  public boolean requiresDocument() {
    return true;
  }

  private static ArrayList<OffsetRange> convertPositionsToOffsets(
      ArrayList<OffsetRange> ranges,
      Analyzer analyzer,
      String fieldName,
      List<CharSequence> values)
      throws IOException {

    if (ranges.isEmpty()) {
      return ranges;
    }

    class LeftRight {
      int left = Integer.MAX_VALUE;
      int right = Integer.MIN_VALUE;

      @Override
      public String toString() {
        return "[" + "L: " + left + ", R: " + right + ']';
      }
    }

    Map<Integer, LeftRight> requiredPositionSpans = new HashMap<>();
    int minPosition = Integer.MAX_VALUE;
    int maxPosition = Integer.MIN_VALUE;
    for (OffsetRange range : ranges) {
      requiredPositionSpans.computeIfAbsent(range.from, (key) -> new LeftRight());
      requiredPositionSpans.computeIfAbsent(range.to, (key) -> new LeftRight());
      minPosition = Math.min(minPosition, range.from);
      maxPosition = Math.max(maxPosition, range.to);
    }

    int position = -1;
    int valueOffset = 0;
    for (int valueIndex = 0, max = values.size(); valueIndex < max; valueIndex++) {
      final String value = values.get(valueIndex).toString();
      final boolean lastValue = valueIndex + 1 == max;

      TokenStream ts = analyzer.tokenStream(fieldName, value);
      OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
      PositionIncrementAttribute posAttr = ts.getAttribute(PositionIncrementAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        position += posAttr.getPositionIncrement();

        if (position >= minPosition) {
          LeftRight leftRight = requiredPositionSpans.get(position);
          if (leftRight != null) {
            int startOffset = valueOffset + offsetAttr.startOffset();
            int endOffset = valueOffset + offsetAttr.endOffset();

            leftRight.left = Math.min(leftRight.left, startOffset);
            leftRight.right = Math.max(leftRight.right, endOffset);
          }

          // Only short-circuit if we're on the last value (which should be the common
          // case since most fields would only have a single value anyway). We need
          // to make sure of this because otherwise offsetAttr would have incorrect value.
          if (position > maxPosition && lastValue) {
            break;
          }
        }
      }
      ts.end();
      position += posAttr.getPositionIncrement() + analyzer.getPositionIncrementGap(fieldName);
      valueOffset += offsetAttr.endOffset() + analyzer.getOffsetGap(fieldName);
      ts.close();
    }

    ArrayList<OffsetRange> converted = new ArrayList<>();
    for (OffsetRange range : ranges) {
      LeftRight left = requiredPositionSpans.get(range.from);
      LeftRight right = requiredPositionSpans.get(range.to);
      if (left == null
          || right == null
          || left.left == Integer.MAX_VALUE
          || right.right == Integer.MIN_VALUE) {
        throw new RuntimeException("Position not properly initialized for range: " + range);
      }
      converted.add(new OffsetRange(left.left, right.right));
    }

    return converted;
  }
}
