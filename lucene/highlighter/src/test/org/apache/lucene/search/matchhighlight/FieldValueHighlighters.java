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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * A factory of predefined field highlighters.
 *
 * @see MatchHighlighter#addFieldHighlighter
 */
public final class FieldValueHighlighters {
  private FieldValueHighlighters() {
  }

  private static abstract class AbstractFieldValueHighlighter implements MatchHighlighter.FieldValueHighlighter {
    private final BiPredicate<String, Boolean> testPredicate;

    protected AbstractFieldValueHighlighter(BiPredicate<String, Boolean> testPredicate) {
      this.testPredicate = testPredicate;
    }

    @Override
    public final boolean test(String field, boolean hasMatches) {
      return testPredicate.test(field, hasMatches);
    }

    @Override
    public Collection<String> alwaysFetchedFields() {
      return Collections.emptyList();
    }
  }

  public static MatchHighlighter.FieldValueHighlighter highlighted(
      int maxPassageWindow,
      int maxPassages,
      String ellipsis,
      String markerStart,
      String markerEnd,
      Predicate<String> matchFields) {
    return highlighted(maxPassageWindow, maxPassages, new PassageFormatter(ellipsis, markerStart, markerEnd), matchFields);
  }

  /**
   * Highlight the given fields only if they contained matches.
   */
  public static MatchHighlighter.FieldValueHighlighter highlighted(
      int maxPassageWindow,
      int maxPassages,
      PassageFormatter passageFormatter,
      Predicate<String> matchFields) {
    PassageSelector passageSelector = new PassageSelector();
    return new AbstractFieldValueHighlighter((field, hasMatches) -> matchFields.test(field) && hasMatches) {
      @Override
      public List<String> format(String[] values, String contiguousValue,
                                 List<OffsetRange> valueRanges, List<MatchHighlighter.QueryOffsetRange> matchOffsets) {
        assert matchOffsets != null;

        List<Passage> bestPassages =
            passageSelector.pickBest(contiguousValue, matchOffsets, maxPassageWindow, maxPassages, valueRanges);

        return passageFormatter.format(contiguousValue, bestPassages, valueRanges);
      }
    };
  }

  /**
   * Always returns raw field values, no highlighting or value truncation is applied.
   */
  public static MatchHighlighter.FieldValueHighlighter verbatimValue(String... fields) {
    HashSet<String> matchFields = new HashSet<>(Arrays.asList(fields));
    return new AbstractFieldValueHighlighter((field, hasMatches) -> matchFields.contains(field)) {
      @Override
      public Collection<String> alwaysFetchedFields() {
        return matchFields;
      }

      @Override
      public List<String> format(String[] values, String contiguousValue, List<OffsetRange> valueRanges,
                                 List<MatchHighlighter.QueryOffsetRange> matchOffsets) {
        return Arrays.asList(values);
      }
    };
  }

  /**
   * Matches all fields and omits their value in the output (so that no highlight or value is emitted).
   */
  public static MatchHighlighter.FieldValueHighlighter skipRemaining() {
    return new AbstractFieldValueHighlighter((field, hasMatches) -> true) {
      @Override
      public List<String> format(String[] values, String contiguousValue, List<OffsetRange> valueRanges,
                                 List<MatchHighlighter.QueryOffsetRange> matchOffsets) {
        return null;
      }
    };
  }
}
