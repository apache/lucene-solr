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
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * A factory of {@link
 * org.apache.lucene.search.matchhighlight.MatchHighlighter.FieldValueHighlighter} classes that
 * cover typical use cases (verbatim values, highlights, abbreviations).
 *
 * @see MatchHighlighter#appendFieldHighlighter
 */
public final class FieldValueHighlighters {
  private FieldValueHighlighters() {}

  private abstract static class AbstractFieldValueHighlighter
      implements MatchHighlighter.FieldValueHighlighter {
    private final BiPredicate<String, Boolean> testPredicate;

    protected AbstractFieldValueHighlighter(BiPredicate<String, Boolean> testPredicate) {
      this.testPredicate = testPredicate;
    }

    @Override
    public final boolean isApplicable(String field, boolean hasMatches) {
      return testPredicate.test(field, hasMatches);
    }
  }

  /**
   * Displays up to {@code maxLeadingCharacters} of the field's value, regardless of whether it
   * contained highlights or not.
   */
  public static MatchHighlighter.FieldValueHighlighter maxLeadingCharacters(
      int maxLeadingCharacters, String ellipsis, Set<String> fields) {
    PassageSelector passageSelector = defaultPassageSelector();
    PassageFormatter passageFormatter = new PassageFormatter(ellipsis, "", "");
    return new AbstractFieldValueHighlighter((field, hasMatches) -> fields.contains(field)) {
      @Override
      public List<String> format(
          String field,
          String[] values,
          String contiguousValue,
          List<OffsetRange> valueRanges,
          List<MatchHighlighter.QueryOffsetRange> matchOffsets) {
        List<Passage> bestPassages =
            passageSelector.pickBest(
                contiguousValue, Collections.emptyList(), maxLeadingCharacters, 1, valueRanges);

        return passageFormatter.format(contiguousValue, bestPassages, valueRanges);
      }

      @Override
      public Collection<String> alwaysFetchedFields() {
        return fields;
      }
    };
  }

  /** Default preconfigured {@link PassageSelector}. */
  public static PassageSelector defaultPassageSelector() {
    return new PassageSelector(
        PassageSelector.DEFAULT_SCORER, new BreakIteratorShrinkingAdjuster());
  }

  /**
   * Highlights fields matching predicate {@code matchFields} only if they contained query matches.
   */
  public static MatchHighlighter.FieldValueHighlighter highlighted(
      int maxPassageWindow,
      int maxPassages,
      PassageFormatter passageFormatter,
      Predicate<String> matchFields) {
    PassageSelector passageSelector = defaultPassageSelector();
    return new AbstractFieldValueHighlighter(
        (field, hasMatches) -> matchFields.test(field) && hasMatches) {
      @Override
      public List<String> format(
          String field,
          String[] values,
          String contiguousValue,
          List<OffsetRange> valueRanges,
          List<MatchHighlighter.QueryOffsetRange> matchOffsets) {
        assert matchOffsets != null;

        List<Passage> bestPassages =
            passageSelector.pickBest(
                contiguousValue, matchOffsets, maxPassageWindow, maxPassages, valueRanges);

        return passageFormatter.format(contiguousValue, bestPassages, valueRanges);
      }
    };
  }

  /** Always returns raw field values, no highlighting or value truncation is applied. */
  public static MatchHighlighter.FieldValueHighlighter verbatimValue(
      String field, String... moreFields) {
    HashSet<String> matchFields = new HashSet<>(Arrays.asList(moreFields));
    matchFields.add(field);
    return new AbstractFieldValueHighlighter((fld, hasMatches) -> matchFields.contains(fld)) {
      @Override
      public Collection<String> alwaysFetchedFields() {
        return matchFields;
      }

      @Override
      public List<String> format(
          String field,
          String[] values,
          String contiguousValue,
          List<OffsetRange> valueRanges,
          List<MatchHighlighter.QueryOffsetRange> matchOffsets) {
        return Arrays.asList(values);
      }
    };
  }

  /**
   * Matches all fields and omits their value in the output (so that no highlight or value is
   * emitted).
   */
  public static MatchHighlighter.FieldValueHighlighter skipRemaining() {
    return new AbstractFieldValueHighlighter((field, hasMatches) -> true) {
      @Override
      public List<String> format(
          String field,
          String[] values,
          String contiguousValue,
          List<OffsetRange> valueRanges,
          List<MatchHighlighter.QueryOffsetRange> matchOffsets) {
        return null;
      }
    };
  }
}
