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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

/** A simple ASCII match range highlighter for tests. */
final class AsciiMatchRangeHighlighter {
  private final Analyzer analyzer;
  private final PassageFormatter passageFormatter;
  private final PassageSelector selector;

  private int maxPassageWindow = 160;
  private int maxPassages = 10;

  public AsciiMatchRangeHighlighter(Analyzer analyzer) {
    this.passageFormatter = new PassageFormatter("...", ">", "<");
    this.selector = new PassageSelector();
    this.analyzer = analyzer;
  }

  public Map<String, List<String>> apply(
      Document document, Map<String, List<OffsetRange>> fieldHighlights) {
    ArrayList<OffsetRange> valueRanges = new ArrayList<>();
    Map<String, List<String>> fieldSnippets = new LinkedHashMap<>();

    fieldHighlights.forEach(
        (field, matchRanges) -> {
          int offsetGap = analyzer.getOffsetGap(field);

          String[] values = document.getValues(field);
          String value;
          if (values.length == 1) {
            value = values[0];
          } else {
            // This can be inefficient if offset gap is large but recomputing
            // offsets in a smart way doesn't make sense for tests.
            String fieldGapPadding = " ".repeat(offsetGap);
            value = String.join(fieldGapPadding, values);
          }

          // Create permitted range windows for passages so that they don't cross
          // multi-value boundary.
          valueRanges.clear();
          int offset = 0;
          for (CharSequence v : values) {
            valueRanges.add(new OffsetRange(offset, offset + v.length()));
            offset += v.length();
            offset += offsetGap;
          }

          List<Passage> passages =
              selector.pickBest(value, matchRanges, maxPassageWindow, maxPassages, valueRanges);

          fieldSnippets.put(field, passageFormatter.format(value, passages, valueRanges));
        });

    return fieldSnippets;
  }
}
