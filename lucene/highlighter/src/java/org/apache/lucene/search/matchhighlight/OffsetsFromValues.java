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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.search.MatchesIterator;

/**
 * This strategy works for fields where we know the match occurred but there are no known positions
 * or offsets.
 *
 * <p>We re-analyze field values and return offset ranges for entire values (not individual tokens).
 * Re-analysis is required because analyzer may return an unknown offset gap.
 */
public final class OffsetsFromValues implements OffsetsRetrievalStrategy {
  private final String field;
  private final Analyzer analyzer;

  public OffsetsFromValues(String field, Analyzer analyzer) {
    this.field = field;
    this.analyzer = analyzer;
  }

  @Override
  public List<OffsetRange> get(
      MatchesIterator matchesIterator, MatchRegionRetriever.FieldValueProvider doc)
      throws IOException {
    List<CharSequence> values = doc.getValues(field);

    ArrayList<OffsetRange> ranges = new ArrayList<>();
    int valueOffset = 0;
    for (CharSequence charSequence : values) {
      final String value = charSequence.toString();

      TokenStream ts = analyzer.tokenStream(field, value);
      OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
      ts.reset();
      int startOffset = valueOffset;
      while (ts.incrementToken()) {
        // Go through all tokens to increment offset attribute properly.
      }
      ts.end();
      valueOffset += offsetAttr.endOffset();
      ranges.add(new OffsetRange(startOffset, valueOffset));
      valueOffset += analyzer.getOffsetGap(field);
      ts.close();
    }
    return ranges;
  }

  @Override
  public boolean requiresDocument() {
    return true;
  }
}
