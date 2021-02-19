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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;

/**
 * This strategy works for fields where we know the match occurred but there are no known positions
 * or offsets.
 *
 * <p>We re-analyze field values and return offset ranges for returned tokens that are also returned
 * by the query's term collector.
 */
public final class OffsetsFromTokens implements OffsetsRetrievalStrategy {
  private final String field;
  private final Analyzer analyzer;

  public OffsetsFromTokens(String field, Analyzer analyzer) {
    this.field = field;
    this.analyzer = analyzer;
  }

  @Override
  public List<OffsetRange> get(
      MatchesIterator matchesIterator, MatchRegionRetriever.FieldValueProvider doc)
      throws IOException {
    List<CharSequence> values = doc.getValues(field);

    Set<BytesRef> matchTerms = new HashSet<>();
    while (matchesIterator.next()) {
      Query q = matchesIterator.getQuery();
      q.visit(
          new QueryVisitor() {
            @Override
            public void consumeTerms(Query query, Term... terms) {
              for (Term t : terms) {
                if (field.equals(t.field())) {
                  matchTerms.add(t.bytes());
                }
              }
            }
          });
    }

    ArrayList<OffsetRange> ranges = new ArrayList<>();
    int valueOffset = 0;
    for (int valueIndex = 0, max = values.size(); valueIndex < max; valueIndex++) {
      final String value = values.get(valueIndex).toString();

      TokenStream ts = analyzer.tokenStream(field, value);
      OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
      TermToBytesRefAttribute termAttr = ts.getAttribute(TermToBytesRefAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        if (matchTerms.contains(termAttr.getBytesRef())) {
          int startOffset = valueOffset + offsetAttr.startOffset();
          int endOffset = valueOffset + offsetAttr.endOffset();
          ranges.add(new OffsetRange(startOffset, endOffset));
        }
      }
      ts.end();
      valueOffset += offsetAttr.endOffset() + analyzer.getOffsetGap(field);
      ts.close();
    }
    return ranges;
  }

  @Override
  public boolean requiresDocument() {
    return true;
  }
}
