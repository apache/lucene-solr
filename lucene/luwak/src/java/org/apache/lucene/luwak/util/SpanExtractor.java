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

package org.apache.lucene.luwak.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.Spans;

/**
 * Utility methods for extracting and collecting Spans from query trees
 */
public class SpanExtractor {

  private SpanExtractor() {
  }

  /**
   * Get a list of all Spans made available from the passed-in Scorer
   *
   * @param scorer         the scorer to extract spans from
   * @param errorOnNoSpans if true, throw an error if no Spans can be extracted
   *                       from the Scorer or any of its children
   * @return a List of Spans
   */
  public static List<Spans> getSpans(Scorable scorer, boolean errorOnNoSpans) throws IOException {

    List<Spans> spans = new ArrayList<>();
    if (scorer instanceof SpanScorer) {
      spans.add(((SpanScorer) scorer).getSpans());
      return spans;
    }

    Collection<Scorable.ChildScorable> children = scorer.getChildren();
    if (errorOnNoSpans && children.isEmpty())
      throw new RuntimeException("Couldn't extract SpanScorer from " + scorer.getClass().getCanonicalName());

    for (Scorable.ChildScorable child : children) {
      spans.addAll(getSpans(child.child, errorOnNoSpans));
    }

    return spans;
  }

  /**
   * Collect all Spans extracted from a Scorer using a SpanCollector
   *
   * @param scorer         the scorer to extract Spans from
   * @param collector      the SpanCollector
   * @param errorOnNoSpans if true, throw an error if no Spans can be extracted
   *                       from the Scorer or any of its children
   * @throws IOException on error
   */
  public static void collect(Scorable scorer, SpanCollector collector, boolean errorOnNoSpans) throws IOException {

    List<Spans> allSpans = getSpans(scorer, errorOnNoSpans);
    int doc = scorer.docID();

    for (Spans spans : allSpans) {
      int spanDoc = spans.docID();
      // if the Scorer advances lazily, then not all of its subspans may be on
      // the correct document
      if (spanDoc == doc || (spanDoc < doc && spans.advance(doc) == doc)) {
        while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
          if (spans.startPosition() != -1)
            spans.collect(collector);
        }
      }
    }

  }
}
