package org.apache.lucene.search.highlight;


/**
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
import org.apache.lucene.analysis.Token;

import java.util.List;


/**
 * {@link Fragmenter} implementation which breaks text up into same-size
 * fragments but does not split up Spans. This is a simple sample class.
 */
public class SimpleSpanFragmenter implements Fragmenter {
  private static final int DEFAULT_FRAGMENT_SIZE = 100;
  private int fragmentSize;
  private int currentNumFrags;
  private int position = -1;
  private SpanScorer spanScorer;
  private int waitForPos = -1;

  /**
   * @param spanscorer SpanScorer that was used to score hits
   */
  public SimpleSpanFragmenter(SpanScorer spanscorer) {
    this(spanscorer, DEFAULT_FRAGMENT_SIZE);
  }

  /**
   * @param spanscorer SpanScorer that was used to score hits
   * @param fragmentSize size in bytes of each fragment
   */
  public SimpleSpanFragmenter(SpanScorer spanscorer, int fragmentSize) {
    this.fragmentSize = fragmentSize;
    this.spanScorer = spanscorer;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.Fragmenter#isNewFragment(org.apache.lucene.analysis.Token)
   */
  public boolean isNewFragment(Token token) {
    position += token.getPositionIncrement();

    if (waitForPos == position) {
      waitForPos = -1;
    } else if (waitForPos != -1) {
      return false;
    }

    WeightedSpanTerm wSpanTerm = spanScorer.getWeightedSpanTerm(token.term());

    if (wSpanTerm != null) {
      List positionSpans = wSpanTerm.getPositionSpans();

      for (int i = 0; i < positionSpans.size(); i++) {
        if (((PositionSpan) positionSpans.get(i)).start == position) {
          waitForPos = ((PositionSpan) positionSpans.get(i)).end + 1;

          return true;
        }
      }
    }

    boolean isNewFrag = token.endOffset() >= (fragmentSize * currentNumFrags);

    if (isNewFrag) {
      currentNumFrags++;
    }

    return isNewFrag;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.Fragmenter#start(java.lang.String)
   */
  public void start(String originalText) {
    position = 0;
    currentNumFrags = 1;
  }
}
