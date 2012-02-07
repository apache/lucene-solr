package org.apache.lucene.search;

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

import java.io.IOException;

/** Scorer for conjunctions, sets of terms, all of which are required. */
final class MatchOnlyConjunctionTermScorer extends ConjunctionTermScorer {
  MatchOnlyConjunctionTermScorer(Weight weight, float coord,
      DocsAndFreqs[] docsAndFreqs) throws IOException {
    super(weight, coord, docsAndFreqs);
  }

  @Override
  public float score() throws IOException {
    float sum = 0.0f;
    for (DocsAndFreqs docs : docsAndFreqs) {
      sum += docs.docScorer.score(lastDoc, 1);
    }
    return sum * coord;
  }
}
