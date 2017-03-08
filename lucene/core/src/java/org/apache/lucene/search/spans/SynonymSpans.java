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
package org.apache.lucene.search.spans;

import java.util.List;

import org.apache.lucene.search.similarities.Similarity.SimScorer;


/**
 * A spans for merging and equal scoring of given spans.
 * This does not provide score values.
 *
 * @lucene.experimental
 */
public class SynonymSpans extends DisjunctionSpans {
  SimScorer simScorer;

  /** Construct a SynonymSpans.
   * @param spanQuery   The query that provides the subSpans.
   * @param subSpans    Over which the disjunction is to be taken.
   * @param simScorer   To be used for scoring.
   */
  public SynonymSpans(SpanQuery spanQuery, List<Spans> subSpans, SimScorer simScorer) {
    super(spanQuery, subSpans);
    this.simScorer = simScorer;
  }

  @Override
  public String toString() {
    return "SynonymSpans(" + spanQuery + ")@" + docID() + ": " + startPosition() + " - " + endPosition();
  }
}
