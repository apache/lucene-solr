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

import java.io.IOException;
import java.util.Set;

/**
 * Record span matches in a document and compute a document score.
 * <br>
 * For {@link SpansTreeQuery}. Public for extension.
 *
 * @lucene.experimental
 */
public abstract class SpansDocScorer<SpansT extends Spans> {
  protected int currentDoc;

  /**
   * Create a SpansDocScorer
   */
  public SpansDocScorer() {
    currentDoc = -1;
  }

  /** The document for which matches are recorded. */
  public int docID() { return currentDoc; }

  /** Called before the first match of the spans is to be recorded for the document. */
  public void beginDoc(int doc) throws IOException {
    currentDoc = doc;
  }

  /** Provide the SpansDocScorers at the current document. */
  public abstract void extractSpansDocScorersAtDoc(Set<AsSingleTermSpansDocScorer<?>> spansDocScorersAtDoc);

  /** Record a match with its slop factor at the given position. */
  public abstract void recordMatch(double slopFactor, int position);

  /** Return the matching frequency of the last {@link #beginDoc} document. */
  public abstract int docMatchFreq();

  /** Return the score of the last {@link #beginDoc} document. */
  public abstract double docScore() throws IOException;
}
