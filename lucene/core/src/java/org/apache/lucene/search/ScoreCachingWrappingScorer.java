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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link Scorer} which wraps another scorer and caches the score of the
 * current document. Successive calls to {@link #score()} will return the same
 * result and will not invoke the wrapped Scorer's score() method, unless the
 * current document has changed.<br>
 * This class might be useful due to the changes done to the {@link Collector}
 * interface, in which the score is not computed for a document by default, only
 * if the collector requests it. Some collectors may need to use the score in
 * several places, however all they have in hand is a {@link Scorer} object, and
 * might end up computing the score of a document more than once.
 */
public final class ScoreCachingWrappingScorer extends Scorable {

  private int curDoc = -1;
  private float curScore;
  private final Scorable in;

  /**
   * Wraps the provided {@link Scorable} unless it's already an instance of
   * {@code ScoreCachingWrappingScorer}, in which case it will just return the provided instance.
   * @param scorer Underlying {@code Scorable} to wrap
   * @return Instance of {@code ScoreCachingWrappingScorer} wrapping the underlying {@code scorer}
   */
  public static Scorable wrap(Scorable scorer) {
    if (scorer instanceof ScoreCachingWrappingScorer) {
      return scorer;
    }
    return new ScoreCachingWrappingScorer(scorer);
  }

  /**
   * Creates a new instance by wrapping the given scorer.
   * @deprecated Use {@link ScoreCachingWrappingScorer#wrap(Scorable)} instead
   */
  @Deprecated
  public ScoreCachingWrappingScorer(Scorable scorer) {
    this.in = scorer;
  }

  @Override
  public float score() throws IOException {
    int doc = in.docID();
    if (doc != curDoc) {
      curScore = in.score();
      curDoc = doc;
    }

    return curScore;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    in.setMinCompetitiveScore(minScore);
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public Collection<ChildScorable> getChildren() {
    return Collections.singleton(new ChildScorable(in, "CACHED"));
  }
}
