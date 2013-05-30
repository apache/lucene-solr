package org.apache.lucene.search;

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
public class ScoreCachingWrappingScorer extends Scorer {

  private final Scorer scorer;
  private int curDoc = -1;
  private float curScore;
  
  /** Creates a new instance by wrapping the given scorer. */
  public ScoreCachingWrappingScorer(Scorer scorer) {
    super(scorer.weight);
    this.scorer = scorer;
  }

  @Override
  public boolean score(Collector collector, int max, int firstDocID) throws IOException {
    return scorer.score(collector, max, firstDocID);
  }
  
  @Override
  public float score() throws IOException {
    int doc = scorer.docID();
    if (doc != curDoc) {
      curScore = scorer.score();
      curDoc = doc;
    }
    
    return curScore;
  }

  @Override
  public int freq() throws IOException {
    return scorer.freq();
  }

  @Override
  public int docID() {
    return scorer.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return scorer.nextDoc();
  }
  
  @Override
  public void score(Collector collector) throws IOException {
    scorer.score(collector);
  }
  
  @Override
  public int advance(int target) throws IOException {
    return scorer.advance(target);
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    return Collections.singleton(new ChildScorer(scorer, "CACHED"));
  }

  @Override
  public long cost() {
    return scorer.cost();
  }
}
