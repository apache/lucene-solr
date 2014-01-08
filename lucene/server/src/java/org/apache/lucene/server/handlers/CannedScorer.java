package org.apache.lucene.server.handlers;

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

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

// TODO: somehow share w/ the many copies of this class in
// Lucene ... but I don't want to lend the thing any
// credibility!!
final class CannedScorer extends Scorer {

  float score;
  int doc;

  public CannedScorer(int doc, float score) {
    super((Weight) null);
    this.doc = doc;
    this.score = score;
  }

  @Override
  public float score() {
    return score;
  }
    
  @Override
  public int freq() {
    throw new UnsupportedOperationException(); // TODO: wtf does this class do?
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int advance(int target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int nextDoc() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long cost() {
    return 1;
  }
}
