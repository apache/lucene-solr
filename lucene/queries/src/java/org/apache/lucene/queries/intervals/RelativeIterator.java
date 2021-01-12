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

package org.apache.lucene.queries.intervals;

import java.io.IOException;

abstract class RelativeIterator extends IntervalIterator {

  final IntervalIterator a;
  final IntervalIterator b;

  boolean bpos;

  RelativeIterator(IntervalIterator a, IntervalIterator b) {
    this.a = a;
    this.b = b;
  }

  @Override
  public int docID() {
    return a.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    int doc = a.nextDoc();
    reset();
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    int doc = a.advance(target);
    reset();
    return doc;
  }

  @Override
  public long cost() {
    return a.cost();
  }

  protected void reset() throws IOException {
    int doc = a.docID();
    bpos = b.docID() == doc || (b.docID() < doc && b.advance(doc) == doc);
  }

  @Override
  public int start() {
    return a.start();
  }

  @Override
  public int end() {
    return a.end();
  }

  @Override
  public int gaps() {
    return a.gaps();
  }

  @Override
  public float matchCost() {
    return a.matchCost() + b.matchCost();
  }
}
