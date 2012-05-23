package org.apache.lucene.search.positions;

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

import org.apache.lucene.search.Scorer;

/**
 * 
 * @lucene.experimental
 */
// nocommit - javadoc
abstract class BooleanPositionIterator extends PositionIntervalIterator {

  protected final PositionIntervalIterator[] iterators;
  protected final IntervalQueue queue;

  public BooleanPositionIterator(Scorer scorer, Scorer[] subScorers,
      IntervalQueue queue) throws IOException {
    super(scorer);
    this.queue = queue;
    iterators = new PositionIntervalIterator[subScorers.length];
    for (int i = 0; i < subScorers.length; i++) {
      iterators[i] = subScorers[i].positions();
    }
  }
  
  public BooleanPositionIterator(Scorer scorer, PositionIntervalIterator[] iterators,
      IntervalQueue queue) throws IOException {
    super(scorer);
    this.queue = queue;
    this.iterators = iterators;
  }

  @Override
  public PositionIntervalIterator[] subs(boolean inOrder) {
    return iterators;
  }

  abstract void advance() throws IOException;

}