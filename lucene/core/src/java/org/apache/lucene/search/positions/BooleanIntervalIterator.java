package org.apache.lucene.search.positions;

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

import com.sun.tools.javac.resources.compiler;

import java.io.IOException;
import java.util.List;

/**
 * 
 * @lucene.experimental
 */
// nocommit - javadoc
public abstract class BooleanIntervalIterator extends IntervalIterator {

  protected final IntervalIterator[] iterators;
  protected final IntervalQueue queue;

  protected BooleanIntervalIterator(Scorer scorer, IntervalIterator[] iterators,
      IntervalQueue queue, boolean collectPositions) throws IOException {
    super(scorer, collectPositions);
    this.queue = queue;
    this.iterators = iterators;
  }

  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return iterators;
  }

  abstract void advance() throws IOException;
  
  public static IntervalIterator[] pullIterators(boolean collectPositions, Scorer... scorers)
      throws IOException {
    IntervalIterator[] iterators = new IntervalIterator[scorers.length];
    for (int i = 0; i < scorers.length; i++) {
      iterators[i] = scorers[i].positions(collectPositions);
    }
    return iterators;
  }
  
  public static IntervalIterator[] pullIterators(boolean collectPositions,List<Scorer> scorers)
      throws IOException {
    IntervalIterator[] iterators = new IntervalIterator[scorers.size()];
    for (int i = 0; i < iterators.length; i++) {
      iterators[i] = scorers.get(i).positions(collectPositions);
    }
    return iterators;
  }
}