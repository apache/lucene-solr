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
import java.io.IOException;

import org.apache.lucene.search.Scorer;



/**
 * An interval iterator that has the semantics of sloppy phrase query.
 */
public class MaxLengthPositionIntervalIterator extends PositionIntervalIterator {
  
  private final int maxLen;
  private ConjunctionPositionIterator iter;

  public MaxLengthPositionIntervalIterator(Scorer scorer, int maxLength,
      ConjunctionPositionIterator iter) throws IOException {
    super(scorer, iter.collectPositions);
    this.maxLen = maxLength;
    this.iter = iter;
  }
  
  @Override
  public int advanceTo(int docId) throws IOException {
    return iter.advanceTo(docId);
  }
  
  @Override
  public PositionInterval next() throws IOException {
    PositionInterval current;
    do {
      current = iter.next();
      if (current == null) {
        break;
      }
      //NOCOMMIT this is an impl detail of ConjuIter that shoudl reside somewhere else
      // maybe specialize for this?
      if (iter.matchDistance() <= maxLen) {
        break;
      }
    } while(true);
    return current;
  }
  
  @Override
  public void collect(PositionCollector collector) {
    assert collectPositions;
    iter.collect(collector);
  }
  
  @Override
  public PositionIntervalIterator[] subs(boolean inOrder) {
    return iter.subs(inOrder);
  }
  
}
