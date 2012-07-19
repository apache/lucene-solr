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

import org.apache.lucene.search.positions.IntervalIterator.IntervalFilter;


/**
 * @lucene.experimental
 */ // nocommit - javadoc
public class WithinIntervalIterator extends IntervalIterator implements IntervalFilter {
  private int howMany;
  private IntervalIterator iterator;
  private Interval interval;
  
  public WithinIntervalIterator(int howMany, IntervalIterator iterator) {
    super(iterator != null ? iterator.scorer : null, iterator != null ? iterator.collectPositions : false);
    this.howMany = howMany;
    this.iterator = iterator;
  }
  
  public WithinIntervalIterator(int howMany) {
    this(howMany, null); // use this instance as a filter template
  }
  @Override
  public Interval next() throws IOException {
    while ((interval = iterator.next()) != null) {
      if((iterator.matchDistance()) <= howMany){
        return interval;
      }
    }
    return null;
  }

  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return new IntervalIterator[] {iterator};
  }

  public IntervalIterator filter(IntervalIterator iter) {
    return new WithinIntervalIterator(howMany, iter);
  }

  @Override
  public void collect(IntervalCollector collector) {
    assert collectPositions;
    collector.collectComposite(null, interval, iterator.docID());
    iterator.collect(collector);
  }

  @Override
  public int advanceTo(int docId) throws IOException {
    return currentDoc = iterator.advanceTo(docId);
  }

  @Override
  public int matchDistance() {
    return iterator.matchDistance();
  }

}
