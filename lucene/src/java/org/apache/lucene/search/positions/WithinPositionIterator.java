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

import org.apache.lucene.search.positions.PositionIntervalIterator.PositionIntervalFilter;


/**
 * @lucene.experimental
 */ // nocommit - javadoc
public class WithinPositionIterator extends PositionIntervalIterator implements PositionIntervalFilter {
  private int howMany;
  private PositionIntervalIterator iterator;
  private PositionInterval interval;
  
  public WithinPositionIterator(int howMany, PositionIntervalIterator iterator) {
    super(iterator != null ? iterator.scorer : null);
    this.howMany = howMany;
    this.iterator = iterator;
  }
  
  public WithinPositionIterator(int howMany) {
    this(howMany, null); // use this instance as a filter template
  }
  @Override
  public PositionInterval next() throws IOException {
    while ((interval = iterator.next()) != null) {
      if((interval.end - interval.begin) <= howMany){
        return interval;
      }
    }
    return null;
  }

  @Override
  public PositionIntervalIterator[] subs(boolean inOrder) {
    return new PositionIntervalIterator[] {iterator};
  }

  public PositionIntervalIterator filter(PositionIntervalIterator iter) {
    return new WithinPositionIterator(howMany, iter);
  }

  @Override
  public void collect() {
    collector.collectComposite(null, interval, iterator.docID());
    iterator.collect();
  }

  @Override
  public int advanceTo(int docId) throws IOException {
    return currentDoc = iterator.advanceTo(docId);
  }

}
