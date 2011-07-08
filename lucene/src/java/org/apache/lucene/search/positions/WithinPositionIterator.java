package org.apache.lucene.search.positions;

import java.io.IOException;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionIntervalFilter;


public class WithinPositionIterator extends PositionIntervalIterator implements PositionIntervalFilter {
  private int howMany;
  private PositionIntervalIterator iterator;
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
    PositionInterval interval = null;
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

}
