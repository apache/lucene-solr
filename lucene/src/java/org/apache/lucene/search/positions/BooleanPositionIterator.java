package org.apache.lucene.search.positions;

import java.io.IOException;

import org.apache.lucene.search.Scorer;

@SuppressWarnings("serial")
abstract class BooleanPositionIterator extends PositionIntervalIterator {

  protected int docId = -1;
  protected final PositionIntervalIterator[] iterators;
  protected final IntervalQueue queue;

  public BooleanPositionIterator(Scorer scorer, Scorer[] subScorers, IntervalQueue queue) throws IOException {
    super(scorer);
    this.queue = queue;
    iterators = new PositionIntervalIterator[subScorers.length];
    for (int i = 0; i < subScorers.length; i++) {
      iterators[i] = subScorers[i].positions();
    }
  }

  @Override
  public PositionIntervalIterator[] subs(boolean inOrder) {
    return iterators;
  }
  
  abstract void advance() throws IOException;

}