package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;

/** Scorer for conjunctions, sets of queries, all of which are required. */
class ConjunctionScorer extends Scorer {
  private LinkedList scorers = new LinkedList();
  private boolean firstTime = true;
  private boolean more = true;
  private float coord;

  public ConjunctionScorer(Similarity similarity) {
    super(similarity);
  }

  final void add(Scorer scorer) {
    scorers.addLast(scorer);
  }

  private Scorer first() { return (Scorer)scorers.getFirst(); }
  private Scorer last() { return (Scorer)scorers.getLast(); }

  public int doc() { return first().doc(); }

  public boolean next() throws IOException {
    if (firstTime) {
      init(true);
    } else if (more) {
      more = last().next();                       // trigger further scanning
    }
    return doNext();
  }
  
  private boolean doNext() throws IOException {
    while (more && first().doc() < last().doc()) { // find doc w/ all clauses
      more = first().skipTo(last().doc());      // skip first upto last
      scorers.addLast(scorers.removeFirst());   // move first to last
    }
    return more;                                // found a doc with all clauses
  }

  public boolean skipTo(int target) throws IOException {
    if(firstTime) {
      init(false);
    }
    
    Iterator i = scorers.iterator();
    while (more && i.hasNext()) {
      more = ((Scorer)i.next()).skipTo(target);
    }
    
    if (more)
      sortScorers();                              // re-sort scorers
    
    return doNext();
  }

  public float score() throws IOException {
    float score = 0.0f;                           // sum scores
    Iterator i = scorers.iterator();
    while (i.hasNext())
      score += ((Scorer)i.next()).score();
    score *= coord;
    return score;
  }
  
  private void init(boolean initScorers) throws IOException {
    //  compute coord factor
    coord = getSimilarity().coord(scorers.size(), scorers.size());
   
    more = scorers.size() > 0;

    if(initScorers){
      // move each scorer to its first entry
      Iterator i = scorers.iterator();
      while (more && i.hasNext()) {
        more = ((Scorer)i.next()).next();
      }
      if (more)
        sortScorers(); // initial sort of list
    }

    firstTime = false;
  }

  private void sortScorers() {
    // move scorers to an array
    Scorer[] array = (Scorer[])scorers.toArray(new Scorer[scorers.size()]);
    scorers.clear();                              // empty the list

    // note that this comparator is not consistent with equals!
    Arrays.sort(array, new Comparator() {         // sort the array
        public int compare(Object o1, Object o2) {
          return ((Scorer)o1).doc() - ((Scorer)o2).doc();
        }
      });
    
    for (int i = 0; i < array.length; i++) {
      scorers.addLast(array[i]);                  // re-build list, now sorted
    }
  }

  public Explanation explain(int doc) {
    throw new UnsupportedOperationException();
  }

}
