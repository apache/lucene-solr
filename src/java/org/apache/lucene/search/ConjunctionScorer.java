package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2004 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import java.util.*;
import org.apache.lucene.index.*;

/** Scorer for conjunctions, sets of queries, all of which are required. */
final class ConjunctionScorer extends Scorer {
  private LinkedList scorers = new LinkedList();
  private boolean firstTime = true;
  private boolean more = true;
  private float coord;

  public ConjunctionScorer(Similarity similarity) {
    super(similarity);
  }

  final void add(Scorer scorer) throws IOException {
    scorers.addLast(scorer);
  }

  private Scorer first() { return (Scorer)scorers.getFirst(); }
  private Scorer last() { return (Scorer)scorers.getLast(); }

  public int doc() { return first().doc(); }

  public boolean next() throws IOException {
    if (firstTime) {
      init();
    } else if (more) {
      more = last().next();                       // trigger further scanning
    }

    while (more && first().doc() < last().doc()) { // find doc w/ all clauses
      more = first().skipTo(last().doc());      // skip first upto last
      scorers.addLast(scorers.removeFirst());   // move first to last
    }
    
    return more;                                // found a doc with all clauses
  }

  public boolean skipTo(int target) throws IOException {
    Iterator i = scorers.iterator();
    while (more && i.hasNext()) {
      more = ((Scorer)i.next()).skipTo(target);
    }
    if (more)
      sortScorers();                              // re-sort scorers
    return more;
  }

  public float score() throws IOException {
    float score = 0.0f;                           // sum scores
    Iterator i = scorers.iterator();
    while (i.hasNext())
      score += ((Scorer)i.next()).score();
    score *= coord;
    return score;
  }

  private void init() throws IOException {
    more = scorers.size() > 0;

    // compute coord factor
    coord = getSimilarity().coord(scorers.size(), scorers.size());

    // move each scorer to its first entry
    Iterator i = scorers.iterator();
    while (more && i.hasNext()) {
      more = ((Scorer)i.next()).next();
    }
    if (more)
      sortScorers();                              // initial sort of list

    firstTime = false;
  }

  private void sortScorers() throws IOException {
    // move scorers to an array
    Scorer[] array = (Scorer[])scorers.toArray(new Scorer[scorers.size()]);
    scorers.clear();                              // empty the list

    Arrays.sort(array, new Comparator() {         // sort the array
        public int compare(Object o1, Object o2) {
          return ((Scorer)o1).doc() - ((Scorer)o2).doc();
        }
        public boolean equals(Object o1, Object o2) {
          return ((Scorer)o1).doc() == ((Scorer)o2).doc();
        }
      });
    
    for (int i = 0; i < array.length; i++) {
      scorers.addLast(array[i]);                  // re-build list, now sorted
    }
  }

  public Explanation explain(int doc) throws IOException {
    throw new UnsupportedOperationException();
  }

}
