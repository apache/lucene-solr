package org.apache.lucene.search;

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
import java.util.ArrayList;

import org.apache.lucene.search.similarities.Similarity;

final class SloppyPhraseScorer extends PhraseScorer {
  private int slop;
  private boolean checkedRepeats; // flag to only check in first candidate doc in case there are no repeats
  private boolean hasRepeats; // flag indicating that there are repeats (already checked in first candidate doc)
  private PhraseQueue pq; // for advancing min position
  private PhrasePositions[] nrPps; // non repeating pps ordered by their query offset
  
  SloppyPhraseScorer(Weight weight, PhraseQuery.PostingsAndFreq[] postings,
      int slop, Similarity.SloppyDocScorer docScorer) {
    super(weight, postings, docScorer);
    this.slop = slop;
  }
  
  /**
   * Score a candidate doc for all slop-valid position-combinations (matches) 
   * encountered while traversing/hopping the PhrasePositions.
   * <br> The score contribution of a match depends on the distance: 
   * <br> - highest score for distance=0 (exact match).
   * <br> - score gets lower as distance gets higher.
   * <br>Example: for query "a b"~2, a document "x a b a y" can be scored twice: 
   * once for "a b" (distance=0), and once for "b a" (distance=2).
   * <br>Possibly not all valid combinations are encountered, because for efficiency  
   * we always propagate the least PhrasePosition. This allows to base on 
   * PriorityQueue and move forward faster. 
   * As result, for example, document "a b c b a"
   * would score differently for queries "a b c"~4 and "c b a"~4, although 
   * they really are equivalent. 
   * Similarly, for doc "a b c b a f g", query "c b"~2 
   * would get same score as "g f"~2, although "c b"~2 could be matched twice.
   * We may want to fix this in the future (currently not, for performance reasons).
   */
  @Override
  protected float phraseFreq() throws IOException {
    int end = initPhrasePositions();
    //printPositions(System.err, "INIT DONE:");
    if (end==Integer.MIN_VALUE) {
      return 0.0f;
    }
    
    float freq = 0.0f;
    PhrasePositions pp = pq.pop();
    int matchLength = end - pp.position;
    int next = pq.size()>0 ? pq.top().position : pp.position;
    //printQueue(System.err, pp, "Bef Loop: next="+next+" mlen="+end+"-"+pp.position+"="+matchLength);
    while (pp.nextPosition() && (end=advanceRepeats(pp, end)) != Integer.MIN_VALUE)  {
      if (pp.position > next) {
        //printQueue(System.err, pp, "A: >next="+next+" matchLength="+matchLength);
        if (matchLength <= slop) {
          freq += docScorer.computeSlopFactor(matchLength); // score match
        }      
        pq.add(pp);
        pp = pq.pop();
        next = pq.size()>0 ? pq.top().position : pp.position;
        matchLength = end - pp.position;
        //printQueue(System.err, pp, "B: >next="+next+" matchLength="+matchLength);
      } else {
        int matchLength2 = end - pp.position;
        //printQueue(System.err, pp, "C: mlen2<mlen: next="+next+" matchLength="+matchLength+" matchLength2="+matchLength2);
        if (matchLength2 < matchLength) {
          matchLength = matchLength2;
        }
      }
    }
    if (matchLength <= slop) {
      freq += docScorer.computeSlopFactor(matchLength); // score match
    }    
    return freq;
  }

  /**
   * Advance repeating pps of an input (non-repeating) pp.
   * Return a modified 'end' in case pp or its repeats exceeds original 'end'.
   * "Dirty" trick: when there are repeats, modifies pp's position to that of 
   * least repeater of pp (needed when due to holes repeaters' positions are "back").
   */
  private int advanceRepeats(PhrasePositions pp, int end) throws IOException {
    int repeatsEnd = end;
    if (pp.position > repeatsEnd) {
      repeatsEnd = pp.position;
    }
    if (!hasRepeats) {
      return repeatsEnd;
    }
    int tpPos = tpPos(pp);
    for (PhrasePositions pp2=pp.nextRepeating; pp2!=null; pp2=pp2.nextRepeating) {
      while (tpPos(pp2) <= tpPos) {
        if (!pp2.nextPosition()) {
          return Integer.MIN_VALUE;
        }
      }
      tpPos = tpPos(pp2);
      if (pp2.position > repeatsEnd) {
        repeatsEnd = pp2.position;
      }
      // "dirty" trick: with holes, given a pp, its repeating pp2 might have smaller position.
      // so in order to have the right "start" in matchLength computation we fake pp.position.
      // this relies on pp.nextPosition() not using pp.position.
      if (pp2.position < pp.position) { 
        pp.position = pp2.position;     
      }
    }
    return repeatsEnd;
  }

  /**
   * Initialize PhrasePositions in place.
   * There is a one time initialization for this scorer (taking place at the first doc that matches all terms):
   * <ul>
   *  <li>Detect groups of repeating pps: those with same tpPos (tpPos==position in the doc) but different offsets in query.
   *  <li>For each such group:
   *  <ul>
   *   <li>form an inner linked list of the repeating ones.
   *   <li>propagate all group members but first so that they land on different tpPos().
   *  </ul>
   *  <li>Mark whether there are repetitions at all, so that scoring queries with no repetitions has no overhead due to this computation.
   *  <li>Insert to pq only non repeating PPs, or PPs that are the first in a repeating group.
   * </ul>
   * Examples:
   * <ol>
   *  <li>no repetitions: <b>"ho my"~2</b>
   *  <li>repetitions: <b>"ho my my"~2</b>
   *  <li>repetitions: <b>"my ho my"~2</b>
   * </ol>
   * @return end (max position), or Integer.MIN_VALUE if any term ran out (i.e. done) 
   */
  private int initPhrasePositions() throws IOException {
    int end = Integer.MIN_VALUE;
    
    // no repeats at all (most common case is also the simplest one)
    if (checkedRepeats && !hasRepeats) {
      // build queue from list
      pq.clear();
      for (PhrasePositions pp=min,prev=null; prev!=max; pp=(prev=pp).next) {  // iterate cyclic list: done once handled max
        pp.firstPosition();
        if (pp.position > end) {
          end = pp.position;
        }
        pq.add(pp);         // build pq from list
      }
      return end;
    }
    
    //printPositions(System.err, "Init: 1: Bef position");
    
    // position the pp's
    for (PhrasePositions pp=min,prev=null; prev!=max; pp=(prev=pp).next) {  // iterate cyclic list: done once handled max  
      pp.firstPosition();
    }
    
    //printPositions(System.err, "Init: 2: Aft position");
    
    // one time initialization for this scorer (done only for the first candidate doc)
    if (!checkedRepeats) {
      checkedRepeats = true;
      ArrayList<PhrasePositions> ppsA = new ArrayList<PhrasePositions>();
      PhrasePositions dummyPP = new PhrasePositions(null, -1, -1);
      // check for repeats
      for (PhrasePositions pp=min,prev=null; prev!=max; pp=(prev=pp).next) {  // iterate cyclic list: done once handled max
        if (pp.nextRepeating != null) {
          continue; // a repetition of an earlier pp
        }
        ppsA.add(pp);
        int tpPos = tpPos(pp);
        for (PhrasePositions prevB=pp, pp2=pp.next; pp2!= min; pp2=pp2.next) {
          if (
              pp2.nextRepeating != null  // already detected as a repetition of an earlier pp
              || pp.offset == pp2.offset // not a repetition: the two PPs are originally in same offset in the query! 
              || tpPos(pp2) != tpPos) {  // not a repetition
            continue; 
          }
          // a repetition
          hasRepeats = true;
          prevB.nextRepeating = pp2;  // add pp2 to the repeats linked list
          pp2.nextRepeating = dummyPP; // allows not to handle the last pp in a sub-list
          prevB = pp2;
        }
      }
      if (hasRepeats) {
        // clean dummy markers
        for (PhrasePositions pp=min,prev=null; prev!=max; pp=(prev=pp).next) {  // iterate cyclic list: done once handled max
          if (pp.nextRepeating == dummyPP) {
            pp.nextRepeating = null;
          }
        }
      }
      nrPps = ppsA.toArray(new PhrasePositions[0]);
      pq = new PhraseQueue(nrPps.length);
    }
    
    //printPositions(System.err, "Init: 3: Aft check-repeats");
    
    // with repeats must advance some repeating pp's so they all start with differing tp's
    if (hasRepeats) {
      for (PhrasePositions pp: nrPps) {
        if ((end=advanceRepeats(pp, end)) == Integer.MIN_VALUE) {
          return Integer.MIN_VALUE; // ran out of a term -- done (no valid matches in current doc)
        }
      }
    }
    
    //printPositions(System.err, "Init: 4: Aft advance-repeats");
    
    // build queue from non repeating pps 
    pq.clear();
    for (PhrasePositions pp: nrPps) {
      if (pp.position > end) {
        end = pp.position;
      }
      pq.add(pp);
    }
    
    return end;
  }
  
  /** Actual position in doc of a PhrasePosition, relies on that position = tpPos - offset) */
  private final int tpPos(PhrasePositions pp) {
    return pp.position + pp.offset;
  }
  
//  private void printPositions(PrintStream ps, String title) {
//    ps.println();
//    ps.println("---- "+title);
//    int k = 0;
//    if (nrPps!=null) {
//      for (PhrasePositions pp: nrPps) {
//        ps.println("  " + k++ + "  " + pp);
//      }
//    } else {
//      for (PhrasePositions pp=min; 0==k || pp!=min; pp = pp.next) {  
//        ps.println("  " + k++ + "  " + pp);
//      }
//    }
//  }

//  private void printQueue(PrintStream ps, PhrasePositions ext, String title) {
//    ps.println();
//    ps.println("---- "+title);
//    ps.println("EXT: "+ext);
//    PhrasePositions[] t = new PhrasePositions[pq.size()];
//    if (pq.size()>0) {
//      t[0] = pq.pop();
//      ps.println("  " + 0 + "  " + t[0]);
//      for (int i=1; i<t.length; i++) {
//        t[i] = pq.pop();
//        assert t[i-1].position <= t[i].position : "PQ is out of order: "+(i-1)+"::"+t[i-1]+" "+i+"::"+t[i];
//        ps.println("  " + i + "  " + t[i]);
//      }
//      // add them back
//      for (int i=t.length-1; i>=0; i--) {
//        pq.add(t[i]);
//      }
//    }
//  }
}
