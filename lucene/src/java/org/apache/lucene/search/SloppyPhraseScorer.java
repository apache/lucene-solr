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
import java.util.LinkedHashSet;

final class SloppyPhraseScorer extends PhraseScorer {
    private int slop;
    private PhrasePositions repeats[];
    private PhrasePositions tmpPos[]; // for flipping repeating pps.
    private boolean checkedRepeats;
    
    SloppyPhraseScorer(Weight weight, PhraseQuery.PostingsAndFreq[] postings,
                       int slop, Similarity.SloppyDocScorer docScorer) throws IOException {
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
        
        float freq = 0.0f;
        boolean done = (end<0);
        while (!done) {
            PhrasePositions pp = pq.pop();
            int start = pp.position;
            int next = pq.top().position;

            boolean tpsDiffer = true;
            for (int pos = start; pos <= next || !tpsDiffer; pos = pp.position) {
                if (pos<=next && tpsDiffer)
                    start = pos;                  // advance pp to min window
                if (!pp.nextPosition()) {
                    done = true;          // ran out of a term -- done
                    break;
                }
                PhrasePositions pp2 = null;
                tpsDiffer = !pp.repeats || (pp2 = termPositionsConflict(pp))==null;
                if (pp2!=null && pp2!=pp) {
                  pp = flip(pp,pp2); // flip pp to pp2
                }
            }

            int matchLength = end - start;
            if (matchLength <= slop)
                freq += docScorer.computeSlopFactor(matchLength); // score match

            if (pp.position > end)
                end = pp.position;
            pq.add(pp);               // restore pq
        }

        return freq;
    }
    
    // flip pp2 and pp in the queue: pop until finding pp2, insert back all but pp2, insert pp back.
    // assumes: pp!=pp2, pp2 in pq, pp not in pq.
    // called only when there are repeating pps.
    private PhrasePositions flip(PhrasePositions pp, PhrasePositions pp2) {
      int n=0;
      PhrasePositions pp3;
      //pop until finding pp2
      while ((pp3=pq.pop()) != pp2) {
        tmpPos[n++] = pp3;
      }
      //insert back all but pp2
      for (n--; n>=0; n--) {
        pq.insertWithOverflow(tmpPos[n]);
      }
      //insert pp back
      pq.add(pp);
      return pp2;
    }

    /**
     * Init PhrasePositions in place.
     * There is a one time initialization for this scorer (taking place at the first doc that matches all terms):
     * <br>- Put in repeats[] each pp that has another pp with same position in the doc.
     *       This relies on that the position in PP is computed as (TP.position - offset) and 
     *       so by adding offset we actually compare positions and identify that the two are 
     *       the same term.
     *       An exclusion to this is two distinct terms in the same offset in query and same 
     *       position in doc. This case is detected by comparing just the (query) offsets, 
     *       and two such PPs are not considered "repeating". 
     * <br>- Also mark each such pp by pp.repeats = true.
     * <br>Later can consult with repeats[] in termPositionsConflict(pp), making that check efficient.
     * In particular, this allows to score queries with no repetitions with no overhead due to this computation.
     * <br>- Example 1 - query with no repetitions: "ho my"~2
     * <br>- Example 2 - query with repetitions: "ho my my"~2
     * <br>- Example 3 - query with repetitions: "my ho my"~2
     * <br>Init per doc w/repeats in query, includes propagating some repeating pp's to avoid false phrase detection.  
     * @return end (max position), or -1 if any term ran out (i.e. done) 
     * @throws IOException 
     */
    private int initPhrasePositions() throws IOException {
        int end = 0;
        
        // no repeats at all (most common case is also the simplest one)
        if (checkedRepeats && repeats==null) {
            // build queue from list
            pq.clear();
            for (PhrasePositions pp = first; pp != null; pp = pp.next) {
                pp.firstPosition();
                if (pp.position > end)
                    end = pp.position;
                pq.add(pp);         // build pq from list
            }
            return end;
        }
        
        // position the pp's
        for (PhrasePositions pp = first; pp != null; pp = pp.next)
            pp.firstPosition();
        
        // one time initialization for this scorer
        if (!checkedRepeats) {
            checkedRepeats = true;
            // check for repeats
            LinkedHashSet<PhrasePositions> m = null; // see comment (*) below why order is important
            for (PhrasePositions pp = first; pp != null; pp = pp.next) {
                int tpPos = pp.position + pp.offset;
                for (PhrasePositions pp2 = pp.next; pp2 != null; pp2 = pp2.next) {
                    if (pp.offset == pp2.offset) {
                      continue; // not a repetition: the two PPs are originally in same offset in the query! 
                    }
                    int tpPos2 = pp2.position + pp2.offset;
                    if (tpPos2 == tpPos) { 
                        if (m == null)
                            m = new LinkedHashSet<PhrasePositions>();
                        pp.repeats = true;
                        pp2.repeats = true;
                        m.add(pp);
                        m.add(pp2);
                    }
                }
            }
            if (m!=null)
                repeats = m.toArray(new PhrasePositions[0]);
        }
        
        // with repeats must advance some repeating pp's so they all start with differing tp's
        // (*) It is important that pps are handled by their original order in the query,
        // because we advance the pp with larger offset, and so processing them in that order
        // allows to cover all pairs.
        if (repeats!=null) {
            for (int i = 0; i < repeats.length; i++) {
                PhrasePositions pp = repeats[i];
                PhrasePositions pp2;
                while ((pp2 = termPositionsConflict(pp)) != null) {
                  if (!pp2.nextPosition()) // among pps that do not differ, advance the pp with higher offset
                      return -1;           // ran out of a term -- done
                } 
            }
        }
      
        // build queue from list
        pq.clear();
        for (PhrasePositions pp = first; pp != null; pp = pp.next) {
            if (pp.position > end)
                end = pp.position;
            pq.add(pp);         // build pq from list
        }

        if (repeats!=null) {
          tmpPos = new PhrasePositions[pq.size()];
        }
        
        return end;
    }

    /**
     * We disallow two pp's to have the same TermPosition, thereby verifying multiple occurrences 
     * in the query of the same word would go elsewhere in the matched doc.
     * @return null if differ (i.e. valid) otherwise return the higher offset PhrasePositions
     * out of the first two PPs found to not differ.
     */
    private PhrasePositions termPositionsConflict(PhrasePositions pp) {
        // efficiency note: a more efficient implementation could keep a map between repeating 
        // pp's, so that if pp1a, pp1b, pp1c are repeats of term1, and pp2a, pp2b are repeats 
        // of term2, pp2a would only be checked against pp2b but not against pp1a, pp1b, pp1c. 
        // However this would complicate code, for a rather rare case, so choice is to compromise here.
        int tpPos = pp.position + pp.offset;
        for (int i = 0; i < repeats.length; i++) {
            PhrasePositions pp2 = repeats[i];
            if (pp2 == pp) {
                continue;
            }
            if (pp.offset == pp2.offset) {
              continue; // not a repetition: the two PPs are originally in same offset in the query! 
            }
            int tpPos2 = pp2.position + pp2.offset;
            if (tpPos2 == tpPos) {
                return pp.offset > pp2.offset ? pp : pp2; // do not differ: return the one with higher offset.
            }
        }
        return null; 
    }
}
