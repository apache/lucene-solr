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

import org.apache.lucene.index.TermPositions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

final class SloppyPhraseScorer extends PhraseScorer {
    private int slop;
    private PhrasePositions repeats[];
    private boolean checkedRepeats;

    SloppyPhraseScorer(Weight weight, TermPositions[] tps, int[] offsets, Similarity similarity,
                       int slop, byte[] norms) {
        super(weight, tps, offsets, similarity, norms);
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
     * <br>Pssibly not all valid combinations are encountered, because for efficiency  
     * we always propagate the least PhrasePosition. This allows to base on 
     * PriorityQueue and move forward faster. 
     * As result, for example, document "a b c b a"
     * would score differently for queries "a b c"~4 and "c b a"~4, although 
     * they really are equivalent. 
     * Similarly, for doc "a b c b a f g", query "c b"~2 
     * would get same score as "g f"~2, although "c b"~2 could be matched twice.
     * We may want to fix this in the future (currently not, for performance reasons).
     */
    protected final float phraseFreq() throws IOException {
        int end = initPhrasePositions();
        
        float freq = 0.0f;
        boolean done = (end<0);
        while (!done) {
            PhrasePositions pp = (PhrasePositions) pq.pop();
            int start = pp.position;
            int next = ((PhrasePositions) pq.top()).position;

            boolean tpsDiffer = true;
            for (int pos = start; pos <= next || !tpsDiffer; pos = pp.position) {
                if (pos<=next && tpsDiffer)
                    start = pos;				  // advance pp to min window
                if (!pp.nextPosition()) {
                    done = true;          // ran out of a term -- done
                    break;
                }
                tpsDiffer = !pp.repeats || termPositionsDiffer(pp);
            }

            int matchLength = end - start;
            if (matchLength <= slop)
                freq += getSimilarity().sloppyFreq(matchLength); // score match

            if (pp.position > end)
                end = pp.position;
            pq.put(pp);				  // restore pq
        }

        return freq;
    }
    
    
    /**
     * Init PhrasePositions in place.
     * There is a one time initializatin for this scorer:
     * <br>- Put in repeats[] each pp that has another pp with same position in the doc.
     * <br>- Also mark each such pp by pp.repeats = true.
     * <br>Later can consult with repeats[] in termPositionsDiffer(pp), making that check efficient.
     * In particular, this allows to score queries with no repetiotions with no overhead due to this computation.
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
                pq.put(pp);         // build pq from list
            }
            return end;
        }
        
        // position the pp's
        for (PhrasePositions pp = first; pp != null; pp = pp.next)
            pp.firstPosition();
        
        // one time initializatin for this scorer
        if (!checkedRepeats) {
            checkedRepeats = true;
            // check for repeats
            HashMap m = null;
            for (PhrasePositions pp = first; pp != null; pp = pp.next) {
                int tpPos = pp.position + pp.offset;
                for (PhrasePositions pp2 = pp.next; pp2 != null; pp2 = pp2.next) {
                    int tpPos2 = pp2.position + pp2.offset;
                    if (tpPos2 == tpPos) { 
                        if (m == null)
                            m = new HashMap();
                        pp.repeats = true;
                        pp2.repeats = true;
                        m.put(pp,null);
                        m.put(pp2,null);
                    }
                }
            }
            if (m!=null)
                repeats = (PhrasePositions[]) m.keySet().toArray(new PhrasePositions[0]);
        }
        
        // with repeats must advance some repeating pp's so they all start with differing tp's       
        if (repeats!=null) {
            // must propagate higher offsets first (otherwise might miss matches).
            Arrays.sort(repeats,  new Comparator() {
                public int compare(Object x, Object y) {
                    return ((PhrasePositions) y).offset - ((PhrasePositions) x).offset;
                }});
            // now advance them
            for (int i = 0; i < repeats.length; i++) {
                PhrasePositions pp = repeats[i];
                while (!termPositionsDiffer(pp)) {
                  if (!pp.nextPosition())
                      return -1;    // ran out of a term -- done  
                } 
            }
        }
      
        // build queue from list
        pq.clear();
        for (PhrasePositions pp = first; pp != null; pp = pp.next) {
            if (pp.position > end)
                end = pp.position;
            pq.put(pp);         // build pq from list
        }

        return end;
    }

    // disalow two pp's to have the same tp position, so that same word twice 
    // in query would go elswhere in the matched doc
    private boolean termPositionsDiffer(PhrasePositions pp) {
        // efficiency note: a more efficient implemention could keep a map between repeating 
        // pp's, so that if pp1a, pp1b, pp1c are repeats term1, and pp2a, pp2b are repeats 
        // of term2, pp2a would only be checked against pp2b but not against pp1a, pp1b, pp1c. 
        // However this would complicate code, for a rather rare case, so choice is to compromise here.
        int tpPos = pp.position + pp.offset;
        for (int i = 0; i < repeats.length; i++) {
            PhrasePositions pp2 = repeats[i];
            if (pp2 == pp)
                continue;
            int tpPos2 = pp2.position + pp2.offset;
            if (tpPos2 == tpPos)
                return false;
        }
        return true;
    }
}
