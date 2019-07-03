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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.PriorityQueue;

final class ExactPhraseMatcher extends PhraseMatcher {

  private static class PostingsAndPosition {
    private final PostingsEnum postings;
    private final int offset;
    private int freq, upTo, pos;

    public PostingsAndPosition(PostingsEnum postings, int offset) {
      this.postings = postings;
      this.offset = offset;
    }
  }

  private final PostingsAndPosition[] postings;
  private final DocIdSetIterator approximation;
  private final ImpactsDISI impactsApproximation;

  ExactPhraseMatcher(PhraseQuery.PostingsAndFreq[] postings, ScoreMode scoreMode, SimScorer scorer, float matchCost) {
    super(matchCost);

    final DocIdSetIterator approximation = ConjunctionDISI.intersectIterators(Arrays.stream(postings).map(p -> p.postings).collect(Collectors.toList()));
    final ImpactsSource impactsSource = mergeImpacts(Arrays.stream(postings).map(p -> p.impacts).toArray(ImpactsEnum[]::new));

    if (scoreMode == ScoreMode.TOP_SCORES) {
      this.approximation = this.impactsApproximation = new ImpactsDISI(approximation, impactsSource, scorer);
    } else {
      this.approximation = approximation;
      this.impactsApproximation = new ImpactsDISI(approximation, impactsSource, scorer);
    }

    List<PostingsAndPosition> postingsAndPositions = new ArrayList<>();
    for(PhraseQuery.PostingsAndFreq posting : postings) {
      postingsAndPositions.add(new PostingsAndPosition(posting.postings, posting.position));
    }
    this.postings = postingsAndPositions.toArray(new PostingsAndPosition[postingsAndPositions.size()]);
  }

  @Override
  DocIdSetIterator approximation() {
    return approximation;
  }

  @Override
  ImpactsDISI impactsApproximation() {
    return impactsApproximation;
  }

  @Override
  float maxFreq() {
    int minFreq = postings[0].freq;
    for (int i = 1; i < postings.length; i++) {
      minFreq = Math.min(minFreq, postings[i].freq);
    }
    return minFreq;
  }

  /** Advance the given pos enum to the first doc on or after {@code target}.
   *  Return {@code false} if the enum was exhausted before reaching
   *  {@code target} and {@code true} otherwise. */
  private static boolean advancePosition(PostingsAndPosition posting, int target) throws IOException {
    while (posting.pos < target) {
      if (posting.upTo == posting.freq) {
        return false;
      } else {
        posting.pos = posting.postings.nextPosition();
        posting.upTo += 1;
      }
    }
    return true;
  }

  @Override
  public void reset() throws IOException {
    for (PostingsAndPosition posting : postings) {
      posting.freq = posting.postings.freq();
      posting.pos = -1;
      posting.upTo = 0;
    }
  }

  @Override
  public boolean nextMatch() throws IOException {
    final PostingsAndPosition lead = postings[0];
    if (lead.upTo < lead.freq) {
      lead.pos = lead.postings.nextPosition();
      lead.upTo += 1;
    }
    else {
      return false;
    }
    advanceHead:
    while (true) {
      final int phrasePos = lead.pos - lead.offset;
      for (int j = 1; j < postings.length; ++j) {
        final PostingsAndPosition posting = postings[j];
        final int expectedPos = phrasePos + posting.offset;

        // advance up to the same position as the lead
        if (advancePosition(posting, expectedPos) == false) {
          break advanceHead;
        }

        if (posting.pos != expectedPos) { // we advanced too far
          if (advancePosition(lead, posting.pos - posting.offset + lead.offset)) {
            continue advanceHead;
          } else {
            break advanceHead;
          }
        }
      }
      return true;
    }
    return false;
  }

  @Override
  float sloppyWeight() {
    return 1;
  }

  @Override
  public int startPosition() {
    return postings[0].pos;
  }

  @Override
  public int endPosition() {
    return postings[postings.length - 1].pos;
  }

  @Override
  public int startOffset() throws IOException {
    return postings[0].postings.startOffset();
  }

  @Override
  public int endOffset() throws IOException {
    return postings[postings.length - 1].postings.endOffset();
  }

  /**
   * Merge impacts for multiple terms of an exact phrase.
   */
  static ImpactsSource mergeImpacts(ImpactsEnum[] impactsEnums) {
    // Iteration of block boundaries uses the impacts enum with the lower cost.
    // This is consistent with BlockMaxConjunctionScorer.
    int tmpLeadIndex = -1;
    for (int i = 0; i < impactsEnums.length; ++i) {
      if (tmpLeadIndex == -1 || impactsEnums[i].cost() < impactsEnums[tmpLeadIndex].cost()) {
        tmpLeadIndex = i;
      }
    }
    final int leadIndex = tmpLeadIndex;

    return new ImpactsSource() {

      class SubIterator {
        final Iterator<Impact> iterator;
        Impact current;

        SubIterator(List<Impact> impacts) {
          this.iterator = impacts.iterator();
          this.current = iterator.next();
        }

        boolean next() {
          if (iterator.hasNext() == false) {
            current = null;
            return false;
          } else {
            current = iterator.next();
            return true;
          }
        }
      }

      @Override
      public Impacts getImpacts() throws IOException {
        final Impacts[] impacts = new Impacts[impactsEnums.length];
        for (int i = 0; i < impactsEnums.length; ++i) {
          impacts[i] = impactsEnums[i].getImpacts();
        }
        final Impacts lead = impacts[leadIndex];
        return new Impacts() {

          @Override
          public int numLevels() {
            // Delegate to the lead
            return lead.numLevels();
          }

          @Override
          public int getDocIdUpTo(int level) {
            // Delegate to the lead
            return lead.getDocIdUpTo(level);
          }

          /**
           * Return the minimum level whose impacts are valid up to {@code docIdUpTo},
           * or {@code -1} if there is no such level.
           */
          private int getLevel(Impacts impacts, int docIdUpTo) {
            for (int level = 0, numLevels = impacts.numLevels(); level < numLevels; ++level) {
              if (impacts.getDocIdUpTo(level) >= docIdUpTo) {
                return level;
              }
            }
            return -1;
          }

          @Override
          public List<Impact> getImpacts(int level) {
            final int docIdUpTo = getDocIdUpTo(level);

            PriorityQueue<SubIterator> pq = new PriorityQueue<SubIterator>(impacts.length) {
              @Override
              protected boolean lessThan(SubIterator a, SubIterator b) {
                return a.current.freq < b.current.freq;
              }
            };

            boolean hasImpacts = false;
            List<Impact> onlyImpactList = null;
            for (int i = 0; i < impacts.length; ++i) {
              int impactsLevel = getLevel(impacts[i], docIdUpTo);
              if (impactsLevel == -1) {
                // This instance doesn't have useful impacts, ignore it: this is safe.
                continue;
              }

              List<Impact> impactList = impacts[i].getImpacts(impactsLevel);
              Impact firstImpact = impactList.get(0);
              if (firstImpact.freq == Integer.MAX_VALUE && firstImpact.norm == 1L) {
                // Dummy impacts, ignore it too.
                continue;
              }

              SubIterator subIterator = new SubIterator(impactList);
              pq.add(subIterator);
              if (hasImpacts == false) {
                hasImpacts = true;
                onlyImpactList = impactList;
              } else {
                onlyImpactList = null; // there are multiple impacts
              }
            }

            if (hasImpacts == false) {
              return Collections.singletonList(new Impact(Integer.MAX_VALUE, 1L));
            } else if (onlyImpactList != null) {
              return onlyImpactList;
            }

            // Idea: merge impacts by freq. The tricky thing is that we need to
            // consider freq values that are not in the impacts too. For
            // instance if the list of impacts is [{freq=2,norm=10}, {freq=4,norm=12}],
            // there might well be a document that has a freq of 2 and a length of 11,
            // which was just not added to the list of impacts because {freq=2,norm=10}
            // is more competitive.
            // We walk impacts in parallel through a PQ ordered by freq. At any time,
            // the competitive impact consists of the lowest freq among all entries of
            // the PQ (the top) and the highest norm (tracked separately).
            List<Impact> mergedImpacts = new ArrayList<>();
            SubIterator top = pq.top();
            int currentFreq = top.current.freq;
            long currentNorm = 0;
            for (SubIterator it : pq) {
              if (Long.compareUnsigned(it.current.norm, currentNorm) > 0) {
                currentNorm = it.current.norm;
              }
            }

            outer: while (true) {
              if (mergedImpacts.size() > 0 && mergedImpacts.get(mergedImpacts.size() - 1).norm == currentNorm) {
                mergedImpacts.get(mergedImpacts.size() - 1).freq = currentFreq;
              } else {
                mergedImpacts.add(new Impact(currentFreq, currentNorm));
              }

              do {
                if (top.next() == false) {
                  // At least one clause doesn't have any more documents below the current norm,
                  // so we can safely ignore further clauses. The only reason why they have more
                  // impacts is because they cover more documents that we are not interested in.
                  break outer;
                }
                if (Long.compareUnsigned(top.current.norm, currentNorm) > 0) {
                  currentNorm = top.current.norm;
                }
                top = pq.updateTop();
              } while (top.current.freq == currentFreq);

              currentFreq = top.current.freq;
            }

            return mergedImpacts;
          }
        };
      }

      @Override
      public void advanceShallow(int target) throws IOException {
        for (ImpactsEnum impactsEnum : impactsEnums) {
          impactsEnum.advanceShallow(target);
        }
      }
    };
  }

}
