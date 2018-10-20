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
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

/**
 * A query that treats multiple terms as synonyms.
 * <p>
 * For scoring purposes, this query tries to score the terms as if you
 * had indexed them as one term: it will match any of the terms but
 * only invoke the similarity a single time, scoring the sum of all
 * term frequencies for the document.
 */
public final class SynonymQuery extends Query {
  private final Term terms[];
  
  /**
   * Creates a new SynonymQuery, matching any of the supplied terms.
   * <p>
   * The terms must all have the same field.
   */
  public SynonymQuery(Term... terms) {
    this.terms = Objects.requireNonNull(terms).clone();
    // check that all terms are the same field
    String field = null;
    for (Term term : terms) {
      if (field == null) {
        field = term.field();
      } else if (!term.field().equals(field)) {
        throw new IllegalArgumentException("Synonyms must be across the same field");
      }
    }
    if (terms.length > BooleanQuery.getMaxClauseCount()) {
      throw new BooleanQuery.TooManyClauses();
    }
    Arrays.sort(this.terms);
  }

  public List<Term> getTerms() {
    return Collections.unmodifiableList(Arrays.asList(terms));
  }
  
  @Override
  public String toString(String field) {
    StringBuilder builder = new StringBuilder("Synonym(");
    for (int i = 0; i < terms.length; i++) {
      if (i != 0) {
        builder.append(" ");
      }
      Query termQuery = new TermQuery(terms[i]);
      builder.append(termQuery.toString(field));
    }
    builder.append(")");
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + Arrays.hashCode(terms);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           Arrays.equals(terms, ((SynonymQuery) other).terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // optimize zero and single term cases
    if (terms.length == 0) {
      return new BooleanQuery.Builder().build();
    }
    if (terms.length == 1) {
      return new TermQuery(terms[0]);
    }
    return this;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    if (scoreMode.needsScores()) {
      return new SynonymWeight(this, searcher, scoreMode, boost);
    } else {
      // if scores are not needed, let BooleanWeight deal with optimizing that case.
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      for (Term term : terms) {
        bq.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
      }
      return searcher.rewrite(bq.build()).createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, boost);
    }
  }
  
  class SynonymWeight extends Weight {
    private final TermStates termStates[];
    private final Similarity similarity;
    private final Similarity.SimScorer simWeight;
    private final ScoreMode scoreMode;

    SynonymWeight(Query query, IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      super(query);
      assert scoreMode.needsScores();
      this.scoreMode = scoreMode;
      CollectionStatistics collectionStats = searcher.collectionStatistics(terms[0].field());
      long docFreq = 0;
      long totalTermFreq = 0;
      termStates = new TermStates[terms.length];
      for (int i = 0; i < termStates.length; i++) {
        termStates[i] = TermStates.build(searcher.getTopReaderContext(), terms[i], true);
        TermStatistics termStats = searcher.termStatistics(terms[i], termStates[i]);
        if (termStats != null) {
          docFreq = Math.max(termStats.docFreq(), docFreq);
          totalTermFreq += termStats.totalTermFreq();
        }
      }
      this.similarity = searcher.getSimilarity();
      if (docFreq > 0) {
        TermStatistics pseudoStats = new TermStatistics(new BytesRef("synonym pseudo-term"), docFreq, totalTermFreq);
        this.simWeight = similarity.scorer(boost, collectionStats, pseudoStats);
      } else {
        this.simWeight = null; // no terms exist at all, we won't use similarity
      }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (Term term : SynonymQuery.this.terms) {
        terms.add(term);
      }
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      String field = terms[0].field();
      Terms terms = context.reader().terms(field);
      if (terms == null || terms.hasPositions() == false) {
        return super.matches(context, doc);
      }
      return MatchesUtils.forField(field, () -> DisjunctionMatchesIterator.fromTerms(context, doc, getQuery(), field, Arrays.asList(SynonymQuery.this.terms)));
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          final float freq;
          if (scorer instanceof SynonymScorer) {
            freq = ((SynonymScorer) scorer).freq();
          } else {
            assert scorer instanceof TermScorer;
            freq = ((TermScorer)scorer).freq();
          }
          LeafSimScorer docScorer = new LeafSimScorer(simWeight, context.reader(), terms[0].field(), true);
          Explanation freqExplanation = Explanation.match(freq, "termFreq=" + freq);
          Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
          return Explanation.match(
              scoreExplanation.getValue(),
              "weight(" + getQuery() + " in " + doc + ") ["
                  + similarity.getClass().getSimpleName() + "], result of:",
              scoreExplanation);
        }
      }
      return Explanation.noMatch("no matching term");
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      List<PostingsEnum> iterators = new ArrayList<>();
      List<ImpactsEnum> impacts = new ArrayList<>();
      for (int i = 0; i < terms.length; i++) {
        TermState state = termStates[i].get(context);
        if (state != null) {
          TermsEnum termsEnum = context.reader().terms(terms[i].field()).iterator();
          termsEnum.seekExact(terms[i].bytes(), state);
          if (scoreMode == ScoreMode.TOP_SCORES) {
            ImpactsEnum impactsEnum = termsEnum.impacts(PostingsEnum.FREQS);
            iterators.add(impactsEnum);
            impacts.add(impactsEnum);
          } else {
            PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
            iterators.add(postingsEnum);
            impacts.add(new SlowImpactsEnum(postingsEnum));
          }
        }
      }

      if (iterators.isEmpty()) {
        return null;
      }

      LeafSimScorer simScorer = new LeafSimScorer(simWeight, context.reader(), terms[0].field(), true);

      // we must optimize this case (term not in segment), disjunctions require >= 2 subs
      if (iterators.size() == 1) {
        if (scoreMode == ScoreMode.TOP_SCORES) {
          return new TermScorer(this, impacts.get(0), simScorer);
        } else {
          return new TermScorer(this, iterators.get(0), simScorer);
        }
      }

      // we use termscorers + disjunction as an impl detail
      DisiPriorityQueue queue = new DisiPriorityQueue(iterators.size());
      for (PostingsEnum postings : iterators) {
        queue.add(new DisiWrapper(new TermScorer(this, postings, simScorer)));
      }
      // Even though it is called approximation, it is accurate since none of
      // the sub iterators are two-phase iterators.
      DocIdSetIterator iterator = new DisjunctionDISIApproximation(queue);

      ImpactsSource impactsSource = mergeImpacts(impacts.toArray(new ImpactsEnum[0]));
      ImpactsDISI impactsDisi = new ImpactsDISI(iterator, impactsSource, simScorer.getSimScorer());

      if (scoreMode == ScoreMode.TOP_SCORES) {
        iterator = impactsDisi;
      }

      return new SynonymScorer(this, queue, iterator, impactsDisi, simScorer);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }

  /**
   * Merge impacts for multiple synonyms.
   */
  static ImpactsSource mergeImpacts(ImpactsEnum[] impactsEnums) {
    return new ImpactsSource() {

      class SubIterator {
        final Iterator<Impact> iterator;
        int previousFreq;
        Impact current;

        SubIterator(Iterator<Impact> iterator) {
          this.iterator = iterator;
          this.current = iterator.next();
        }

        void next() {
          previousFreq = current.freq;
          if (iterator.hasNext() == false) {
            current = null;
          } else {
            current = iterator.next();
          }
        }

      }

      @Override
      public Impacts getImpacts() throws IOException {
        final Impacts[] impacts = new Impacts[impactsEnums.length];
        // Use the impacts that have the lower next boundary as a lead.
        // It will decide on the number of levels and the block boundaries.
        Impacts tmpLead = null;
        for (int i = 0; i < impactsEnums.length; ++i) {
          impacts[i] = impactsEnums[i].getImpacts();
          if (tmpLead == null || impacts[i].getDocIdUpTo(0) < tmpLead.getDocIdUpTo(0)) {
            tmpLead = impacts[i];
          }
        }
        final Impacts lead = tmpLead;
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

            List<List<Impact>> toMerge = new ArrayList<>();

            for (int i = 0; i < impactsEnums.length; ++i) {
              if (impactsEnums[i].docID() <= docIdUpTo) {
                int impactsLevel = getLevel(impacts[i], docIdUpTo);
                if (impactsLevel == -1) {
                  // One instance doesn't have impacts that cover up to docIdUpTo
                  // Return impacts that trigger the maximum score
                  return Collections.singletonList(new Impact(Integer.MAX_VALUE, 1L));
                }
                toMerge.add(impacts[i].getImpacts(impactsLevel));
              }
            }
            assert toMerge.size() > 0; // otherwise it would mean the docID is > docIdUpTo, which is wrong

            if (toMerge.size() == 1) {
              // common if one synonym is common and the other one is rare
              return toMerge.get(0);
            }

            PriorityQueue<SubIterator> pq = new PriorityQueue<SubIterator>(impacts.length) {
              @Override
              protected boolean lessThan(SubIterator a, SubIterator b) {
                if (a.current == null) { // means iteration is finished
                  return false;
                }
                if (b.current == null) {
                  return true;
                }
                return Long.compareUnsigned(a.current.norm, b.current.norm) < 0;
              }
            };
            for (List<Impact> impacts : toMerge) {
              pq.add(new SubIterator(impacts.iterator()));
            }

            List<Impact> mergedImpacts = new ArrayList<>();

            // Idea: merge impacts by norm. The tricky thing is that we need to
            // consider norm values that are not in the impacts too. For
            // instance if the list of impacts is [{freq=2,norm=10}, {freq=4,norm=12}],
            // there might well be a document that has a freq of 2 and a length of 11,
            // which was just not added to the list of impacts because {freq=2,norm=10}
            // is more competitive. So the way it works is that we track the sum of
            // the term freqs that we have seen so far in order to account for these
            // implicit impacts.

            long sumTf = 0;
            SubIterator top = pq.top();
            do {
              final long norm = top.current.norm;
              do {
                sumTf += top.current.freq - top.previousFreq;
                top.next();
                top = pq.updateTop();
              } while (top.current != null && top.current.norm == norm);

              final int freqUpperBound = (int) Math.min(Integer.MAX_VALUE, sumTf);
              if (mergedImpacts.isEmpty()) {
                mergedImpacts.add(new Impact(freqUpperBound, norm));
              } else {
                Impact prevImpact = mergedImpacts.get(mergedImpacts.size() - 1);
                assert Long.compareUnsigned(prevImpact.norm, norm) < 0;
                if (freqUpperBound > prevImpact.freq) {
                  mergedImpacts.add(new Impact(freqUpperBound, norm));
                } // otherwise the previous impact is already more competitive
              }
            } while (top.current != null);

            return mergedImpacts;
          }
        };
      }

      @Override
      public void advanceShallow(int target) throws IOException {
        for (ImpactsEnum impactsEnum : impactsEnums) {
          if (impactsEnum.docID() < target) {
            impactsEnum.advanceShallow(target);
          }
        }
      }
    };
  }

  private static class SynonymScorer extends Scorer {

    private final DisiPriorityQueue queue;
    private final DocIdSetIterator iterator;
    private final ImpactsDISI impactsDisi;
    private final LeafSimScorer simScorer;

    SynonymScorer(Weight weight, DisiPriorityQueue queue, DocIdSetIterator iterator,
        ImpactsDISI impactsDisi, LeafSimScorer simScorer) {
      super(weight);
      this.queue = queue;
      this.iterator = iterator;
      this.impactsDisi = impactsDisi;
      this.simScorer = simScorer;
    }

    @Override
    public int docID() {
      return iterator.docID();
    }

    int freq() throws IOException {
      DisiWrapper w = queue.topList();
      int freq = ((PostingsEnum) w.iterator).freq();
      for (w = w.next; w != null; w = w.next) {
        freq += ((PostingsEnum) w.iterator).freq();
        if (freq < 0) { // overflow
          return Integer.MAX_VALUE;
        }
      }
      return freq;
    }

    @Override
    public float score() throws IOException {
      return simScorer.score(iterator.docID(), freq());
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return impactsDisi.getMaxScore(upTo);
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      return impactsDisi.advanceShallow(target);
    }

    @Override
    public void setMinCompetitiveScore(float minScore) {
      impactsDisi.setMinCompetitiveScore(minScore);
    }
  }
}
