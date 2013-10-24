package org.apache.lucene.search.posfilter;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.Similarity;

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

public class PhraseQuery2 extends PositionFilterQuery {

  private final BooleanQuery innerBQ;
  private final int slop;

  public PhraseQuery2(int slop) {
    super(new BooleanQuery(), new ExactPhraseScorerFactory(slop));
    this.innerBQ = (BooleanQuery) innerQuery;
    this.slop = slop;
  }

  public PhraseQuery2() {
    this(0);
  }

  public int getSlop() {
    return slop;
  }

  public void add(Term term) {
    innerBQ.add(new TermQuery(term), BooleanClause.Occur.MUST);
  }

  public void addMultiTerm(Term... terms) {
    if (terms.length == 1) {
      add(terms[0]);
      return;
    }
    BooleanQuery disj = new BooleanQuery();
    for (Term term : terms) {
      disj.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
    }
    innerBQ.add(disj, BooleanClause.Occur.MUST);
  }

  private static class ExactPhraseScorerFactory implements ScorerFilterFactory {

    private final int slop;

    ExactPhraseScorerFactory(int slop) {
      this.slop = slop;
    }

    @Override
    public Scorer scorer(Scorer filteredScorer, Similarity.SimScorer simScorer) {
      if (slop == 0)
        return new BlockPhraseScorer(filteredScorer, simScorer);
      else
        return new PartiallyOrderedNearScorer(filteredScorer, slop, simScorer);
    }

    @Override
    public String getName() {
      return slop == 0 ? "ExactPhrase" : "SloppyPhrase/" + slop;
    }
  }

}
